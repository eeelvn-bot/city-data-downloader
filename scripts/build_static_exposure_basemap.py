#!/usr/bin/env python3
"""Build a static ground-exposure basemap for a Chinese city."""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib import error, parse, request

import folium
import numpy as np
from osgeo import gdal, ogr, osr
from PIL import Image
from pyproj import Transformer
from shapely.geometry import GeometryCollection, MultiPolygon, Point, Polygon, shape
from shapely.ops import transform as shapely_transform

from download_city_data import (
    DEFAULT_OVERPASS_ENDPOINTS,
    DEFAULT_ROAD_FILTER,
    WORLDPOP_CHN_2020_URL,
    city_name_candidates,
    http_get_json,
    run_overpass,
    slugify,
)

gdal.UseExceptions()
ogr.UseExceptions()

BOUNDARY_BUFFER_M = 2_000.0
DEFAULT_ANALYSIS_CRS = "EPSG:32651"
DEFAULT_RESOLUTION = 100.0
DEFAULT_WORLDPOP_YEAR = "2020"
DEFAULT_GHSL_LAYER = "built_s"
DEFAULT_WORLDCOVER_YEAR = "2021"
DEFAULT_NODATA = -9999.0
DEFAULT_PREVIEW_WIDTH = 1400

WORLD_COVER_CLASS_WEIGHTS = {
    10: 0.10,
    20: 0.15,
    30: 0.18,
    40: 0.35,
    50: 1.00,
    60: 0.08,
    70: 0.02,
    80: 0.02,
    90: 0.10,
    95: 0.10,
    100: 0.08,
}
WORLD_COVER_BUILTUP_CLASS = 50

LANDUSE_DEFAULT_SCORE = 0.18
OSM_LANDUSE_SCORES = {
    "residential": 1.0,
    "commercial": 0.95,
    "retail": 0.95,
    "industrial": 0.75,
    "education": 1.0,
    "hospital": 1.0,
    "cemetery": 0.25,
    "recreation_ground": 0.25,
    "park": 0.25,
    "forest": 0.08,
    "farmland": 0.30,
    "orchard": 0.30,
    "reservoir": 0.02,
    "basin": 0.02,
    "water": 0.02,
}

SENSITIVE_RULES = [
    {"kind": "school", "base": 1.0, "radius": 250.0},
    {"kind": "university", "base": 1.0, "radius": 250.0},
    {"kind": "hospital", "base": 1.0, "radius": 250.0},
    {"kind": "clinic", "base": 1.0, "radius": 250.0},
    {"kind": "stadium", "base": 0.9, "radius": 250.0},
    {"kind": "sports_centre", "base": 0.9, "radius": 250.0},
    {"kind": "station", "base": 1.0, "radius": 250.0},
    {"kind": "bus_station", "base": 1.0, "radius": 250.0},
    {"kind": "airport", "base": 1.0, "radius": 250.0},
    {"kind": "substation", "base": 0.8, "radius": 300.0},
    {"kind": "plant", "base": 0.8, "radius": 300.0},
    {"kind": "storage_tank", "base": 1.0, "radius": 300.0},
    {"kind": "oil_gas_chemical", "base": 1.0, "radius": 300.0},
    {"kind": "government", "base": 0.8, "radius": 300.0},
]

ROAD_GROUPS = [
    {
        "name": "road_major",
        "filter": {"highway": {"motorway", "trunk", "primary", "motorway_link", "trunk_link", "primary_link"}},
        "base": 1.0,
        "inner": 80.0,
        "outer": 300.0,
    },
    {
        "name": "road_secondary",
        "filter": {"highway": {"secondary", "tertiary", "secondary_link", "tertiary_link"}},
        "base": 0.7,
        "inner": 50.0,
        "outer": 180.0,
    },
    {
        "name": "road_local",
        "filter": {"highway": {"residential", "service", "living_street", "unclassified", "road"}},
        "base": 0.4,
        "inner": 30.0,
        "outer": 100.0,
    },
]

RAIL_GROUPS = [
    {
        "name": "rail_hsr",
        "predicate": "hsr",
        "base": 1.0,
        "inner": 100.0,
        "outer": 300.0,
    },
    {
        "name": "rail_regular",
        "predicate": "rail",
        "base": 0.8,
        "inner": 80.0,
        "outer": 250.0,
    },
]

E_STATIC_WEIGHTS = {
    "r_pop": 0.40,
    "r_built": 0.15,
    "r_transport": 0.15,
    "r_sensitive": 0.20,
    "r_land": 0.10,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a static exposure basemap for a city.")
    parser.add_argument("--city", required=True, help="Chinese city name, for example 杭州市")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--resolution", type=float, default=DEFAULT_RESOLUTION, help="Target raster resolution in meters")
    parser.add_argument("--analysis-crs", default=DEFAULT_ANALYSIS_CRS, help="Target projected CRS")
    parser.add_argument("--boundary-source", default="osm_admin", choices=["osm_admin"], help="Boundary source")
    parser.add_argument("--worldpop-year", default=DEFAULT_WORLDPOP_YEAR, help="WorldPop year tag for manifest")
    parser.add_argument("--ghsl-layer", default=DEFAULT_GHSL_LAYER, help="GHSL layer tag for manifest")
    parser.add_argument("--worldcover-year", default=DEFAULT_WORLDCOVER_YEAR, help="WorldCover year tag for manifest")
    parser.add_argument(
        "--ghsl-src",
        action="append",
        default=[],
        help="Local path or URL to GHSL raster; repeatable or comma-separated",
    )
    parser.add_argument(
        "--worldcover-src",
        action="append",
        default=[],
        help="Local path or URL to WorldCover raster; repeatable or comma-separated",
    )
    parser.add_argument("--osm-timeout", type=int, default=240, help="Overpass timeout in seconds")
    parser.add_argument("--stac-timeout", type=int, default=90, help="WorldCover STAC timeout in seconds")
    parser.add_argument("--skip-preview", action="store_true", help="Skip PNG/HTML preview generation")
    return parser.parse_args()


def log(message: str) -> None:
    print(message, flush=True)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def flatten_inputs(values: Sequence[str]) -> List[str]:
    items: List[str] = []
    for value in values:
        if not value:
            continue
        for part in str(value).split(","):
            text = part.strip()
            if text:
                items.append(text)
    return items


def is_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def download_file(url: str, out_path: Path, timeout: int = 120) -> Path:
    if out_path.exists():
        return out_path
    headers = {"User-Agent": "city-data-downloader/1.0"}
    req = request.Request(url, headers=headers)
    with request.urlopen(req, timeout=timeout) as response, out_path.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    return out_path


def copy_or_download_source(source: str, cache_dir: Path, timeout: int = 120) -> Path:
    if is_url(source):
        name = parse.urlparse(source).path.split("/")[-1] or "dataset.bin"
        target = cache_dir / name
        return download_file(source, target, timeout=timeout)
    src = Path(source).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"Source not found: {src}")
    target = cache_dir / src.name
    if target.exists():
        return target
    shutil.copy2(src, target)
    return target


def request_json(url: str, payload: Optional[Dict[str, Any]] = None, timeout: int = 90) -> Any:
    headers = {
        "User-Agent": "city-data-downloader/1.0",
        "Accept": "application/json",
    }
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers=headers, method="POST" if data else "GET")
    with request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def resolve_city_boundary(city: str) -> Tuple[Polygon | MultiPolygon, Dict[str, Any]]:
    headers = {"User-Agent": "city-data-downloader/1.0"}
    for name in city_name_candidates(city):
        params = parse.urlencode(
            {
                "q": name,
                "format": "jsonv2",
                "countrycodes": "cn",
                "limit": 10,
                "addressdetails": 1,
                "polygon_geojson": 1,
            }
        )
        url = f"https://nominatim.openstreetmap.org/search?{params}"
        records = http_get_json(url, headers=headers, timeout=60)
        if not records:
            continue
        boundary_record: Optional[Dict[str, Any]] = None
        for record in records:
            if record.get("class") == "boundary" and record.get("type") in {"administrative", "city"} and record.get("geojson"):
                boundary_record = record
                break
        if boundary_record is None:
            for record in records:
                if record.get("geojson"):
                    boundary_record = record
                    break
        if boundary_record is None:
            continue
        geometry = shape(boundary_record["geojson"])
        if not geometry.is_valid:
            geometry = geometry.buffer(0)
        if geometry.is_empty:
            continue
        if isinstance(geometry, Polygon):
            geometry = MultiPolygon([geometry])
        elif isinstance(geometry, GeometryCollection):
            polys = [geom for geom in geometry.geoms if isinstance(geom, Polygon)]
            geometry = MultiPolygon(polys)
        if not isinstance(geometry, MultiPolygon) or geometry.is_empty:
            continue
        info = {
            "display_name": boundary_record.get("display_name", name),
            "osm_id": boundary_record.get("osm_id"),
            "osm_type": boundary_record.get("osm_type"),
            "class": boundary_record.get("class"),
            "type": boundary_record.get("type"),
            "bbox": boundary_record.get("boundingbox"),
        }
        return geometry, info
    raise RuntimeError(f"Could not resolve administrative boundary for {city}")


def build_transformers(target_crs: str) -> Tuple[Transformer, Transformer]:
    to_analysis = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
    to_wgs84 = Transformer.from_crs(target_crs, "EPSG:4326", always_xy=True)
    return to_analysis, to_wgs84


def project_geometry(geometry: Polygon | MultiPolygon, transformer: Transformer) -> Polygon | MultiPolygon:
    projected = shapely_transform(transformer.transform, geometry)
    if not projected.is_valid:
        projected = projected.buffer(0)
    return projected


def compute_buffer_bbox(geometry_utm: Polygon | MultiPolygon, transformer_back: Transformer, buffer_m: float) -> Tuple[float, float, float, float]:
    buffered = geometry_utm.buffer(buffer_m)
    buffered_wgs84 = shapely_transform(transformer_back.transform, buffered)
    minx, miny, maxx, maxy = buffered_wgs84.bounds
    return miny, maxy, minx, maxx


def write_geojson(path: Path, features: List[Dict[str, Any]]) -> None:
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False), encoding="utf-8")


def geometry_mapping(geometry: Any) -> Dict[str, Any]:
    return geometry.__geo_interface__


def boundary_feature(geometry: Polygon | MultiPolygon, properties: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": geometry_mapping(geometry),
        "properties": properties,
    }


def build_osm_transport_query(bbox: Tuple[float, float, float, float], timeout: int) -> str:
    south, north, west, east = bbox
    return f"""
[out:json][timeout:{timeout}];
(
  way({south},{west},{north},{east})[highway~"{DEFAULT_ROAD_FILTER}"];
  way({south},{west},{north},{east})[railway=rail];
);
out geom tags;
""".strip()


def build_osm_sensitive_query(bbox: Tuple[float, float, float, float], timeout: int) -> str:
    south, north, west, east = bbox
    return f"""
[out:json][timeout:{timeout}];
(
  nwr({south},{west},{north},{east})[amenity~"^(school|college|university|kindergarten|hospital|clinic|bus_station)$"];
  nwr({south},{west},{north},{east})[leisure~"^(stadium|sports_centre)$"];
  nwr({south},{west},{north},{east})[railway=station];
  nwr({south},{west},{north},{east})[aeroway~"^(aerodrome|terminal)$"];
  nwr({south},{west},{north},{east})[power~"^(substation|plant)$"];
  nwr({south},{west},{north},{east})[man_made=storage_tank];
  nwr({south},{west},{north},{east})[office=government];
  nwr({south},{west},{north},{east})[industrial~"^(oil|gas|chemical)$"];
  nwr({south},{west},{north},{east})[landuse~"^(industrial|commercial|retail)$"];
);
out center tags;
""".strip()


def build_osm_landuse_query(bbox: Tuple[float, float, float, float], timeout: int) -> str:
    south, north, west, east = bbox
    return f"""
[out:json][timeout:{timeout}];
(
  way({south},{west},{north},{east})[landuse~"^(residential|commercial|retail|industrial|cemetery|forest|farmland|orchard)$"];
  relation({south},{west},{north},{east})[landuse~"^(residential|commercial|retail|industrial|cemetery|forest|farmland|orchard)$"];
  way({south},{west},{north},{east})[amenity~"^(school|college|university|kindergarten|hospital|clinic)$"];
  relation({south},{west},{north},{east})[amenity~"^(school|college|university|kindergarten|hospital|clinic)$"];
  way({south},{west},{north},{east})[leisure~"^(park|recreation_ground)$"];
  relation({south},{west},{north},{east})[leisure~"^(park|recreation_ground)$"];
  way({south},{west},{north},{east})[natural=water];
  relation({south},{west},{north},{east})[natural=water];
  way({south},{west},{north},{east})[water];
  relation({south},{west},{north},{east})[water];
);
out geom tags;
""".strip()


def pick_coordinates(element: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    if "lat" in element and "lon" in element:
        return element.get("lon"), element.get("lat")
    center = element.get("center") or {}
    if "lon" in center and "lat" in center:
        return center.get("lon"), center.get("lat")
    return None, None


def line_geometry_from_overpass(element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    geom = element.get("geometry") or []
    coords = [[pt["lon"], pt["lat"]] for pt in geom if "lon" in pt and "lat" in pt]
    if len(coords) < 2:
        return None
    return {"type": "LineString", "coordinates": coords}


def polygon_geometry_from_overpass(element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    geom = element.get("geometry") or []
    coords = [[pt["lon"], pt["lat"]] for pt in geom if "lon" in pt and "lat" in pt]
    if len(coords) < 4:
        return None
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    return {"type": "Polygon", "coordinates": [coords]}


def is_high_speed_rail(tags: Dict[str, Any]) -> bool:
    if str(tags.get("highspeed", "")).lower() in {"yes", "true", "1"}:
        return True
    if "高铁" in str(tags.get("name", "")):
        return True
    usage = str(tags.get("usage", "")).lower()
    if usage == "main":
        maxspeed_text = str(tags.get("maxspeed", ""))
        digits = "".join(ch for ch in maxspeed_text if ch.isdigit())
        if digits and int(digits) >= 200:
            return True
    return False


def classify_sensitive(tags: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    amenity = str(tags.get("amenity", "")).lower()
    leisure = str(tags.get("leisure", "")).lower()
    railway = str(tags.get("railway", "")).lower()
    aeroway = str(tags.get("aeroway", "")).lower()
    power = str(tags.get("power", "")).lower()
    man_made = str(tags.get("man_made", "")).lower()
    office = str(tags.get("office", "")).lower()
    industrial = str(tags.get("industrial", "")).lower()
    if amenity in {"school", "college", "university", "kindergarten"}:
        return {"kind": "university" if amenity in {"college", "university"} else "school"}
    if amenity in {"hospital", "clinic"}:
        return {"kind": amenity}
    if leisure in {"stadium", "sports_centre"}:
        return {"kind": leisure}
    if railway == "station":
        return {"kind": "station"}
    if amenity == "bus_station":
        return {"kind": "bus_station"}
    if aeroway in {"aerodrome", "terminal"}:
        return {"kind": "airport"}
    if power in {"substation", "plant"}:
        return {"kind": power}
    if man_made == "storage_tank":
        return {"kind": "storage_tank"}
    if office == "government":
        return {"kind": "government"}
    if industrial in {"oil", "gas", "chemical"}:
        return {"kind": "oil_gas_chemical"}
    return None


def classify_landuse_score(tags: Dict[str, Any]) -> float:
    amenity = str(tags.get("amenity", "")).lower()
    leisure = str(tags.get("leisure", "")).lower()
    water = str(tags.get("water", "")).lower()
    natural = str(tags.get("natural", "")).lower()
    landuse = str(tags.get("landuse", "")).lower()
    if amenity in {"school", "college", "university", "kindergarten"}:
        return OSM_LANDUSE_SCORES["education"]
    if amenity in {"hospital", "clinic"}:
        return OSM_LANDUSE_SCORES["hospital"]
    if leisure in {"park", "recreation_ground"}:
        return OSM_LANDUSE_SCORES[leisure]
    if natural == "water" or water:
        return OSM_LANDUSE_SCORES["water"]
    if landuse in OSM_LANDUSE_SCORES:
        return OSM_LANDUSE_SCORES[landuse]
    return LANDUSE_DEFAULT_SCORE


def overpass_to_transport_features(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    roads: List[Dict[str, Any]] = []
    rails: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for element in data.get("elements") or []:
        if element.get("type") != "way":
            continue
        geometry = line_geometry_from_overpass(element)
        if not geometry:
            continue
        feature_id = f"way/{element.get('id')}"
        if feature_id in seen:
            continue
        seen.add(feature_id)
        tags = element.get("tags") or {}
        props = {
            "id": feature_id,
            "name": tags.get("name", ""),
            "highway": str(tags.get("highway", "")),
            "railway": str(tags.get("railway", "")),
            "is_hsr": bool(is_high_speed_rail(tags)) if tags.get("railway") == "rail" else False,
            "raw_tags": tags,
        }
        feature = {"type": "Feature", "geometry": geometry, "properties": props}
        if tags.get("highway"):
            roads.append(feature)
        elif tags.get("railway") == "rail":
            rails.append(feature)
    return roads, rails


def overpass_to_sensitive_features(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    features: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for element in data.get("elements") or []:
        tags = element.get("tags") or {}
        classification = classify_sensitive(tags)
        if not classification:
            continue
        lon, lat = pick_coordinates(element)
        if lon is None or lat is None:
            continue
        feature_id = f"{element.get('type', 'obj')}/{element.get('id')}"
        if feature_id in seen:
            continue
        seen.add(feature_id)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "id": feature_id,
                    "name": tags.get("name", ""),
                    "kind": classification["kind"],
                    "raw_tags": tags,
                },
            }
        )
    return features


def overpass_to_landuse_features(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    features: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for element in data.get("elements") or []:
        geom = polygon_geometry_from_overpass(element)
        if not geom:
            continue
        feature_id = f"{element.get('type', 'obj')}/{element.get('id')}"
        if feature_id in seen:
            continue
        seen.add(feature_id)
        tags = element.get("tags") or {}
        features.append(
            {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "id": feature_id,
                    "name": tags.get("name", ""),
                    "score": classify_landuse_score(tags),
                    "landuse": str(tags.get("landuse", "")),
                    "amenity": str(tags.get("amenity", "")),
                    "leisure": str(tags.get("leisure", "")),
                    "natural": str(tags.get("natural", "")),
                    "water": str(tags.get("water", "")),
                    "raw_tags": tags,
                },
            }
        )
    features.sort(key=lambda item: float(item["properties"].get("score", LANDUSE_DEFAULT_SCORE)))
    return features


def query_osm_layers(city: str, bbox: Tuple[float, float, float, float], timeout: int, raw_dir: Path, vector_dir: Path) -> Dict[str, Path]:
    roads_path = vector_dir / "hangzhou_roads.geojson"
    rails_path = vector_dir / "hangzhou_rail.geojson"
    roads_raw = raw_dir / "osm_transport_raw.json"
    if not (roads_path.exists() and rails_path.exists()):
        roads_json = run_overpass(build_osm_transport_query(bbox, timeout), DEFAULT_OVERPASS_ENDPOINTS, timeout=timeout)
        roads_raw.write_text(json.dumps(roads_json, ensure_ascii=False), encoding="utf-8")
        roads, rails = overpass_to_transport_features(roads_json)
        write_geojson(roads_path, roads)
        write_geojson(rails_path, rails)
    else:
        log("Reusing cached OSM transport layers")

    sensitive_path = vector_dir / "hangzhou_sensitive_poi.geojson"
    sensitive_raw = raw_dir / "osm_sensitive_raw.json"
    if not sensitive_path.exists():
        sensitive_json = run_overpass(build_osm_sensitive_query(bbox, timeout), DEFAULT_OVERPASS_ENDPOINTS, timeout=timeout)
        sensitive_raw.write_text(json.dumps(sensitive_json, ensure_ascii=False), encoding="utf-8")
        sensitive_features = overpass_to_sensitive_features(sensitive_json)
        write_geojson(sensitive_path, sensitive_features)
    else:
        log("Reusing cached OSM sensitive layer")

    landuse_raw = raw_dir / "osm_landuse_raw.json"
    landuse_path = vector_dir / "hangzhou_landuse.geojson"
    if not landuse_path.exists():
        try:
            landuse_json = run_overpass(build_osm_landuse_query(bbox, timeout), DEFAULT_OVERPASS_ENDPOINTS, timeout=timeout)
            landuse_raw.write_text(json.dumps(landuse_json, ensure_ascii=False), encoding="utf-8")
            landuse_features = overpass_to_landuse_features(landuse_json)
            write_geojson(landuse_path, landuse_features)
        except Exception as exc:
            log(f"OSM landuse query failed, continuing with empty overlay: {exc}")
            landuse_raw.write_text(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2), encoding="utf-8")
            write_geojson(landuse_path, [])
    else:
        log("Reusing cached OSM landuse layer")

    return {
        "roads": roads_path,
        "rail": rails_path,
        "sensitive": sensitive_path,
        "landuse": landuse_path,
    }


def write_boundary(boundary_wgs84: Polygon | MultiPolygon, info: Dict[str, Any], vector_dir: Path) -> Path:
    path = vector_dir / "hangzhou_boundary.geojson"
    write_geojson(path, [boundary_feature(boundary_wgs84, info)])
    return path


def create_reference_raster(path: Path, bounds: Tuple[float, float, float, float], resolution: float, epsg_wkt: str, nodata: float) -> Tuple[int, int, Tuple[float, float, float, float, float, float]]:
    minx, miny, maxx, maxy = bounds
    aligned_minx = math.floor(minx / resolution) * resolution
    aligned_miny = math.floor(miny / resolution) * resolution
    aligned_maxx = math.ceil(maxx / resolution) * resolution
    aligned_maxy = math.ceil(maxy / resolution) * resolution
    width = int(round((aligned_maxx - aligned_minx) / resolution))
    height = int(round((aligned_maxy - aligned_miny) / resolution))
    geotransform = (aligned_minx, resolution, 0.0, aligned_maxy, 0.0, -resolution)
    driver = gdal.GetDriverByName("GTiff")
    dataset = driver.Create(str(path), width, height, 1, gdal.GDT_Float32, options=["COMPRESS=LZW", "TILED=YES"])
    dataset.SetGeoTransform(geotransform)
    dataset.SetProjection(epsg_wkt)
    band = dataset.GetRasterBand(1)
    band.Fill(nodata)
    band.SetNoDataValue(nodata)
    band.FlushCache()
    dataset.FlushCache()
    dataset = None
    return width, height, geotransform


def rasterize_layer(vector_path: Path, out_path: Path, ref_path: Path, burn_value: Optional[float] = None, attribute: Optional[str] = None, all_touched: bool = True) -> Path:
    ref = gdal.Open(str(ref_path))
    if ref is None:
        raise RuntimeError(f"Could not open reference raster: {ref_path}")
    driver = gdal.GetDriverByName("GTiff")
    out = driver.Create(
        str(out_path),
        ref.RasterXSize,
        ref.RasterYSize,
        1,
        gdal.GDT_Float32,
        options=["COMPRESS=LZW", "TILED=YES"],
    )
    out.SetGeoTransform(ref.GetGeoTransform())
    out.SetProjection(ref.GetProjection())
    band = out.GetRasterBand(1)
    band.Fill(0.0)
    band.SetNoDataValue(0.0)
    source = ogr.Open(str(vector_path))
    if source is None:
        raise RuntimeError(f"Could not open vector source: {vector_path}")
    layer = source.GetLayer(0)
    options: List[str] = []
    if all_touched:
        options.append("ALL_TOUCHED=TRUE")
    if attribute:
        options.append(f"ATTRIBUTE={attribute}")
        gdal.RasterizeLayer(out, [1], layer, options=options)
    elif burn_value is not None:
        gdal.RasterizeLayer(out, [1], layer, burn_values=[burn_value], options=options)
    else:
        raise ValueError("Either burn_value or attribute is required")
    band.FlushCache()
    out.FlushCache()
    source = None
    out = None
    ref = None
    return out_path


def create_boundary_mask(boundary_path: Path, ref_path: Path, out_path: Path) -> np.ndarray:
    rasterize_layer(boundary_path, out_path, ref_path, burn_value=1.0)
    dataset = gdal.Open(str(out_path))
    array = dataset.GetRasterBand(1).ReadAsArray().astype(bool)
    dataset = None
    return array


def array_stats(values: np.ndarray, mask: Optional[np.ndarray] = None) -> Dict[str, float]:
    if mask is not None:
        data = values[mask]
    else:
        data = values.ravel()
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
    return {
        "min": float(np.min(finite)),
        "max": float(np.max(finite)),
        "mean": float(np.mean(finite)),
        "p50": float(np.percentile(finite, 50)),
        "p95": float(np.percentile(finite, 95)),
        "p99": float(np.percentile(finite, 99)),
    }


def save_array_as_tif(path: Path, ref_path: Path, array: np.ndarray, nodata: float) -> None:
    ref = gdal.Open(str(ref_path))
    driver = gdal.GetDriverByName("GTiff")
    out = driver.Create(
        str(path),
        ref.RasterXSize,
        ref.RasterYSize,
        1,
        gdal.GDT_Float32,
        options=["COMPRESS=LZW", "TILED=YES"],
    )
    out.SetGeoTransform(ref.GetGeoTransform())
    out.SetProjection(ref.GetProjection())
    band = out.GetRasterBand(1)
    band.WriteArray(array.astype(np.float32))
    band.SetNoDataValue(nodata)
    band.FlushCache()
    out.FlushCache()
    out = None
    ref = None


def warp_dataset(src: str, dst: Path, ref_path: Path, resample_alg: str, dst_nodata: float = DEFAULT_NODATA, output_type: int = gdal.GDT_Float32) -> Path:
    ref = gdal.Open(str(ref_path))
    geotransform = ref.GetGeoTransform()
    minx = geotransform[0]
    maxy = geotransform[3]
    maxx = minx + ref.RasterXSize * geotransform[1]
    miny = maxy + ref.RasterYSize * geotransform[5]
    options = gdal.WarpOptions(
        format="GTiff",
        outputBounds=(minx, miny, maxx, maxy),
        width=ref.RasterXSize,
        height=ref.RasterYSize,
        dstSRS=ref.GetProjection(),
        resampleAlg=resample_alg,
        dstNodata=dst_nodata,
        outputType=output_type,
        creationOptions=["COMPRESS=LZW", "TILED=YES"],
    )
    gdal.Warp(str(dst), src, options=options)
    ref = None
    return dst


def warp_dataset_to_grid(src: str, dst: Path, bounds: Tuple[float, float, float, float], target_crs: str, resolution: float, resample_alg: str, dst_nodata: float = DEFAULT_NODATA, output_type: int = gdal.GDT_Float32) -> Path:
    minx, miny, maxx, maxy = bounds
    options = gdal.WarpOptions(
        format="GTiff",
        outputBounds=(minx, miny, maxx, maxy),
        xRes=resolution,
        yRes=resolution,
        targetAlignedPixels=True,
        dstSRS=target_crs,
        resampleAlg=resample_alg,
        dstNodata=dst_nodata,
        outputType=output_type,
        creationOptions=["COMPRESS=LZW", "TILED=YES"],
    )
    gdal.Warp(str(dst), src, options=options)
    return dst


def find_geotiff_assets(files: Sequence[Path]) -> List[Path]:
    rasters: List[Path] = []
    for path in files:
        if path.is_dir():
            rasters.extend(sorted(path.rglob("*.tif")))
            rasters.extend(sorted(path.rglob("*.tiff")))
        elif path.suffix.lower() in {".tif", ".tiff"}:
            rasters.append(path)
    return rasters


def expand_raster_sources(files: Sequence[Path], cache_dir: Path) -> List[Path]:
    expanded: List[Path] = []
    for path in files:
        suffix = path.suffix.lower()
        if suffix == ".zip":
            extract_dir = ensure_dir(cache_dir / f"{path.stem}_unzipped")
            sentinel = extract_dir / ".extracted"
            if not sentinel.exists():
                with zipfile.ZipFile(path) as archive:
                    archive.extractall(extract_dir)
                sentinel.write_text(path.name, encoding="utf-8")
            expanded.extend(find_geotiff_assets([extract_dir]))
            continue
        expanded.append(path)
    rasters = find_geotiff_assets(expanded)
    if not rasters:
        raise RuntimeError(f"No GeoTIFF assets found in sources: {[str(path) for path in files]}")
    return rasters


def build_vrt(sources: Sequence[Path], out_path: Path) -> Path:
    if not sources:
        raise RuntimeError("No raster sources available to build VRT")
    gdal.BuildVRT(str(out_path), [str(path) for path in sources])
    return out_path


def locate_worldpop_source(cache_dir: Path) -> Path:
    filename = "chn_pd_2020_1km_UNadj.tif"
    local_data = Path(__file__).resolve().parents[3] / "data" / "population" / "chn_pd_2020_1km.tif"
    if local_data.exists():
        return local_data
    target = cache_dir / filename
    if target.exists():
        return target
    log(f"Downloading WorldPop source to {target}")
    return download_file(WORLDPOP_CHN_2020_URL, target)


def resolve_worldcover_sources(args: argparse.Namespace, raw_dir: Path, bbox_wgs84: Tuple[float, float, float, float]) -> List[Path]:
    provided = flatten_inputs(args.worldcover_src)
    if provided:
        cache_dir = ensure_dir(raw_dir / "worldcover")
        return expand_raster_sources([copy_or_download_source(source, cache_dir) for source in provided], cache_dir)

    env_url = os.getenv("WORLDCOVER_SRC")
    if env_url:
        cache_dir = ensure_dir(raw_dir / "worldcover")
        return expand_raster_sources([copy_or_download_source(env_url, cache_dir)], cache_dir)

    stac_url = os.getenv("WORLDCOVER_STAC_URL", "https://services.terrascope.be/stac/search")
    collection = os.getenv("WORLDCOVER_STAC_COLLECTION", "urn:eop:VITO:ESA_WorldCover_10m_2021_AWS_V2")
    payload = {
        "collections": [collection],
        "bbox": [bbox_wgs84[2], bbox_wgs84[0], bbox_wgs84[3], bbox_wgs84[1]],
        "limit": 16,
    }
    cache_dir = ensure_dir(raw_dir / "worldcover")
    try:
        response = request_json(stac_url, payload=payload, timeout=args.stac_timeout)
    except Exception as exc:
        raise RuntimeError(
            "WorldCover source not provided and automatic STAC discovery failed. "
            "Provide --worldcover-src <path-or-url>."
        ) from exc

    sources: List[Path] = []
    for feature in response.get("features") or []:
        assets = feature.get("assets") or {}
        href = None
        for asset in assets.values():
            candidate = str(asset.get("href", ""))
            if candidate.lower().endswith(".tif") or candidate.lower().endswith(".tiff"):
                href = candidate
                break
        if href:
            sources.append(copy_or_download_source(href, cache_dir, timeout=args.stac_timeout))
    if not sources:
        raise RuntimeError("WorldCover STAC search returned no raster assets. Provide --worldcover-src <path-or-url>.")
    return expand_raster_sources(sources, cache_dir)


def resolve_ghsl_sources(args: argparse.Namespace, raw_dir: Path) -> List[Path]:
    provided = flatten_inputs(args.ghsl_src)
    if provided:
        cache_dir = ensure_dir(raw_dir / "ghsl")
        return expand_raster_sources([copy_or_download_source(source, cache_dir) for source in provided], cache_dir)

    env_url = os.getenv("GHSL_BUILT_S_URL") or os.getenv("GHSL_SRC")
    if env_url:
        cache_dir = ensure_dir(raw_dir / "ghsl")
        return expand_raster_sources([copy_or_download_source(env_url, cache_dir)], cache_dir)

    cache_dir = ensure_dir(raw_dir / "ghsl")
    cached = expand_raster_sources(list(cache_dir.iterdir()), cache_dir) if any(cache_dir.iterdir()) else []
    if cached:
        return cached
    raise RuntimeError(
        "GHSL source not provided. Provide --ghsl-src <path-or-url> or set GHSL_BUILT_S_URL."
    )


def dataset_to_array(path: Path) -> Tuple[np.ndarray, Tuple[float, float, float, float, float, float], str, float]:
    dataset = gdal.Open(str(path))
    band = dataset.GetRasterBand(1)
    array = band.ReadAsArray().astype(np.float32)
    nodata = band.GetNoDataValue()
    geotransform = dataset.GetGeoTransform()
    projection = dataset.GetProjection()
    dataset = None
    return array, geotransform, projection, float(nodata) if nodata is not None else np.nan


def normalize_percentile(array: np.ndarray, mask: np.ndarray, percentile: float) -> np.ndarray:
    valid = array[mask & np.isfinite(array) & (array > 0)]
    if valid.size == 0:
        return np.zeros_like(array, dtype=np.float32)
    threshold = float(np.percentile(valid, percentile))
    if threshold <= 0:
        return np.zeros_like(array, dtype=np.float32)
    return np.clip(array / threshold, 0.0, 1.0).astype(np.float32)


def classify_worldcover_arrays(worldcover_mode_100m: Path, mask: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    array, _, _, nodata = dataset_to_array(worldcover_mode_100m)
    land = np.zeros_like(array, dtype=np.float32)
    for klass, weight in WORLD_COVER_CLASS_WEIGHTS.items():
        land[array == klass] = weight
    built_mask = np.zeros_like(array, dtype=np.float32)
    built_mask[array == WORLD_COVER_BUILTUP_CLASS] = 1.0
    if np.isfinite(nodata):
        land[array == nodata] = 0.0
        built_mask[array == nodata] = 0.0
    land[~mask] = 0.0
    built_mask[~mask] = 0.0
    return land, built_mask


def create_worldcover_derivatives(worldcover_sources: Sequence[Path], working_dir: Path, ref_path: Path, mask: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    vrt = build_vrt(worldcover_sources, working_dir / "worldcover.vrt")
    mode_100m = warp_dataset(str(vrt), working_dir / "worldcover_mode_100m.tif", ref_path, resample_alg="mode", dst_nodata=0, output_type=gdal.GDT_Int16)
    return classify_worldcover_arrays(mode_100m, mask)


def create_ghsl_derivatives(ghsl_sources: Sequence[Path], working_dir: Path, ref_path: Path, mask: np.ndarray) -> np.ndarray:
    vrt = build_vrt(ghsl_sources, working_dir / "ghsl.vrt")
    ghsl_100m = warp_dataset(str(vrt), working_dir / "ghsl_100m.tif", ref_path, resample_alg="average", dst_nodata=0, output_type=gdal.GDT_Float32)
    array, _, _, nodata = dataset_to_array(ghsl_100m)
    if np.isfinite(nodata):
        array[array == nodata] = 0.0
    array[~mask] = 0.0
    return normalize_percentile(array, mask, 95.0)


def create_worldpop_coarse(worldpop_src: Path, working_dir: Path, boundary_bounds_utm: Tuple[float, float, float, float], analysis_crs: str) -> Path:
    coarse_path = working_dir / "worldpop_utm_1000m.tif"
    return warp_dataset_to_grid(str(worldpop_src), coarse_path, boundary_bounds_utm, analysis_crs, resolution=1000.0, resample_alg="near", dst_nodata=0.0)


def compute_population_dasymetric(
    worldpop_1km_path: Path,
    ref_path: Path,
    mask: np.ndarray,
    ghsl_norm: np.ndarray,
    worldcover_land: np.ndarray,
) -> np.ndarray:
    coarse_array, coarse_gt, _, coarse_nodata = dataset_to_array(worldpop_1km_path)
    ref_dataset = gdal.Open(str(ref_path))
    ref_gt = ref_dataset.GetGeoTransform()
    height = ref_dataset.RasterYSize
    width = ref_dataset.RasterXSize
    ref_dataset = None

    rows, cols = np.indices((height, width))
    xs = ref_gt[0] + (cols + 0.5) * ref_gt[1]
    ys = ref_gt[3] + (rows + 0.5) * ref_gt[5]
    coarse_cols = np.floor((xs - coarse_gt[0]) / coarse_gt[1]).astype(np.int64)
    coarse_rows = np.floor((ys - coarse_gt[3]) / coarse_gt[5]).astype(np.int64)
    valid = (
        mask
        & (coarse_cols >= 0)
        & (coarse_cols < coarse_array.shape[1])
        & (coarse_rows >= 0)
        & (coarse_rows < coarse_array.shape[0])
    )
    if np.isfinite(coarse_nodata):
        valid &= coarse_array[coarse_rows.clip(0), coarse_cols.clip(0)] != coarse_nodata

    flat_idx = coarse_rows * coarse_array.shape[1] + coarse_cols
    weights = (0.7 * ghsl_norm + 0.3 * worldcover_land).astype(np.float32)
    weights = np.where(valid, weights, 0.0)
    counts = np.where(valid, 1.0, 0.0).astype(np.float32)

    weight_sums = np.bincount(flat_idx[valid].ravel(), weights=weights[valid].ravel(), minlength=coarse_array.size)
    cell_counts = np.bincount(flat_idx[valid].ravel(), weights=counts[valid].ravel(), minlength=coarse_array.size)
    src_flat = coarse_array.ravel()

    pop = np.zeros((height, width), dtype=np.float32)
    valid_flat_idx = flat_idx[valid]
    valid_weights = weights[valid]
    denom = weight_sums[valid_flat_idx]
    numer = src_flat[valid_flat_idx] * valid_weights
    population_values = np.zeros(valid_weights.shape, dtype=np.float32)
    non_zero = denom > 0
    population_values[non_zero] = numer[non_zero] / denom[non_zero]
    zero_mask = ~non_zero
    if np.any(zero_mask):
        counts_denom = cell_counts[valid_flat_idx[zero_mask]]
        counts_non_zero = counts_denom > 0
        zero_indices = np.flatnonzero(zero_mask)
        assign_indices = zero_indices[counts_non_zero]
        if assign_indices.size:
            population_values[assign_indices] = src_flat[valid_flat_idx[assign_indices]] / counts_denom[counts_non_zero]
    pop[valid] = population_values
    pop[~mask] = 0.0
    return pop


def compute_r_pop(population_dasy: np.ndarray, mask: np.ndarray) -> np.ndarray:
    valid = population_dasy[mask & np.isfinite(population_dasy) & (population_dasy > 0)]
    if valid.size == 0:
        return np.zeros_like(population_dasy, dtype=np.float32)
    p99 = float(np.percentile(valid, 99))
    if p99 <= 0:
        return np.zeros_like(population_dasy, dtype=np.float32)
    return np.clip(np.log1p(population_dasy) / math.log1p(p99), 0.0, 1.0).astype(np.float32)


def ogr_open(path: Path) -> ogr.DataSource:
    datasource = ogr.Open(str(path))
    if datasource is None:
        raise RuntimeError(f"Could not open vector file: {path}")
    return datasource


def filter_features_to_geojson(source_path: Path, output_path: Path, field_name: str, allowed_values: Iterable[Any]) -> Optional[Path]:
    source = ogr_open(source_path)
    layer = source.GetLayer(0)
    allowed = {str(value) for value in allowed_values}
    driver = ogr.GetDriverByName("GeoJSON")
    if output_path.exists():
        driver.DeleteDataSource(str(output_path))
    dest = driver.CreateDataSource(str(output_path))
    spatial_ref = layer.GetSpatialRef()
    out_layer = dest.CreateLayer(layer.GetName(), srs=spatial_ref, geom_type=layer.GetGeomType())
    layer_defn = layer.GetLayerDefn()
    for idx in range(layer_defn.GetFieldCount()):
        out_layer.CreateField(layer_defn.GetFieldDefn(idx))
    out_layer_defn = out_layer.GetLayerDefn()
    kept = 0
    for feature in layer:
        value = str(feature.GetField(field_name) or "")
        if value not in allowed:
            continue
        out_feature = ogr.Feature(out_layer_defn)
        out_feature.SetFrom(feature)
        out_layer.CreateFeature(out_feature)
        out_feature = None
        kept += 1
    source = None
    dest = None
    if kept == 0:
        output_path.unlink(missing_ok=True)
        return None
    return output_path


def compute_proximity_risk(binary_vector_path: Optional[Path], ref_path: Path, working_dir: Path, inner: float, outer: float, base: float, cache_name: str) -> np.ndarray:
    ref = gdal.Open(str(ref_path))
    shape_hw = (ref.RasterYSize, ref.RasterXSize)
    ref = None
    if binary_vector_path is None:
        return np.zeros(shape_hw, dtype=np.float32)

    binary_raster = working_dir / f"{cache_name}_binary.tif"
    proximity_raster = working_dir / f"{cache_name}_distance.tif"
    rasterize_layer(binary_vector_path, binary_raster, ref_path, burn_value=1.0)
    src = gdal.Open(str(binary_raster))
    driver = gdal.GetDriverByName("GTiff")
    prox = driver.Create(
        str(proximity_raster),
        src.RasterXSize,
        src.RasterYSize,
        1,
        gdal.GDT_Float32,
        options=["COMPRESS=LZW", "TILED=YES"],
    )
    prox.SetGeoTransform(src.GetGeoTransform())
    prox.SetProjection(src.GetProjection())
    src_band = src.GetRasterBand(1)
    prox_band = prox.GetRasterBand(1)
    gdal.ComputeProximity(src_band, prox_band, ["VALUES=1", "DISTUNITS=GEO"])
    distances = prox_band.ReadAsArray().astype(np.float32)
    src = None
    prox = None
    risk = np.zeros_like(distances, dtype=np.float32)
    risk[distances <= inner] = base
    transition = (distances > inner) & (distances < outer)
    risk[transition] = base * (1.0 - ((distances[transition] - inner) / (outer - inner)))
    return np.clip(risk, 0.0, base).astype(np.float32)


def compute_transport_risk(roads_path: Path, rail_path: Path, ref_path: Path, working_dir: Path) -> np.ndarray:
    ref = gdal.Open(str(ref_path))
    shape_hw = (ref.RasterYSize, ref.RasterXSize)
    ref = None
    risks: List[np.ndarray] = [np.zeros(shape_hw, dtype=np.float32)]
    for group in ROAD_GROUPS:
        subset = filter_features_to_geojson(roads_path, working_dir / f"{group['name']}.geojson", "highway", group["filter"]["highway"])
        risks.append(compute_proximity_risk(subset, ref_path, working_dir, group["inner"], group["outer"], group["base"], group["name"]))
    rail_all = filter_features_to_geojson(rail_path, working_dir / "rail_regular.geojson", "railway", {"rail"})
    hsr = filter_features_to_geojson(rail_path, working_dir / "rail_hsr.geojson", "is_hsr", {"True", "true", "1"})
    risks.append(compute_proximity_risk(rail_all, ref_path, working_dir, 80.0, 250.0, 0.8, "rail_regular"))
    risks.append(compute_proximity_risk(hsr, ref_path, working_dir, 100.0, 300.0, 1.0, "rail_hsr"))
    return np.max(np.stack(risks, axis=0), axis=0).astype(np.float32)


def compute_sensitive_risk(sensitive_path: Path, ref_path: Path, working_dir: Path) -> np.ndarray:
    ref = gdal.Open(str(ref_path))
    shape_hw = (ref.RasterYSize, ref.RasterXSize)
    ref = None
    risks: List[np.ndarray] = [np.zeros(shape_hw, dtype=np.float32)]
    for rule in SENSITIVE_RULES:
        subset = filter_features_to_geojson(sensitive_path, working_dir / f"sensitive_{rule['kind']}.geojson", "kind", {rule["kind"]})
        risks.append(
            compute_proximity_risk(
                subset,
                ref_path,
                working_dir,
                inner=0.0,
                outer=rule["radius"],
                base=rule["base"],
                cache_name=f"sensitive_{rule['kind']}",
            )
        )
    return np.max(np.stack(risks, axis=0), axis=0).astype(np.float32)


def compute_landuse_risk(landuse_path: Path, ref_path: Path, working_dir: Path) -> np.ndarray:
    raster_path = working_dir / "landuse_score_100m.tif"
    rasterize_layer(landuse_path, raster_path, ref_path, attribute="score")
    array, _, _, _ = dataset_to_array(raster_path)
    return np.clip(array, 0.0, 1.0).astype(np.float32)


def compose_r_built(ghsl_norm: np.ndarray, wc_built: np.ndarray) -> np.ndarray:
    return np.clip(0.8 * ghsl_norm + 0.2 * wc_built, 0.0, 1.0).astype(np.float32)


def compose_r_land(worldcover_land: np.ndarray, osm_land: np.ndarray) -> np.ndarray:
    result = worldcover_land.copy()
    override = osm_land > 0
    result[override] = osm_land[override]
    return np.clip(result, 0.0, 1.0).astype(np.float32)


def compose_e_static(components: Dict[str, np.ndarray], mask: np.ndarray) -> np.ndarray:
    result = np.zeros_like(next(iter(components.values())), dtype=np.float32)
    for key, weight in E_STATIC_WEIGHTS.items():
        result += weight * components[key]
    result = np.clip(result, 0.0, 1.0)
    result[~mask] = DEFAULT_NODATA
    return result.astype(np.float32)


def apply_mask(array: np.ndarray, mask: np.ndarray, nodata: float) -> np.ndarray:
    out = array.astype(np.float32).copy()
    out[~mask] = nodata
    return out


def build_manifest_payload(
    *,
    city: str,
    args: argparse.Namespace,
    boundary_info: Dict[str, Any],
    buffered_bbox_wgs84: Tuple[float, float, float, float],
    worldpop_src: Path,
    ghsl_sources: Sequence[Path],
    worldcover_sources: Sequence[Path],
    boundary_path: Path,
    osm_paths: Dict[str, Path],
    raster_dir: Path,
    preview_png: Path,
    preview_html: Path,
    arrays_for_stats: Dict[str, np.ndarray],
    mask: np.ndarray,
    conservation: Dict[str, float],
) -> Dict[str, Any]:
    return {
        "city": city,
        "boundary_source": args.boundary_source,
        "boundary": boundary_info,
        "analysis_crs": args.analysis_crs,
        "resolution_m": args.resolution,
        "weights": E_STATIC_WEIGHTS,
        "sources": {
            "worldpop": {
                "year": args.worldpop_year,
                "url": WORLDPOP_CHN_2020_URL,
                "resolved_path": str(worldpop_src),
            },
            "ghsl": {
                "layer": args.ghsl_layer,
                "resolved_paths": [str(path) for path in ghsl_sources],
            },
            "worldcover": {
                "year": args.worldcover_year,
                "resolved_paths": [str(path) for path in worldcover_sources],
            },
            "osm": {
                "query_bbox": {
                    "south": buffered_bbox_wgs84[0],
                    "north": buffered_bbox_wgs84[1],
                    "west": buffered_bbox_wgs84[2],
                    "east": buffered_bbox_wgs84[3],
                },
                "query_time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
        },
        "outputs": {
            "boundary": str(boundary_path),
            "roads": str(osm_paths["roads"]),
            "rail": str(osm_paths["rail"]),
            "sensitive": str(osm_paths["sensitive"]),
            "landuse": str(osm_paths["landuse"]),
            "rasters": {
                "population_dasy": str(raster_dir / "hangzhou_population_dasy_100m.tif"),
                "r_pop": str(raster_dir / "hangzhou_r_pop_100m.tif"),
                "r_built": str(raster_dir / "hangzhou_r_built_100m.tif"),
                "r_landcover": str(raster_dir / "hangzhou_r_landcover_100m.tif"),
                "r_transport": str(raster_dir / "hangzhou_r_transport_100m.tif"),
                "r_sensitive": str(raster_dir / "hangzhou_r_sensitive_100m.tif"),
                "r_landuse_osm": str(raster_dir / "hangzhou_r_landuse_osm_100m.tif"),
                "r_land": str(raster_dir / "hangzhou_r_land_100m.tif"),
                "e_static": str(raster_dir / "hangzhou_e_static_100m.tif"),
            },
            "preview_png": str(preview_png) if preview_png.exists() else None,
            "preview_html": str(preview_html) if preview_html.exists() else None,
        },
        "stats": {
            "r_pop": array_stats(arrays_for_stats["r_pop"], mask),
            "r_built": array_stats(arrays_for_stats["r_built"], mask),
            "r_transport": array_stats(arrays_for_stats["r_transport"], mask),
            "r_sensitive": array_stats(arrays_for_stats["r_sensitive"], mask),
            "r_land": array_stats(arrays_for_stats["r_land"], mask),
            "e_static": array_stats(arrays_for_stats["e_static"], mask),
            "population_conservation": conservation,
        },
    }


def validate_population_conservation(pop_dasy: np.ndarray, coarse_worldpop_path: Path, ref_path: Path, mask: np.ndarray) -> Dict[str, float]:
    coarse, coarse_gt, _, _ = dataset_to_array(coarse_worldpop_path)
    ref_dataset = gdal.Open(str(ref_path))
    ref_gt = ref_dataset.GetGeoTransform()
    rows, cols = np.indices(pop_dasy.shape)
    xs = ref_gt[0] + (cols + 0.5) * ref_gt[1]
    ys = ref_gt[3] + (rows + 0.5) * ref_gt[5]
    coarse_cols = np.floor((xs - coarse_gt[0]) / coarse_gt[1]).astype(np.int64)
    coarse_rows = np.floor((ys - coarse_gt[3]) / coarse_gt[5]).astype(np.int64)
    valid = mask & (coarse_cols >= 0) & (coarse_cols < coarse.shape[1]) & (coarse_rows >= 0) & (coarse_rows < coarse.shape[0])
    flat_idx = coarse_rows * coarse.shape[1] + coarse_cols
    grouped = np.bincount(flat_idx[valid].ravel(), weights=pop_dasy[valid].ravel(), minlength=coarse.size)
    coverage = np.bincount(flat_idx[valid].ravel(), minlength=coarse.size)
    source = coarse.ravel()
    covered = (source > 0) & (coverage > 0)
    if not np.any(covered):
        return {
            "max_relative_error": 0.0,
            "mean_relative_error": 0.0,
            "covered_cell_count": 0,
            "excluded_outside_cell_count": int(np.count_nonzero(source > 0)),
        }
    relative = np.abs(grouped[covered] - source[covered]) / np.maximum(source[covered], 1e-6)
    return {
        "max_relative_error": float(np.max(relative)),
        "mean_relative_error": float(np.mean(relative)),
        "covered_cell_count": int(np.count_nonzero(covered)),
        "excluded_outside_cell_count": int(np.count_nonzero((source > 0) & (coverage == 0))),
    }


def make_preview_png(array: np.ndarray, mask: np.ndarray, out_path: Path) -> None:
    data = np.clip(np.where(mask, array, 0.0), 0.0, 1.0)
    rgba = np.zeros((data.shape[0], data.shape[1], 4), dtype=np.uint8)
    red = np.clip(255 * np.power(data, 0.65), 0, 255).astype(np.uint8)
    green = np.clip(255 * (1.0 - np.abs(data - 0.45) * 1.4), 0, 255).astype(np.uint8)
    blue = np.clip(255 * (1.0 - data), 0, 255).astype(np.uint8)
    rgba[..., 0] = red
    rgba[..., 1] = green
    rgba[..., 2] = blue
    rgba[..., 3] = np.where(mask, 220, 0).astype(np.uint8)
    image = Image.fromarray(rgba, mode="RGBA")
    if max(image.size) > DEFAULT_PREVIEW_WIDTH:
        image.thumbnail((DEFAULT_PREVIEW_WIDTH, DEFAULT_PREVIEW_WIDTH), Image.Resampling.BILINEAR)
    image.save(out_path)


def warp_raster_for_preview(src_path: Path, out_path: Path, max_dim: int = DEFAULT_PREVIEW_WIDTH) -> Path:
    src = gdal.Open(str(src_path))
    width = src.RasterXSize
    height = src.RasterYSize
    src = None
    scale = min(1.0, max_dim / max(width, height))
    target_width = max(1, int(round(width * scale)))
    target_height = max(1, int(round(height * scale)))
    options = gdal.WarpOptions(
        format="GTiff",
        dstSRS="EPSG:4326",
        width=target_width,
        height=target_height,
        resampleAlg="near",
        dstNodata=DEFAULT_NODATA,
        outputType=gdal.GDT_Float32,
        creationOptions=["COMPRESS=LZW", "TILED=YES"],
    )
    gdal.Warp(str(out_path), str(src_path), options=options)
    return out_path


def make_preview_layers(preview_dir: Path, raster_paths: Dict[str, Path]) -> Dict[str, Dict[str, Any]]:
    outputs: Dict[str, Dict[str, Any]] = {}
    overlay_bounds: Optional[Tuple[float, float, float, float]] = None
    for name, raster_path in raster_paths.items():
        slug = str(name).strip().lower().replace(" ", "_")
        warped_path = preview_dir / f"hangzhou_{slug}_preview_wgs84.tif"
        warp_raster_for_preview(raster_path, warped_path)
        array, _, _, nodata = dataset_to_array(warped_path)
        mask = np.isfinite(array)
        if np.isfinite(nodata):
            mask &= array != nodata
        out_path = preview_dir / f"hangzhou_{slug}_preview.png"
        make_preview_png(np.where(mask, array, 0.0), mask, out_path)
        if overlay_bounds is None:
            overlay_bounds = raster_bounds_wgs84(warped_path)
        outputs[name] = {
            "png_path": out_path,
            "image": np.asarray(Image.open(out_path)),
            "bounds": overlay_bounds,
        }
    return outputs


def raster_bounds_wgs84(ref_path: Path) -> Tuple[float, float, float, float]:
    dataset = gdal.Open(str(ref_path))
    geotransform = dataset.GetGeoTransform()
    minx = geotransform[0]
    maxy = geotransform[3]
    maxx = minx + dataset.RasterXSize * geotransform[1]
    miny = maxy + dataset.RasterYSize * geotransform[5]
    transformer = Transformer.from_crs(dataset.GetProjection(), "EPSG:4326", always_xy=True)
    dataset = None
    corners = [
        transformer.transform(minx, miny),
        transformer.transform(maxx, miny),
        transformer.transform(maxx, maxy),
        transformer.transform(minx, maxy),
    ]
    lons = [pt[0] for pt in corners]
    lats = [pt[1] for pt in corners]
    return min(lons), min(lats), max(lons), max(lats)


def make_preview_html(city: str, boundary_path: Path, preview_layers: Dict[str, Dict[str, Any]], layer_arrays: Dict[str, np.ndarray], mask: np.ndarray, out_path: Path) -> None:
    boundary = json.loads(boundary_path.read_text(encoding="utf-8"))
    feature = boundary["features"][0]
    geom = shape(feature["geometry"])
    center = [geom.centroid.y, geom.centroid.x]
    fmap = folium.Map(location=center, zoom_start=10, tiles="OpenStreetMap", control_scale=True)
    folium.GeoJson(boundary, name="boundary").add_to(fmap)
    for layer_name, layer_info in preview_layers.items():
        minx, miny, maxx, maxy = layer_info["bounds"]
        folium.raster_layers.ImageOverlay(
            image=layer_info["image"],
            bounds=[[miny, minx], [maxy, maxx]],
            name=layer_name,
            opacity=0.75,
            interactive=False,
            mercator_project=True,
            show=layer_name == "E_static",
        ).add_to(fmap)
    stats = array_stats(layer_arrays["E_static"], mask)
    layer_stats_html = "".join(
        f"<tr><td style='padding-right:8px'>{name}</td><td>{array_stats(array, mask)['mean']:.3f}</td><td>{array_stats(array, mask)['p95']:.3f}</td></tr>"
        for name, array in layer_arrays.items()
    )
    html = (
        f"<div style='position: fixed; top: 12px; left: 12px; z-index: 9999; "
        f"background: rgba(255,255,255,0.95); padding: 10px 12px; border-radius: 6px; "
        f"font-size: 13px; box-shadow: 0 2px 10px rgba(0,0,0,0.15);'>"
        f"<b>{city} E_static</b><br>"
        f"mean={stats['mean']:.3f}<br>p95={stats['p95']:.3f}<br>p99={stats['p99']:.3f}<br>"
        f"<div style='margin-top:6px'><table style='font-size:12px'><tr><th align='left'>Layer</th><th align='right'>Mean</th><th align='right' style='padding-left:8px'>P95</th></tr>{layer_stats_html}</table></div></div>"
    )
    fmap.get_root().html.add_child(folium.Element(html))
    folium.LayerControl(collapsed=False).add_to(fmap)
    fmap.save(str(out_path))


def get_epsg_wkt(crs_text: str) -> str:
    sr = osr.SpatialReference()
    sr.SetFromUserInput(crs_text)
    return sr.ExportToWkt()


def main() -> int:
    args = parse_args()
    city = args.city.strip()
    outdir = ensure_dir(Path(args.outdir).expanduser().resolve())
    raw_dir = ensure_dir(outdir / "raw")
    vector_dir = ensure_dir(outdir / "vector")
    raster_dir = ensure_dir(outdir / "rasters")
    preview_dir = ensure_dir(outdir / "preview")
    working_dir = ensure_dir(outdir / "_working")

    log(f"Resolving administrative boundary for {city} ...")
    boundary_wgs84, boundary_info = resolve_city_boundary(city)
    boundary_path = write_boundary(boundary_wgs84, boundary_info, vector_dir)

    to_analysis, to_wgs84 = build_transformers(args.analysis_crs)
    boundary_utm = project_geometry(boundary_wgs84, to_analysis)
    buffered_bbox_wgs84 = compute_buffer_bbox(boundary_utm, to_wgs84, BOUNDARY_BUFFER_M)

    log("Querying OSM layers ...")
    osm_paths = query_osm_layers(city, buffered_bbox_wgs84, args.osm_timeout, ensure_dir(raw_dir / "osm"), vector_dir)

    epsg_wkt = get_epsg_wkt(args.analysis_crs)
    ref_path = raster_dir / "hangzhou_reference_100m.tif"
    log("Creating 100m analysis grid ...")
    create_reference_raster(ref_path, boundary_utm.bounds, args.resolution, epsg_wkt, DEFAULT_NODATA)
    mask = create_boundary_mask(boundary_path, ref_path, working_dir / "boundary_mask.tif")

    log("Preparing WorldPop source ...")
    worldpop_src = locate_worldpop_source(ensure_dir(raw_dir / "worldpop"))
    worldpop_coarse = create_worldpop_coarse(worldpop_src, working_dir, boundary_utm.bounds, args.analysis_crs)

    log("Preparing GHSL source ...")
    ghsl_sources = resolve_ghsl_sources(args, raw_dir)
    ghsl_norm = create_ghsl_derivatives(ghsl_sources, working_dir, ref_path, mask)

    log("Preparing WorldCover source ...")
    worldcover_sources = resolve_worldcover_sources(args, raw_dir, buffered_bbox_wgs84)
    worldcover_land, worldcover_built = create_worldcover_derivatives(worldcover_sources, working_dir, ref_path, mask)

    log("Computing population dasymetric layer ...")
    population_dasy = compute_population_dasymetric(worldpop_coarse, ref_path, mask, ghsl_norm, worldcover_land)
    r_pop = compute_r_pop(population_dasy, mask)
    r_built = compose_r_built(ghsl_norm, worldcover_built)

    log("Computing transport and sensitive layers ...")
    r_transport = compute_transport_risk(osm_paths["roads"], osm_paths["rail"], ref_path, working_dir)
    r_sensitive = compute_sensitive_risk(osm_paths["sensitive"], ref_path, working_dir)

    log("Computing land layer ...")
    osm_land = compute_landuse_risk(osm_paths["landuse"], ref_path, working_dir)
    r_land = compose_r_land(worldcover_land, osm_land)

    components = {
        "r_pop": r_pop,
        "r_built": r_built,
        "r_transport": r_transport,
        "r_sensitive": r_sensitive,
        "r_land": r_land,
    }
    e_static = compose_e_static(components, mask)

    log("Writing rasters ...")
    save_array_as_tif(raster_dir / "hangzhou_population_dasy_100m.tif", ref_path, apply_mask(population_dasy, mask, DEFAULT_NODATA), DEFAULT_NODATA)
    save_array_as_tif(raster_dir / "hangzhou_r_pop_100m.tif", ref_path, apply_mask(r_pop, mask, DEFAULT_NODATA), DEFAULT_NODATA)
    save_array_as_tif(raster_dir / "hangzhou_r_built_100m.tif", ref_path, apply_mask(r_built, mask, DEFAULT_NODATA), DEFAULT_NODATA)
    save_array_as_tif(raster_dir / "hangzhou_r_landcover_100m.tif", ref_path, apply_mask(worldcover_land, mask, DEFAULT_NODATA), DEFAULT_NODATA)
    save_array_as_tif(raster_dir / "hangzhou_r_transport_100m.tif", ref_path, apply_mask(r_transport, mask, DEFAULT_NODATA), DEFAULT_NODATA)
    save_array_as_tif(raster_dir / "hangzhou_r_sensitive_100m.tif", ref_path, apply_mask(r_sensitive, mask, DEFAULT_NODATA), DEFAULT_NODATA)
    save_array_as_tif(raster_dir / "hangzhou_r_landuse_osm_100m.tif", ref_path, apply_mask(osm_land, mask, DEFAULT_NODATA), DEFAULT_NODATA)
    save_array_as_tif(raster_dir / "hangzhou_r_land_100m.tif", ref_path, apply_mask(r_land, mask, DEFAULT_NODATA), DEFAULT_NODATA)
    save_array_as_tif(raster_dir / "hangzhou_e_static_100m.tif", ref_path, e_static, DEFAULT_NODATA)

    preview_png = preview_dir / "hangzhou_e_static_preview.png"
    preview_html = preview_dir / "hangzhou_e_static_preview.html"
    if not args.skip_preview:
        log("Building preview files ...")
        preview_layer_arrays = {
            "E_static": np.where(mask, e_static, 0.0),
            "R_pop": np.where(mask, r_pop, 0.0),
            "R_built": np.where(mask, r_built, 0.0),
            "R_transport": np.where(mask, r_transport, 0.0),
            "R_sensitive": np.where(mask, r_sensitive, 0.0),
            "R_land": np.where(mask, r_land, 0.0),
            "R_landcover": np.where(mask, worldcover_land, 0.0),
            "R_landuse_osm": np.where(mask, osm_land, 0.0),
        }
        preview_layer_rasters = {
            "E_static": raster_dir / "hangzhou_e_static_100m.tif",
            "R_pop": raster_dir / "hangzhou_r_pop_100m.tif",
            "R_built": raster_dir / "hangzhou_r_built_100m.tif",
            "R_transport": raster_dir / "hangzhou_r_transport_100m.tif",
            "R_sensitive": raster_dir / "hangzhou_r_sensitive_100m.tif",
            "R_land": raster_dir / "hangzhou_r_land_100m.tif",
            "R_landcover": raster_dir / "hangzhou_r_landcover_100m.tif",
            "R_landuse_osm": raster_dir / "hangzhou_r_landuse_osm_100m.tif",
        }
        preview_layers = make_preview_layers(preview_dir, preview_layer_rasters)
        if "E_static" in preview_layers and not preview_layers["E_static"]["png_path"].samefile(preview_png):
            shutil.copy2(preview_layers["E_static"]["png_path"], preview_png)
        make_preview_html(city, boundary_path, preview_layers, preview_layer_arrays, mask, preview_html)

    conservation = validate_population_conservation(population_dasy, worldpop_coarse, ref_path, mask)
    manifest = build_manifest_payload(
        city=city,
        args=args,
        boundary_info=boundary_info,
        buffered_bbox_wgs84=buffered_bbox_wgs84,
        worldpop_src=worldpop_src,
        ghsl_sources=ghsl_sources,
        worldcover_sources=worldcover_sources,
        boundary_path=boundary_path,
        osm_paths=osm_paths,
        raster_dir=raster_dir,
        preview_png=preview_png,
        preview_html=preview_html,
        arrays_for_stats={
            "r_pop": r_pop,
            "r_built": r_built,
            "r_transport": r_transport,
            "r_sensitive": r_sensitive,
            "r_land": r_land,
            "e_static": np.where(mask, e_static, np.nan),
        },
        mask=mask,
        conservation=conservation,
    )
    manifest_path = outdir / "hangzhou_build_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Finished. Manifest written to {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
