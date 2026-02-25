#!/usr/bin/env python3
"""Download city POI/landuse/hydro/transport/population layers by city name."""

from __future__ import annotations

import argparse
import csv
import http.client
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib import error, parse, request

import folium
from folium.plugins import MarkerCluster

WORLDPOP_CHN_2020_URL = (
    "https://data.worldpop.org/GIS/Population_Density/"
    "Global_2000_2020_1km_UNadj/2020/CHN/chn_pd_2020_1km_UNadj.tif"
)

DEFAULT_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

DEFAULT_POI_KEYS = ["amenity", "shop", "tourism", "leisure", "office", "place", "military"]
DEFAULT_LANDUSE_KEYS = ["landuse", "natural", "leisure"]
DEFAULT_ROAD_FILTER = (
    "^(motorway|trunk|primary|secondary|tertiary|"
    "primary_link|secondary_link|tertiary_link|"
    "unclassified|residential|living_street|service|road)$"
)
DEFAULT_HYDRO_WATERWAY_VALUES = ["river", "canal", "stream", "ditch", "drain"]
DEFAULT_HYDRO_WATER_SURFACE_LANDUSE = ["reservoir", "basin"]


def slugify(value: str) -> str:
    value = value.strip().replace(" ", "_")
    safe = []
    for ch in value:
        if ch.isalnum() or ch in "-_":
            safe.append(ch)
        else:
            safe.append("-")
    result = "".join(safe).strip("-")
    return result or "city"


def city_name_candidates(city: str) -> List[str]:
    city = city.strip()
    if not city:
        return []
    if city.endswith("市"):
        return [city, city[:-1]]
    return [city, f"{city}市"]


def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 60) -> Any:
    req = request.Request(url, headers=headers or {"User-Agent": "city-data-downloader/1.0"})
    with request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_overpass(query: str, endpoints: Iterable[str], timeout: int = 180) -> Dict[str, Any]:
    body = parse.urlencode({"data": query}).encode("utf-8")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "city-data-downloader/1.0",
    }
    last_exc: Optional[Exception] = None
    for ep in endpoints:
        for attempt in range(3):
            try:
                req = request.Request(ep, data=body, headers=headers, method="POST")
                with request.urlopen(req, timeout=timeout + 30) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (
                error.HTTPError,
                error.URLError,
                TimeoutError,
                json.JSONDecodeError,
                http.client.IncompleteRead,
                http.client.RemoteDisconnected,
                ConnectionResetError,
            ) as exc:
                last_exc = exc
                if attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"All Overpass endpoints failed. Last error: {last_exc}")


def resolve_city_bbox(city: str) -> Tuple[float, float, float, float, str]:
    headers = {"User-Agent": "city-data-downloader/1.0"}
    for name in city_name_candidates(city):
        params = parse.urlencode(
            {
                "q": name,
                "format": "jsonv2",
                "countrycodes": "cn",
                "limit": 5,
                "addressdetails": 1,
            }
        )
        url = f"https://nominatim.openstreetmap.org/search?{params}"
        data = http_get_json(url, headers=headers, timeout=60)
        if not data:
            continue
        chosen = None
        for item in data:
            if item.get("class") == "boundary" and item.get("type") in {"administrative", "city"}:
                chosen = item
                break
        if chosen is None:
            chosen = data[0]
        bbox = chosen.get("boundingbox")
        if not bbox or len(bbox) != 4:
            continue
        south, north, west, east = map(float, bbox)
        return south, north, west, east, chosen.get("display_name", name)
    raise RuntimeError(f"Could not resolve city bbox for: {city}")


def build_poi_query(bbox: Tuple[float, float, float, float], poi_keys: List[str], timeout: int) -> str:
    south, north, west, east = bbox
    blocks = "\n".join([f"  nwr({south},{west},{north},{east})[{k}];" for k in poi_keys])
    return f"""
[out:json][timeout:{timeout}];
(
{blocks}
);
out center tags;
""".strip()


def build_landuse_query(
    bbox: Tuple[float, float, float, float], landuse_keys: List[str], timeout: int
) -> str:
    south, north, west, east = bbox
    blocks = "\n".join(
        [
            f"  way({south},{west},{north},{east})[{k}];\n  relation({south},{west},{north},{east})[{k}];"
            for k in landuse_keys
        ]
    )
    return f"""
[out:json][timeout:{timeout}];
(
{blocks}
);
out geom tags;
""".strip()


def build_transport_query(bbox: Tuple[float, float, float, float], timeout: int) -> str:
    south, north, west, east = bbox
    return f"""
[out:json][timeout:{timeout}];
(
  way({south},{west},{north},{east})[highway~"{DEFAULT_ROAD_FILTER}"];
  way({south},{west},{north},{east})[railway=rail][highspeed=yes];
  way({south},{west},{north},{east})[railway=rail][name~"高铁"];
  way({south},{west},{north},{east})[railway=rail][usage=main][maxspeed~"^(2[0-9]{{2}}|3[0-9]{{2}})$"];
);
out geom tags;
""".strip()


def build_hydro_query(bbox: Tuple[float, float, float, float], timeout: int) -> str:
    south, north, west, east = bbox
    waterway_regex = "|".join(DEFAULT_HYDRO_WATERWAY_VALUES)
    landuse_regex = "|".join(DEFAULT_HYDRO_WATER_SURFACE_LANDUSE)
    return f"""
[out:json][timeout:{timeout}];
(
  way({south},{west},{north},{east})[natural=water];
  relation({south},{west},{north},{east})[natural=water];
  way({south},{west},{north},{east})[water];
  relation({south},{west},{north},{east})[water];
  way({south},{west},{north},{east})[landuse~"^({landuse_regex})$"];
  relation({south},{west},{north},{east})[landuse~"^({landuse_regex})$"];
  way({south},{west},{north},{east})[waterway=riverbank];
  relation({south},{west},{north},{east})[waterway=riverbank];
  way({south},{west},{north},{east})[waterway~"^({waterway_regex})$"];
  relation({south},{west},{north},{east})[waterway~"^({waterway_regex})$"];
);
out geom tags;
""".strip()


def split_bbox_grid(
    bbox: Tuple[float, float, float, float],
    rows: int = 2,
    cols: int = 2,
) -> List[Tuple[float, float, float, float]]:
    south, north, west, east = bbox
    if rows <= 1 and cols <= 1:
        return [bbox]
    lat_step = (north - south) / max(rows, 1)
    lon_step = (east - west) / max(cols, 1)
    out: List[Tuple[float, float, float, float]] = []
    for r in range(rows):
        s = south + r * lat_step
        n = north if r == rows - 1 else south + (r + 1) * lat_step
        for c in range(cols):
            w = west + c * lon_step
            e = east if c == cols - 1 else west + (c + 1) * lon_step
            out.append((s, n, w, e))
    return out


def pick_coordinates(el: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    if "lat" in el and "lon" in el:
        return el.get("lat"), el.get("lon")
    center = el.get("center") or {}
    if "lat" in center and "lon" in center:
        return center.get("lat"), center.get("lon")
    return None, None


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def download_poi(
    city: str,
    outdir: Path,
    poi_keys: List[str],
    timeout: int,
    bbox: Tuple[float, float, float, float],
) -> Dict[str, str]:
    query = build_poi_query(bbox, poi_keys, timeout)
    data = run_overpass(query, DEFAULT_OVERPASS_ENDPOINTS, timeout=timeout)
    elements = data.get("elements") or []
    if not elements:
        raise RuntimeError(f"No POI results for city: {city}")

    rows: List[Dict[str, Any]] = []
    for el in elements:
        lat, lon = pick_coordinates(el)
        tags = el.get("tags") or {}
        poi_type = ""
        for key in poi_keys:
            if key in tags:
                poi_type = f"{key}:{tags.get(key)}"
                break
        rows.append(
            {
                "id": f"{el.get('type', 'obj')}/{el.get('id', '')}",
                "osm_type": el.get("type", ""),
                "osm_id": el.get("id", ""),
                "name": tags.get("name", ""),
                "poi_type": poi_type,
                "lat": lat,
                "lon": lon,
                "raw_tags": json.dumps(tags, ensure_ascii=False),
            }
        )

    ts = time.strftime("%Y%m%d_%H%M%S")
    slug = slugify(city)
    csv_path = outdir / f"{slug}_poi_{ts}.csv"
    geojson_path = outdir / f"{slug}_poi_{ts}.geojson"
    write_csv(
        csv_path,
        rows,
        ["id", "osm_type", "osm_id", "name", "poi_type", "lat", "lon", "raw_tags"],
    )

    features = []
    for row in rows:
        if row["lat"] is None or row["lon"] is None:
            continue
        props = dict(row)
        props.pop("lat", None)
        props.pop("lon", None)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [row["lon"], row["lat"]]},
                "properties": props,
            }
        )
    geojson_path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False),
        encoding="utf-8",
    )
    return {"csv": str(csv_path), "geojson": str(geojson_path), "count": str(len(rows))}


def geometry_from_overpass(el: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if "lat" in el and "lon" in el:
        return {"type": "Point", "coordinates": [el["lon"], el["lat"]]}
    geom = el.get("geometry") or []
    if not geom:
        return None
    coords = [[pt["lon"], pt["lat"]] for pt in geom if "lon" in pt and "lat" in pt]
    if len(coords) < 2:
        return None
    if coords[0] == coords[-1] and len(coords) >= 4:
        return {"type": "Polygon", "coordinates": [coords]}
    return {"type": "LineString", "coordinates": coords}


def _relation_member_geometries(el: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    members = el.get("members") or []
    for m in members:
        geom = m.get("geometry") or []
        coords = [[pt["lon"], pt["lat"]] for pt in geom if "lon" in pt and "lat" in pt]
        if len(coords) < 2:
            continue
        if coords[0] == coords[-1] and len(coords) >= 4:
            out.append({"type": "Polygon", "coordinates": [coords]})
        else:
            out.append({"type": "LineString", "coordinates": coords})
    return out


def _is_water_surface(tags: Dict[str, Any]) -> bool:
    if str(tags.get("natural", "")).lower() == "water":
        return True
    if str(tags.get("water", "")).strip():
        return True
    if str(tags.get("landuse", "")).lower() in set(DEFAULT_HYDRO_WATER_SURFACE_LANDUSE):
        return True
    return str(tags.get("waterway", "")).lower() == "riverbank"


def _is_waterway(tags: Dict[str, Any]) -> bool:
    return str(tags.get("waterway", "")).lower() in set(DEFAULT_HYDRO_WATERWAY_VALUES)


def _is_hsr_way(tags: Dict[str, Any]) -> bool:
    if str(tags.get("highspeed", "")).lower() in {"yes", "true", "1"}:
        return True
    if "高铁" in str(tags.get("name", "")):
        return True
    usage = str(tags.get("usage", "")).lower()
    maxspeed = str(tags.get("maxspeed", ""))
    if usage == "main":
        m = re.search(r"(\\d+)", maxspeed)
        if m and int(m.group(1)) >= 200:
            return True
    return False


def download_transport(
    city: str,
    outdir: Path,
    timeout: int,
    bbox: Tuple[float, float, float, float],
) -> Dict[str, str]:
    elements: List[Dict[str, Any]] = []
    seen_elems: set[Tuple[Any, Any]] = set()
    for tile_bbox in split_bbox_grid(bbox, rows=2, cols=2):
        query = build_transport_query(tile_bbox, timeout)
        data = run_overpass(query, DEFAULT_OVERPASS_ENDPOINTS, timeout=timeout)
        for el in data.get("elements") or []:
            ek = (el.get("type"), el.get("id"))
            if ek in seen_elems:
                continue
            seen_elems.add(ek)
            elements.append(el)
    if not elements:
        raise RuntimeError(f"No transport results for city: {city}")

    roads: List[Dict[str, Any]] = []
    hsr: List[Dict[str, Any]] = []
    seen = set()
    for el in elements:
        if el.get("type") != "way":
            continue
        way_id = el.get("id")
        if way_id in seen:
            continue
        seen.add(way_id)
        tags = el.get("tags") or {}
        geom = geometry_from_overpass(el)
        if not geom or geom.get("type") != "LineString":
            continue
        props = {
            "id": f"way/{way_id}",
            "name": tags.get("name", ""),
            "highway": tags.get("highway", ""),
            "railway": tags.get("railway", ""),
            "highspeed": str(tags.get("highspeed", "")),
            "usage": str(tags.get("usage", "")),
            "maxspeed": str(tags.get("maxspeed", "")),
            "raw_tags": tags,
        }
        if "highway" in tags:
            roads.append({"type": "Feature", "geometry": geom, "properties": props})
        elif tags.get("railway") == "rail" and _is_hsr_way(tags):
            hsr.append({"type": "Feature", "geometry": geom, "properties": props})

    ts = time.strftime("%Y%m%d_%H%M%S")
    slug = slugify(city)
    roads_geojson = outdir / f"{slug}_roads_{ts}.geojson"
    hsr_geojson = outdir / f"{slug}_hsr_{ts}.geojson"
    roads_geojson.write_text(
        json.dumps({"type": "FeatureCollection", "features": roads}, ensure_ascii=False),
        encoding="utf-8",
    )
    hsr_geojson.write_text(
        json.dumps({"type": "FeatureCollection", "features": hsr}, ensure_ascii=False),
        encoding="utf-8",
    )
    return {
        "roads_geojson": str(roads_geojson),
        "hsr_geojson": str(hsr_geojson),
        "roads_count": str(len(roads)),
        "hsr_count": str(len(hsr)),
    }


def download_landuse(
    city: str,
    outdir: Path,
    landuse_keys: List[str],
    timeout: int,
    bbox: Tuple[float, float, float, float],
) -> Dict[str, str]:
    elements: List[Dict[str, Any]] = []
    seen_elems: set[Tuple[Any, Any]] = set()
    for tile_bbox in split_bbox_grid(bbox, rows=2, cols=2):
        query = build_landuse_query(tile_bbox, landuse_keys, timeout)
        data = run_overpass(query, DEFAULT_OVERPASS_ENDPOINTS, timeout=timeout)
        for el in data.get("elements") or []:
            ek = (el.get("type"), el.get("id"))
            if ek in seen_elems:
                continue
            seen_elems.add(ek)
            elements.append(el)
    if not elements:
        raise RuntimeError(f"No landuse results for city: {city}")

    features = []
    summary: Dict[str, int] = {}
    for el in elements:
        tags = el.get("tags") or {}
        geom = geometry_from_overpass(el)
        if geom is None:
            continue
        tag_key = ""
        tag_val = ""
        for k in landuse_keys:
            if k in tags:
                tag_key = k
                tag_val = str(tags.get(k, ""))
                break
        summary_key = f"{tag_key}:{tag_val}" if tag_key else "unknown:unknown"
        summary[summary_key] = summary.get(summary_key, 0) + 1
        features.append(
            {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "id": f"{el.get('type', 'obj')}/{el.get('id', '')}",
                    "name": tags.get("name", ""),
                    "landuse_type": summary_key,
                    "raw_tags": tags,
                },
            }
        )

    ts = time.strftime("%Y%m%d_%H%M%S")
    slug = slugify(city)
    geojson_path = outdir / f"{slug}_landuse_{ts}.geojson"
    summary_csv = outdir / f"{slug}_landuse_summary_{ts}.csv"
    geojson_path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False),
        encoding="utf-8",
    )
    rows = [{"landuse_type": k, "count": v} for k, v in sorted(summary.items(), key=lambda x: -x[1])]
    write_csv(summary_csv, rows, ["landuse_type", "count"])
    return {
        "geojson": str(geojson_path),
        "summary_csv": str(summary_csv),
        "count": str(len(features)),
    }


def download_hydro(
    city: str,
    outdir: Path,
    timeout: int,
    bbox: Tuple[float, float, float, float],
) -> Dict[str, str]:
    elements: List[Dict[str, Any]] = []
    seen_elems: set[Tuple[Any, Any]] = set()
    for tile_bbox in split_bbox_grid(bbox, rows=2, cols=2):
        query = build_hydro_query(tile_bbox, timeout)
        data = run_overpass(query, DEFAULT_OVERPASS_ENDPOINTS, timeout=timeout)
        for el in data.get("elements") or []:
            ek = (el.get("type"), el.get("id"))
            if ek in seen_elems:
                continue
            seen_elems.add(ek)
            elements.append(el)
    if not elements:
        raise RuntimeError(f"No hydro results for city: {city}")

    water_surface_features: List[Dict[str, Any]] = []
    waterway_features: List[Dict[str, Any]] = []
    seen: set[Tuple[Any, str]] = set()

    for el in elements:
        tags = el.get("tags") or {}
        is_surface = _is_water_surface(tags)
        is_way = _is_waterway(tags)
        if not is_surface and not is_way:
            continue

        layer = "waterway" if is_way and not is_surface else "water_surface"
        geoms: List[Dict[str, Any]] = []
        g = geometry_from_overpass(el)
        if g is not None:
            geoms.append(g)
        elif el.get("type") == "relation":
            geoms.extend(_relation_member_geometries(el))
        if not geoms:
            continue

        elid = f"{el.get('type', 'obj')}/{el.get('id', '')}"
        subtype = ""
        if layer == "waterway":
            subtype = str(tags.get("waterway", ""))
        elif tags.get("water"):
            subtype = str(tags.get("water", ""))
        elif tags.get("natural"):
            subtype = str(tags.get("natural", ""))
        else:
            subtype = str(tags.get("landuse", ""))

        props = {
            "id": elid,
            "name": tags.get("name", ""),
            "layer_type": layer,
            "subtype": subtype,
            "raw_tags": tags,
        }

        for idx, geom in enumerate(geoms):
            geom_key = json.dumps(geom, ensure_ascii=False, sort_keys=True)
            uniq = (elid, geom_key)
            if uniq in seen:
                continue
            seen.add(uniq)
            feat = {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    **props,
                    "part_index": idx if len(geoms) > 1 else 0,
                },
            }
            if layer == "waterway":
                waterway_features.append(feat)
            else:
                water_surface_features.append(feat)

    ts = time.strftime("%Y%m%d_%H%M%S")
    slug = slugify(city)
    water_surface_geojson = outdir / f"{slug}_water_surface_{ts}.geojson"
    waterway_geojson = outdir / f"{slug}_waterway_{ts}.geojson"
    water_surface_geojson.write_text(
        json.dumps({"type": "FeatureCollection", "features": water_surface_features}, ensure_ascii=False),
        encoding="utf-8",
    )
    waterway_geojson.write_text(
        json.dumps({"type": "FeatureCollection", "features": waterway_features}, ensure_ascii=False),
        encoding="utf-8",
    )
    return {
        "water_surface_geojson": str(water_surface_geojson),
        "waterway_geojson": str(waterway_geojson),
        "water_surface_count": str(len(water_surface_features)),
        "waterway_count": str(len(waterway_features)),
    }


def ensure_worldpop_tif(pop_dir: Path) -> Path:
    repo_tif = Path(__file__).resolve().parents[3] / "data" / "population" / "chn_pd_2020_1km.tif"
    if repo_tif.exists():
        return repo_tif
    tif = pop_dir / "chn_pd_2020_1km.tif"
    if tif.exists():
        return tif
    print(f"Downloading WorldPop source: {tif}")
    request.urlretrieve(WORLDPOP_CHN_2020_URL, tif)
    return tif


def ensure_color_ramp(path: Path) -> None:
    if path.exists():
        return
    path.write_text(
        "\n".join(
            [
                "0 255 255 178",
                "100 254 204 92",
                "500 253 141 60",
                "1000 252 78 42",
                "5000 227 26 28",
                "10000 189 0 38",
                "50000 128 0 38",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def run_cmd(cmd: List[str]) -> None:
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Missing required command: {cmd[0]}") from exc


def build_population_tiles(
    city: str,
    outdir: Path,
    bbox: Tuple[float, float, float, float],
    zoom: str,
) -> Dict[str, str]:
    south, north, west, east = bbox
    pop_dir = outdir / "population"
    pop_dir.mkdir(parents=True, exist_ok=True)

    source_tif = ensure_worldpop_tif(pop_dir)
    color_ramp = pop_dir / "color_ramp.txt"
    ensure_color_ramp(color_ramp)

    slug = slugify(city)
    clipped_tif = pop_dir / f"{slug}_pd.tif"
    colored_tif = pop_dir / f"{slug}_colored.tif"
    tiles_dir = pop_dir / f"tiles_{slug}"

    run_cmd(
        [
            "gdal_translate",
            "-projwin",
            str(west),
            str(north),
            str(east),
            str(south),
            str(source_tif),
            str(clipped_tif),
        ]
    )
    run_cmd(["gdaldem", "color-relief", str(clipped_tif), str(color_ramp), str(colored_tif), "-alpha"])
    run_cmd(["gdal2tiles.py", "-z", zoom, "-p", "mercator", "--processes=4", str(colored_tif), str(tiles_dir)])

    return {
        "clipped_tif": str(clipped_tif),
        "colored_tif": str(colored_tif),
        "tiles_dir": str(tiles_dir),
    }


def build_combined_map(
    city: str,
    outdir: Path,
    bbox: Tuple[float, float, float, float],
    poi_geojson: Path,
    landuse_geojson: Path,
    water_surface_geojson: Path,
    waterway_geojson: Path,
    population_tiles_dir: Path,
    roads_geojson: Path,
    hsr_geojson: Path,
) -> Path:
    south, north, west, east = bbox
    center_lat = (south + north) / 2.0
    center_lon = (west + east) / 2.0

    m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles=None, control_scale=True)

    folium.TileLayer("OpenStreetMap", name="普通地图", overlay=False, control=True).add_to(m)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri World Imagery",
        name="卫星图",
        overlay=False,
        control=True,
    ).add_to(m)

    pop_tiles_uri = population_tiles_dir.resolve().as_uri()
    folium.TileLayer(
        tiles=f"{pop_tiles_uri}/{{z}}/{{x}}/{{y}}.png",
        attr="WorldPop 2020 (Local Tiles)",
        name="人口密度",
        overlay=True,
        control=True,
        opacity=0.75,
        tms=True,
    ).add_to(m)

    landuse_obj = json.loads(landuse_geojson.read_text(encoding="utf-8"))
    folium.GeoJson(
        landuse_obj,
        name="土地利用",
        style_function=lambda _: {
            "color": "#f57c00",
            "weight": 1,
            "fillColor": "#ffb74d",
            "fillOpacity": 0.25,
        },
        tooltip=folium.GeoJsonTooltip(fields=["landuse_type", "name"], aliases=["类型", "名称"]),
    ).add_to(m)

    water_surface_obj = json.loads(water_surface_geojson.read_text(encoding="utf-8"))
    folium.GeoJson(
        water_surface_obj,
        name="水面分布",
        style_function=lambda _: {
            "color": "#0288d1",
            "weight": 1.2,
            "fillColor": "#4fc3f7",
            "fillOpacity": 0.28,
        },
    ).add_to(m)

    waterway_obj = json.loads(waterway_geojson.read_text(encoding="utf-8"))
    folium.GeoJson(
        waterway_obj,
        name="河道分布",
        style_function=lambda _: {"color": "#0277bd", "weight": 2, "opacity": 0.92},
    ).add_to(m)

    roads_obj = json.loads(roads_geojson.read_text(encoding="utf-8"))
    folium.GeoJson(
        roads_obj,
        name="道路",
        style_function=lambda _: {"color": "#1e88e5", "weight": 1.6, "opacity": 0.9},
    ).add_to(m)

    hsr_obj = json.loads(hsr_geojson.read_text(encoding="utf-8"))
    folium.GeoJson(
        hsr_obj,
        name="高铁",
        style_function=lambda _: {"color": "#e53935", "weight": 2.2, "opacity": 0.95},
    ).add_to(m)

    poi_obj = json.loads(poi_geojson.read_text(encoding="utf-8"))
    marker_cluster = MarkerCluster(name="POI").add_to(m)
    for feature in poi_obj.get("features", []):
        geometry = feature.get("geometry") or {}
        if geometry.get("type") != "Point":
            continue
        coords = geometry.get("coordinates") or []
        if len(coords) != 2:
            continue
        lon, lat = coords
        props = feature.get("properties") or {}
        name = props.get("name") or "未命名POI"
        poi_type = props.get("poi_type") or ""
        popup_html = f"<b>{name}</b><br>类型: {poi_type}"
        folium.CircleMarker(
            location=[lat, lon],
            radius=4,
            color="#1565c0",
            fill=True,
            fill_color="#1e88e5",
            fill_opacity=0.9,
            weight=1,
            popup=folium.Popup(popup_html, max_width=280),
        ).add_to(marker_cluster)

    folium.Rectangle(
        bounds=[[south, west], [north, east]],
        color="#2e7d32",
        weight=2,
        fill=False,
        tooltip=f"{city} 研究范围",
    ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    map_path = outdir / f"{slugify(city)}_city_layers_map.html"
    m.save(str(map_path))
    return map_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download city POI + landuse + hydro + population tiles."
    )
    p.add_argument("--city", required=True, help="Chinese city name, for example 合肥市")
    p.add_argument("--outdir", default="./output/city_data", help="Output root dir")
    p.add_argument("--poi-keys", default=",".join(DEFAULT_POI_KEYS), help="POI keys, comma-separated")
    p.add_argument(
        "--landuse-keys",
        default=",".join(DEFAULT_LANDUSE_KEYS),
        help="Landuse keys, comma-separated",
    )
    p.add_argument("--timeout", type=int, default=180, help="Overpass timeout (seconds)")
    p.add_argument("--zoom", default="8-14", help="gdal2tiles zoom range")
    p.add_argument("--bbox", default="", help="Override bbox as south,north,west,east")
    p.add_argument("--skip-poi", action="store_true", help="Skip POI download")
    p.add_argument("--skip-landuse", action="store_true", help="Skip landuse download")
    p.add_argument("--skip-hydro", action="store_true", help="Skip hydro (water surface/waterway) download")
    p.add_argument("--skip-population", action="store_true", help="Skip population tiles")
    p.add_argument("--skip-transport", action="store_true", help="Skip roads/high-speed-rail download")
    p.add_argument("--skip-map", action="store_true", help="Skip combined map HTML generation")
    return p.parse_args()


def parse_bbox_override(raw: str) -> Tuple[float, float, float, float]:
    vals = [x.strip() for x in raw.split(",") if x.strip()]
    if len(vals) != 4:
        raise ValueError("--bbox must be south,north,west,east")
    south, north, west, east = [float(x) for x in vals]
    if not (south < north and west < east):
        raise ValueError("--bbox must satisfy south<north and west<east")
    return south, north, west, east


def main() -> int:
    args = parse_args()
    city = args.city.strip()
    if not city:
        print("ERROR: --city cannot be empty", file=sys.stderr)
        return 2

    outdir = Path(args.outdir).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    poi_keys = [x.strip() for x in args.poi_keys.split(",") if x.strip()]
    landuse_keys = [x.strip() for x in args.landuse_keys.split(",") if x.strip()]

    south, north, west, east, display_name = resolve_city_bbox(city)
    if args.bbox.strip():
        south, north, west, east = parse_bbox_override(args.bbox.strip())
        display_name = f"{display_name} (bbox override)"
    print(f"Resolved city: {display_name}", flush=True)
    print(f"BBox: south={south}, north={north}, west={west}, east={east}", flush=True)

    result: Dict[str, Any] = {
        "city": city,
        "resolved_display_name": display_name,
        "bbox": {"south": south, "north": north, "west": west, "east": east},
        "outputs": {},
    }

    try:
        if not args.skip_poi:
            poi_out = outdir / "poi"
            poi_out.mkdir(parents=True, exist_ok=True)
            print("Downloading POI ...", flush=True)
            result["outputs"]["poi"] = download_poi(
                city,
                poi_out,
                poi_keys,
                args.timeout,
                (south, north, west, east),
            )

        if not args.skip_landuse:
            landuse_out = outdir / "landuse"
            landuse_out.mkdir(parents=True, exist_ok=True)
            print("Downloading landuse ...", flush=True)
            result["outputs"]["landuse"] = download_landuse(
                city,
                landuse_out,
                landuse_keys,
                args.timeout,
                (south, north, west, east),
            )

        if not args.skip_hydro:
            hydro_out = outdir / "hydro"
            hydro_out.mkdir(parents=True, exist_ok=True)
            print("Downloading hydro layers (water surface + waterway) ...", flush=True)
            result["outputs"]["hydro"] = download_hydro(
                city=city,
                outdir=hydro_out,
                timeout=args.timeout,
                bbox=(south, north, west, east),
            )

        if not args.skip_population:
            print("Building population tiles ...", flush=True)
            result["outputs"]["population"] = build_population_tiles(
                city,
                outdir,
                (south, north, west, east),
                args.zoom,
            )

        if not args.skip_transport:
            transport_out = outdir / "transport"
            transport_out.mkdir(parents=True, exist_ok=True)
            print("Downloading transport (roads + high-speed rail) ...", flush=True)
            result["outputs"]["transport"] = download_transport(
                city=city,
                outdir=transport_out,
                timeout=args.timeout,
                bbox=(south, north, west, east),
            )

        if (
            not args.skip_map
            and "poi" in result["outputs"]
            and "landuse" in result["outputs"]
            and "hydro" in result["outputs"]
            and "population" in result["outputs"]
            and "transport" in result["outputs"]
        ):
            print("Building combined map ...", flush=True)
            map_path = build_combined_map(
                city=city,
                outdir=outdir,
                bbox=(south, north, west, east),
                poi_geojson=Path(result["outputs"]["poi"]["geojson"]),
                landuse_geojson=Path(result["outputs"]["landuse"]["geojson"]),
                water_surface_geojson=Path(result["outputs"]["hydro"]["water_surface_geojson"]),
                waterway_geojson=Path(result["outputs"]["hydro"]["waterway_geojson"]),
                population_tiles_dir=Path(result["outputs"]["population"]["tiles_dir"]),
                roads_geojson=Path(result["outputs"]["transport"]["roads_geojson"]),
                hsr_geojson=Path(result["outputs"]["transport"]["hsr_geojson"]),
            )
            result["outputs"]["combined_map"] = {"html": str(map_path)}
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        summary_path = outdir / "download_summary.json"
        summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Partial summary: {summary_path}", file=sys.stderr)
        return 1

    summary_path = outdir / "download_summary.json"
    summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
