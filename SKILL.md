---
name: city-data-downloader
description: Download China city datasets by city name, including POI (OSM), landuse (OSM), hydro layers (water surface + waterways), and population density tiles (WorldPop + gdal2tiles). Use when the user wants one-command city data preparation for risk assessment.
---

# City Data Downloader

Use this skill when you need city-level baseline data for risk analysis.

Input: Chinese city name (for example `合肥市`, `上海市`, `深圳市`).

Output:
- POI CSV + GeoJSON
- Landuse GeoJSON + summary CSV
- Hydro GeoJSONs:
  - Water surface (`*_water_surface_*.geojson`)
  - Waterway (`*_waterway_*.geojson`)
- Population clipped TIFF + colored TIFF + tile pyramid directory
- Transport GeoJSONs:
  - Roads (`*_roads_*.geojson`, covering trunk + secondary/local connector road classes)
  - High-speed rail (`*_hsr_*.geojson`)
- Combined city map HTML (multi-layer overlays + base-map switch)
- Static exposure basemap package:
  - `hangzhou_e_static_100m.tif` and intermediate `R_*` rasters
  - `hangzhou_build_manifest.json`
  - Preview PNG + HTML

## Quick Start

```bash
python3 skills/city-data-downloader/scripts/download_city_data.py \
  --city 合肥市 \
  --outdir ./output/city_data
```

```bash
python3 skills/city-data-downloader/scripts/build_static_exposure_basemap.py \
  --city 杭州市 \
  --outdir ./output/exposure_static/hangzhou \
  --ghsl-src /path/to/GHSL_BUILT_S.tif \
  --worldcover-src /path/to/ESA_WorldCover_Map.tif
```

## Workflow

1. Resolve city bounding box from Nominatim (China-only).
2. Query Overpass for city POI data (including `place` and `military`) and export CSV/GeoJSON.
3. Query Overpass for city landuse/natural/leisure features and export GeoJSON + summary.
4. Query Overpass for hydro layers and export two files: water surface + waterway.
5. Download WorldPop China raster when missing.
6. Clip population raster by city bbox.
7. Render colorized population TIFF and build local tiles with `gdal2tiles.py`.
8. Query transport layers and export two files: roads + high-speed rail.
9. Build one HTML map that includes:
   - POI layer (toggle)
   - Landuse layer (toggle)
   - Water surface layer (toggle)
   - Waterway layer (toggle)
   - Population layer (toggle)
   - Roads layer (toggle)
   - High-speed rail layer (toggle)
   - Base-map switch: normal map and satellite map

## Static Exposure Workflow

Use `build_static_exposure_basemap.py` when you need a city-wide static ground-exposure raster for risk modeling.

It does the following:
1. Resolve the city administrative boundary from OSM/Nominatim.
2. Query OSM roads, rail, sensitive POIs, and landuse within `bbox + 2km`.
3. Build a `100m` analysis grid in a projected CRS (default `EPSG:32651`).
4. Prepare `WorldPop`, `GHSL`, and `WorldCover` inputs.
5. Compute:
   - `hangzhou_population_dasy_100m.tif`
   - `hangzhou_r_pop_100m.tif`
   - `hangzhou_r_built_100m.tif`
   - `hangzhou_r_transport_100m.tif`
   - `hangzhou_r_sensitive_100m.tif`
   - `hangzhou_r_land_100m.tif`
6. Compose `hangzhou_e_static_100m.tif`.
7. Write manifest and preview files.

Notes:
- `WorldPop` is downloaded automatically if missing.
- `GHSL` should usually be provided explicitly via `--ghsl-src` or `GHSL_BUILT_S_URL`.
- `WorldCover` can be provided explicitly via `--worldcover-src`; if omitted, the script attempts official STAC discovery and local caching.
- The script depends on `GDAL`, `numpy`, `shapely`, `pyproj`, `Pillow`, and `folium`.

## Main Arguments

- `--city` (required): city name, prefer official Chinese form.
- `--outdir`: output root directory. Default `./output/city_data`.
- `--poi-keys`: OSM POI keys. Default `amenity,shop,tourism,leisure,office,place,military`.
- `--landuse-keys`: OSM landuse keys. Default `landuse,natural,leisure`.
- `--skip-hydro`: skip hydro download (water surface/waterway).
- `--zoom`: gdal2tiles zoom range. Default `8-14`.
- `--skip-population`: only fetch POI + landuse.
- `--skip-poi`: skip POI.
- `--skip-landuse`: skip landuse.
- `--skip-transport`: skip roads/high-speed-rail download.
- `--skip-map`: skip combined HTML map generation.
- `--ghsl-src`: path or URL to GHSL built-up raster(s), repeatable.
- `--worldcover-src`: path or URL to WorldCover raster(s), repeatable.
- `--resolution`: target output resolution in meters for the static exposure raster.

## Notes

- Overpass public endpoints may throttle large cities; rerun if needed.
- Population tiling depends on local GDAL tools:
  - `gdal_translate`
  - `gdaldem`
  - `gdal2tiles.py`
