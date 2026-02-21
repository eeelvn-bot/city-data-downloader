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
  - Roads (`*_roads_*.geojson`)
  - High-speed rail (`*_hsr_*.geojson`)
- Combined city map HTML (multi-layer overlays + base-map switch)

## Quick Start

```bash
python3 skills/city-data-downloader/scripts/download_city_data.py \
  --city 合肥市 \
  --outdir ./output/city_data
```

## Workflow

1. Resolve city bounding box from Nominatim (China-only).
2. Query Overpass for city POI data and export CSV/GeoJSON.
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

## Main Arguments

- `--city` (required): city name, prefer official Chinese form.
- `--outdir`: output root directory. Default `./output/city_data`.
- `--poi-keys`: OSM POI keys. Default `amenity,shop,tourism,leisure,office`.
- `--landuse-keys`: OSM landuse keys. Default `landuse,natural,leisure`.
- `--skip-hydro`: skip hydro download (water surface/waterway).
- `--zoom`: gdal2tiles zoom range. Default `8-14`.
- `--skip-population`: only fetch POI + landuse.
- `--skip-poi`: skip POI.
- `--skip-landuse`: skip landuse.
- `--skip-transport`: skip roads/high-speed-rail download.
- `--skip-map`: skip combined HTML map generation.

## Notes

- Overpass public endpoints may throttle large cities; rerun if needed.
- Population tiling depends on local GDAL tools:
  - `gdal_translate`
  - `gdaldem`
  - `gdal2tiles.py`
