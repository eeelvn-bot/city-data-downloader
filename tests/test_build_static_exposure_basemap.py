import argparse
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import numpy as np
from osgeo import gdal, ogr, osr

SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from build_static_exposure_basemap import (  # noqa: E402
    DEFAULT_NODATA,
    build_manifest_payload,
    compute_population_dasymetric,
    compute_proximity_risk,
    compose_e_static,
    create_reference_raster,
    expand_raster_sources,
    get_epsg_wkt,
    make_preview_html,
    make_preview_layers,
    raster_bounds_wgs84,
    save_array_as_tif,
    validate_population_conservation,
)


class StaticExposureBasemapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _create_ref_raster(self, name: str, bounds: tuple[float, float, float, float], resolution: float) -> Path:
        path = self.root / name
        create_reference_raster(path, bounds, resolution, get_epsg_wkt("EPSG:32651"), DEFAULT_NODATA)
        return path

    def _save_array(self, path: Path, array: np.ndarray, geotransform: tuple[float, float, float, float, float, float]) -> None:
        driver = gdal.GetDriverByName("GTiff")
        dataset = driver.Create(str(path), array.shape[1], array.shape[0], 1, gdal.GDT_Float32)
        dataset.SetGeoTransform(geotransform)
        dataset.SetProjection(get_epsg_wkt("EPSG:32651"))
        band = dataset.GetRasterBand(1)
        band.WriteArray(array.astype(np.float32))
        band.SetNoDataValue(0.0)
        band.FlushCache()
        dataset.FlushCache()
        dataset = None

    def _create_line_vector(self, path: Path, x: float, y0: float, y1: float) -> Path:
        driver = ogr.GetDriverByName("GPKG")
        datasource = driver.CreateDataSource(str(path))
        spatial_ref = osr.SpatialReference()
        spatial_ref.SetFromUserInput("EPSG:32651")
        layer = datasource.CreateLayer("lines", srs=spatial_ref, geom_type=ogr.wkbLineString)
        feature = ogr.Feature(layer.GetLayerDefn())
        line = ogr.Geometry(ogr.wkbLineString)
        line.AddPoint_2D(x, y0)
        line.AddPoint_2D(x, y1)
        feature.SetGeometry(line)
        layer.CreateFeature(feature)
        feature = None
        datasource = None
        return path

    def test_population_dasymetric_conserves_population(self) -> None:
        ref_path = self._create_ref_raster("ref.tif", (0.0, 0.0, 2000.0, 2000.0), 100.0)
        coarse_path = self.root / "coarse.tif"
        coarse_array = np.array([[100.0, 200.0], [300.0, 400.0]], dtype=np.float32)
        coarse_gt = (0.0, 1000.0, 0.0, 2000.0, 0.0, -1000.0)
        self._save_array(coarse_path, coarse_array, coarse_gt)

        mask = np.ones((20, 20), dtype=bool)
        ghsl_norm = np.tile(np.linspace(0.2, 1.0, 20, dtype=np.float32), (20, 1))
        worldcover_land = np.full((20, 20), 0.35, dtype=np.float32)

        pop_dasy = compute_population_dasymetric(coarse_path, ref_path, mask, ghsl_norm, worldcover_land)
        conservation = validate_population_conservation(pop_dasy, coarse_path, ref_path, mask)

        self.assertLess(conservation["max_relative_error"], 1e-5)
        self.assertLess(conservation["mean_relative_error"], 1e-6)
        self.assertAlmostEqual(float(np.sum(pop_dasy)), float(np.sum(coarse_array)), places=4)

    def test_proximity_risk_decays_with_distance(self) -> None:
        ref_path = self._create_ref_raster("proximity_ref.tif", (0.0, 0.0, 100.0, 100.0), 10.0)
        line_path = self._create_line_vector(self.root / "line.gpkg", 45.0, 0.0, 100.0)

        risk = compute_proximity_risk(
            line_path,
            ref_path,
            self.root,
            inner=0.0,
            outer=30.0,
            base=1.0,
            cache_name="synthetic_line",
        )

        center = float(risk[5, 4])
        near = float(risk[5, 2])
        far = float(risk[5, 0])

        self.assertGreaterEqual(center, 0.9)
        self.assertGreater(near, far)
        self.assertAlmostEqual(far, 0.0, places=4)

    def test_population_validation_ignores_outside_boundary_cells(self) -> None:
        ref_path = self._create_ref_raster("partial_ref.tif", (0.0, 0.0, 2000.0, 2000.0), 100.0)
        coarse_path = self.root / "partial_coarse.tif"
        coarse_array = np.array([[100.0, 200.0], [300.0, 400.0]], dtype=np.float32)
        coarse_gt = (0.0, 1000.0, 0.0, 2000.0, 0.0, -1000.0)
        self._save_array(coarse_path, coarse_array, coarse_gt)

        mask = np.zeros((20, 20), dtype=bool)
        mask[:, :10] = True
        ghsl_norm = np.tile(np.linspace(0.2, 1.0, 20, dtype=np.float32), (20, 1))
        worldcover_land = np.full((20, 20), 0.35, dtype=np.float32)

        pop_dasy = compute_population_dasymetric(coarse_path, ref_path, mask, ghsl_norm, worldcover_land)
        conservation = validate_population_conservation(pop_dasy, coarse_path, ref_path, mask)

        self.assertLess(conservation["max_relative_error"], 1e-5)
        self.assertEqual(conservation["covered_cell_count"], 2)
        self.assertEqual(conservation["excluded_outside_cell_count"], 2)

    def test_manifest_payload_contains_stats_and_paths(self) -> None:
        args = argparse.Namespace(
            boundary_source="osm_admin",
            analysis_crs="EPSG:32651",
            resolution=100.0,
            worldpop_year="2020",
            ghsl_layer="built_s",
            worldcover_year="2021",
        )
        raster_dir = self.root / "rasters"
        raster_dir.mkdir()
        boundary_path = self.root / "hangzhou_boundary.geojson"
        boundary_path.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
        osm_paths = {
            "roads": self.root / "hangzhou_roads.geojson",
            "rail": self.root / "hangzhou_rail.geojson",
            "sensitive": self.root / "hangzhou_sensitive_poi.geojson",
            "landuse": self.root / "hangzhou_landuse.geojson",
        }
        for path in osm_paths.values():
            path.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")

        mask = np.array([[True, True], [True, False]])
        components = {
            "r_pop": np.array([[0.3, 0.6], [0.9, 0.0]], dtype=np.float32),
            "r_built": np.array([[0.2, 0.2], [0.2, 0.0]], dtype=np.float32),
            "r_transport": np.array([[0.0, 0.5], [1.0, 0.0]], dtype=np.float32),
            "r_sensitive": np.array([[0.1, 0.2], [0.3, 0.0]], dtype=np.float32),
            "r_land": np.array([[0.4, 0.4], [0.4, 0.0]], dtype=np.float32),
        }
        e_static = compose_e_static(components, mask)
        manifest = build_manifest_payload(
            city="杭州市",
            args=args,
            boundary_info={"display_name": "Hangzhou"},
            buffered_bbox_wgs84=(29.0, 31.0, 119.0, 121.0),
            worldpop_src=self.root / "worldpop.tif",
            ghsl_sources=[self.root / "ghsl.tif"],
            worldcover_sources=[self.root / "worldcover.tif"],
            boundary_path=boundary_path,
            osm_paths=osm_paths,
            raster_dir=raster_dir,
            preview_png=self.root / "preview.png",
            preview_html=self.root / "preview.html",
            arrays_for_stats={
                "r_pop": components["r_pop"],
                "r_built": components["r_built"],
                "r_transport": components["r_transport"],
                "r_sensitive": components["r_sensitive"],
                "r_land": components["r_land"],
                "e_static": np.where(mask, e_static, np.nan),
            },
            mask=mask,
            conservation={"max_relative_error": 0.0, "mean_relative_error": 0.0},
        )

        self.assertEqual(manifest["city"], "杭州市")
        self.assertIn("stats", manifest)
        self.assertIn("e_static", manifest["stats"])
        self.assertAlmostEqual(manifest["stats"]["population_conservation"]["max_relative_error"], 0.0)
        self.assertGreater(manifest["stats"]["e_static"]["p95"], manifest["stats"]["e_static"]["mean"])
        self.assertIsNone(manifest["outputs"]["preview_png"])
        self.assertTrue(str(manifest["outputs"]["rasters"]["e_static"]).endswith("hangzhou_e_static_100m.tif"))

    def test_expand_raster_sources_extracts_nested_zip(self) -> None:
        raster_path = self.root / "nested" / "tile.tif"
        raster_path.parent.mkdir(parents=True)
        self._save_array(raster_path, np.array([[1.0]], dtype=np.float32), (0.0, 1.0, 0.0, 1.0, 0.0, -1.0))

        zip_path = self.root / "ghsl.zip"
        with zipfile.ZipFile(zip_path, "w") as archive:
            archive.write(raster_path, arcname="subdir/tile.tif")

        expanded = expand_raster_sources([zip_path], self.root / "expanded")

        self.assertEqual(len(expanded), 1)
        self.assertTrue(expanded[0].exists())
        self.assertEqual(expanded[0].name, "tile.tif")

    def test_preview_html_contains_all_layer_names(self) -> None:
        boundary_path = self.root / "hangzhou_boundary.geojson"
        boundary_path.write_text(
            '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[120.0,30.0],[120.1,30.0],[120.1,30.1],[120.0,30.1],[120.0,30.0]]]},"properties":{}}]}',
            encoding="utf-8",
        )
        preview_dir = self.root / "preview"
        preview_dir.mkdir()
        ref_path = self._create_ref_raster("preview_ref.tif", (1000.0, 2000.0, 1400.0, 2400.0), 100.0)
        raster_dir = self.root / "rasters"
        raster_dir.mkdir()
        mask = np.ones((4, 4), dtype=bool)
        layers = {
            "E_static": np.full((4, 4), 0.8, dtype=np.float32),
            "R_pop": np.full((4, 4), 0.5, dtype=np.float32),
            "R_transport": np.full((4, 4), 0.2, dtype=np.float32),
        }
        raster_paths = {}
        for name, array in layers.items():
            path = raster_dir / f"{name}.tif"
            save_array_as_tif(path, ref_path, array, DEFAULT_NODATA)
            raster_paths[name] = path
        preview_layers = make_preview_layers(preview_dir, raster_paths)
        html_path = preview_dir / "preview.html"
        make_preview_html("杭州市", boundary_path, preview_layers, layers, mask, html_path)
        html = html_path.read_text(encoding="utf-8")

        self.assertIn("E_static", html)
        self.assertIn("R_pop", html)
        self.assertIn("R_transport", html)

    def test_raster_bounds_wgs84_uses_reference_raster_extent(self) -> None:
        ref_path = self._create_ref_raster("bounds_ref.tif", (500000.0, 3300000.0, 500400.0, 3300400.0), 100.0)
        minx, miny, maxx, maxy = raster_bounds_wgs84(ref_path)
        self.assertLess(minx, maxx)
        self.assertLess(miny, maxy)


if __name__ == "__main__":
    unittest.main()
