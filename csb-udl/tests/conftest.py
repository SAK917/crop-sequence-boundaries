"""Test configuration and fixtures"""

import pytest
from pathlib import Path
import shutil
import arcpy
from typing import Generator


@pytest.fixture
def test_data_dir() -> Path:
    """Base directory for test data"""
    return Path(__file__).parent / "test_data"


@pytest.fixture
def sample_rasters(test_data_dir: Path) -> Generator[Path, None, None]:
    """Sample raster directory with minimal test data"""
    raster_dir = test_data_dir / "rasters"
    raster_dir.mkdir(parents=True, exist_ok=True)

    # Create test years
    for year in (2020, 2021):
        year_dir = raster_dir / str(year)
        year_dir.mkdir(exist_ok=True)

        # Create small test rasters
        for area in ("A1", "B2"):
            raster_path = year_dir / f"{area}_{year}.TIF"
            if not raster_path.exists():
                # Create 10x10 test raster
                arcpy.management.CreateRasterDataset(
                    out_path=str(year_dir),
                    out_name=f"{area}_{year}.TIF",
                    cell_size=30,
                    pixel_type="8_BIT_UNSIGNED",
                    spatial_reference=arcpy.SpatialReference(4326),
                )

    yield raster_dir

    # Cleanup
    shutil.rmtree(raster_dir)


@pytest.fixture
def test_workspace(test_data_dir: Path) -> Generator[Path, None, None]:
    """Test workspace directory"""
    workspace = test_data_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    yield workspace

    # Cleanup
    shutil.rmtree(workspace)
