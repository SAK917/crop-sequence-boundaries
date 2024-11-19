"""Test utilities for CSB processing"""

from pathlib import Path
import numpy as np
import arcpy
from typing import Tuple, Optional


def compare_rasters(raster1: Path, raster2: Path, tolerance: float = 0.001) -> bool:
    """Compare two rasters for equality

    Args:
        raster1: Path to first raster
        raster2: Path to second raster
        tolerance: Maximum allowed difference between pixel values

    Returns:
        True if rasters are equal within tolerance
    """
    try:
        # Convert rasters to numpy arrays
        arr1 = arcpy.RasterToNumPyArray(str(raster1))
        arr2 = arcpy.RasterToNumPyArray(str(raster2))

        # Check dimensions
        if arr1.shape != arr2.shape:
            return False

        # Compare values
        diff = np.abs(arr1 - arr2)
        return np.all(diff <= tolerance)

    except Exception as e:
        print(f"Error comparing rasters: {e}")
        return False


def compare_feature_classes(fc1: Path, fc2: Path, geometry_only: bool = False) -> Tuple[bool, str]:
    """Compare two feature classes

    Args:
        fc1: Path to first feature class
        fc2: Path to second feature class
        geometry_only: Only compare geometries, not attributes

    Returns:
        Tuple of (is_equal, difference_description)
    """
    try:
        # Check feature counts
        count1 = int(arcpy.GetCount_management(str(fc1))[0])
        count2 = int(arcpy.GetCount_management(str(fc2))[0])

        if count1 != count2:
            return False, f"Feature counts differ: {count1} vs {count2}"

        # Compare fields if needed
        if not geometry_only:
            fields1 = {f.name: f.type for f in arcpy.ListFields(str(fc1))}
            fields2 = {f.name: f.type for f in arcpy.ListFields(str(fc2))}

            if fields1 != fields2:
                return False, "Field definitions differ"

        # Compare geometries
        with arcpy.da.SearchCursor(str(fc1), ["SHAPE@"]) as cursor1, arcpy.da.SearchCursor(
            str(fc2), ["SHAPE@"]
        ) as cursor2:

            for feat1, feat2 in zip(cursor1, cursor2):
                if not feat1[0].equals(feat2[0]):
                    return False, "Geometries differ"

        return True, "Feature classes are identical"

    except Exception as e:
        return False, f"Error comparing feature classes: {e}"


def create_test_raster(
    path: Path, size: Tuple[int, int], cell_size: float = 30.0, value_range: Optional[Tuple[float, float]] = None
) -> None:
    """Create test raster with random or specified values

    Args:
        path: Output path
        size: (rows, cols) tuple
        cell_size: Cell size in map units
        value_range: Optional (min, max) for random values
    """
    try:
        # Create random data if range specified
        if value_range:
            min_val, max_val = value_range
            data = np.random.uniform(min_val, max_val, size)
        else:
            data = np.ones(size)

        # Create raster dataset
        arcpy.management.CreateRasterDataset(
            out_path=str(path.parent),
            out_name=path.name,
            cell_size=cell_size,
            pixel_type="32_BIT_FLOAT",
            spatial_reference=arcpy.SpatialReference(4326),
        )

        # Convert array to raster
        raster = arcpy.NumPyArrayToRaster(
            in_array=data, lower_left_corner=arcpy.Point(0, 0), x_cell_size=cell_size, y_cell_size=cell_size
        )

        raster.save(str(path))

    except Exception as e:
        raise RuntimeError(f"Failed to create test raster: {e}")


def create_test_polygons(path: Path, count: int, size_range: Tuple[float, float]) -> None:
    """Create test polygons of varying sizes

    Args:
        path: Output feature class path
        count: Number of polygons to create
        size_range: (min_size, max_size) in map units
    """
    try:
        # Create feature class
        arcpy.CreateFeatureclass_management(out_path=str(path.parent), out_name=path.name, geometry_type="POLYGON")

        min_size, max_size = size_range
        with arcpy.da.InsertCursor(str(path), ["SHAPE@"]) as cursor:
            for _ in range(count):
                # Random size within range
                size = np.random.uniform(min_size, max_size)

                # Create square polygon
                array = arcpy.Array(
                    [
                        arcpy.Point(0, 0),
                        arcpy.Point(0, size),
                        arcpy.Point(size, size),
                        arcpy.Point(size, 0),
                        arcpy.Point(0, 0),
                    ]
                )
                polygon = arcpy.Polygon(array)
                cursor.insertRow([polygon])

    except Exception as e:
        raise RuntimeError(f"Failed to create test polygons: {e}")
