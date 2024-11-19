"""GeoDatabase operations"""

from pathlib import Path
import logging
from typing import Optional, List, Generator, Tuple
import arcpy

from csb.errors import handle_arcpy_errors
from csb.types import GDBPath, FeatureClass


def create_gdb(out_folder: Path, out_name: str) -> None:
    """Create a file geodatabase

    Args:
        out_folder: Output folder path
        out_name: Name for geodatabase
    """
    with handle_arcpy_errors(logging.getLogger(), "create_gdb"):
        arcpy.CreateFileGDB_management(out_folder_path=str(out_folder), out_name=out_name, out_version="CURRENT")


def initialize_gdbs(creation_dir: Path, gdb_name: str, area: str, logger: logging.Logger, error_path: Path) -> None:
    """Initialize geodatabases for CSB processing

    Args:
        creation_dir: Creation directory path
        gdb_name: Base name for geodatabases
        area: Area identifier
        logger: Logger instance
        error_path: Path for error log
    """
    try:
        logger.debug("%s: Creating GDBs...", area)

        # Create required geodatabases
        for folder in ["Vectors_LL", "Vectors_Out", "Vectors_temp", "Vectors_In"]:
            gdb_path = creation_dir / folder
            gdb_path.mkdir(parents=True, exist_ok=True)

            if folder == "Vectors_Out":
                gdb_name_suffix = "_OUT"
            elif folder == "Vectors_temp":
                gdb_name_suffix = "_temp"
            elif folder == "Vectors_In":
                gdb_name_suffix = "_In"
            else:
                gdb_name_suffix = ""

            create_gdb(gdb_path, f"{gdb_name}{gdb_name_suffix}.gdb")

    except Exception as e:
        logger.exception("%s: An error occurred while creating the GDBs", area)
        error_path.write_text(str(e))
        raise


def add_field(output_path: str, area: str, logger: logging.Logger, error_path: Path) -> Optional[List[str]]:
    """Add field for number of years of CDL classification

    Args:
        output_path: Path to output file
        area: Area identifier
        logger: Logger instance
        error_path: Path for error log

    Returns:
        List of field names if successful, None otherwise
    """
    try:
        with handle_arcpy_errors(logger, "add_field"):
            arcpy.AddField_management(
                in_table=output_path,
                field_name="COUNT0",
                field_type="SHORT",
                field_is_nullable="NON_NULLABLE",
                field_is_required="NON_REQUIRED",
            )

        return [field.name for field in arcpy.ListFields(output_path)]

    except Exception as e:
        error_msg = str(e) or "An unknown error occurred while adding the COUNT0 field"
        logger.error(error_msg)
        error_path.write_text(error_msg)
        logger.info(f"{area}: trying to add field again...")
        return None


def feature_class_generator(
    workspace: GDBPath, wild_card: str, feature_type: str, recursive: bool
) -> Generator[Tuple[FeatureClass, str], None, None]:
    """Generate feature classes from workspace

    Args:
        workspace: Path to workspace
        wild_card: Wildcard for filtering
        feature_type: Feature type to filter
        recursive: Whether to search recursively

    Yields:
        Tuple of (feature class path, feature class name)
    """
    with arcpy.EnvManager(workspace=str(workspace)):
        dataset_list = [""]
        if recursive:
            datasets = arcpy.ListDatasets()
            if datasets:
                dataset_list.extend(datasets)

        for dataset in dataset_list:
            featureclasses = arcpy.ListFeatureClasses(wild_card, feature_type, dataset)
            if featureclasses:
                for fc in featureclasses:
                    yield str(workspace / dataset / fc), fc


def repair_topology(in_gdb: GDBPath, temp_gdb: GDBPath, area: str, logger: logging.Logger) -> None:
    """Repair topology errors

    Args:
        in_gdb: Input geodatabase path
        temp_gdb: Temporary geodatabase path
        area: Area identifier
        logger: Logger instance
    """
    with arcpy.EnvManager(workspace=str(temp_gdb)):
        temp_featureclasses = arcpy.ListFeatureClasses()
        if not temp_featureclasses:
            return

        # Find problematic area
        area_counts = {}
        for fc in temp_featureclasses:
            area_name = "_".join(fc.split("_")[:2])
            area_counts[area_name] = area_counts.get(area_name, 0) + 1

        repair_area = min(area_counts.items(), key=lambda x: x[1])[0]

        logger.info("%s: Running repair geometry", repair_area)
        with handle_arcpy_errors(logger, "repair_geometry"):
            arcpy.RepairGeometry_management(f"{in_gdb}/{repair_area}_In")

        logger.info("%s: Repair geometry successful. Running Elimination again", repair_area)
