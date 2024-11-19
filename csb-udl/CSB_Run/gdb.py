"""
GeoDatabase processing
"""

import operator as op
import os
import sys

import arcpy
import numpy as np


def create_gdb(out_folder_path: str, out_name: str) -> None:
    """Create a file geodatabase"""
    arcpy.CreateFileGDB_management(
        out_folder_path=out_folder_path,
        out_name=out_name,
        out_version="CURRENT",
    )


def initialize_gdbs(creation_dir: str, gdb_name: str, area: str, logger, error_path: str) -> None:
    """Initialize the file geodatabases for the CSB processing"""
    try:
        # print(f"{area}: Creating GDBs")
        logger.debug("%s:  Creating GDBs...", area)
        create_gdb(f"{creation_dir}/Vectors_LL", f"{gdb_name}.gdb")
        create_gdb(f"{creation_dir}/Vectors_Out", f"{gdb_name}_OUT.gdb")
        create_gdb(f"{creation_dir}/Vectors_temp", f"{gdb_name}_temp.gdb")
        create_gdb(f"{creation_dir}/Vectors_In", f"{gdb_name}_In.gdb")
    except Exception as e:
        logger.exception("%s: An error occurred while creating the GDBs", area)
        with open(error_path, "a") as f:
            f.write(str(e))
        sys.exit(0)


def add_field(output_path: str, area: str, logger, error_path: str) -> list[str]:
    """Add field for number of years of CDL classification"""

    try:
        arcpy.AddField_management(
            in_table=output_path,
            field_name="COUNT0",
            field_type="SHORT",
            field_precision="",
            field_scale="",
            field_length="",
            field_alias="",
            field_is_nullable="NON_NULLABLE",
            field_is_required="NON_REQUIRED",
            field_domain="",
        )
        column_list = [field.name for field in arcpy.ListFields(output_path)]  # type: ignore
        return column_list

    except Exception as e:
        error_msg = e.args[0] if e.args else arcpy.GetMessage(0)
        if not error_msg:
            error_msg = "An unknown error occurred while adding the COUNT0 field"
        logger.error(error_msg)
        with open(error_path, "a") as f:
            f.write(error_msg)  # type: ignore
        print(f"{area}: trying to add field again...")
        logger.info(f"{area}: trying to add field again...")
        return None


def feature_class_generator(workspace, wild_card, feature_type, recursive):
    """Generator function that yields feature classes in a workspace
    Args:
        workspace (str): path to workspace
        wild_card (str): wildcard to filter feature classes
        feature_type (str): feature type to filter feature classes
        recursive (bool): whether to search recursively in the workspace"""
    with arcpy.EnvManager(workspace=workspace):
        dataset_list = [""]
        if recursive:
            datasets = arcpy.ListDatasets()
            dataset_list.extend(datasets)  # type: ignore

        for dataset in dataset_list:
            featureclasses = arcpy.ListFeatureClasses(wild_card, feature_type, dataset)
            for fc in featureclasses:  # type: ignore
                yield os.path.join(workspace, dataset, fc), fc


# TODO:  Is this still necessary?
def repair_topology(in_gdb, temp_gdb, area, area_logger):
    """Repair topology errors in the input gdb
    Inspects the [area]_temp.gdb where the topology error happened,
    identifies the problem area and repairs the it in the [area]_In.gdb
    Args:
        in_gdb (str): path to input gdb
        temp_gdb (str): path to temp gdb
        area (str): area name
        area_logger (logger): logger for the area"""
    arcpy.env.workspace = temp_gdb  # type: ignore
    temp_featureclasses = arcpy.ListFeatureClasses()

    # find the area that doesn't have 3 FCs in temp (eg one that failed)
    area_featureclasses = []
    for fc in temp_featureclasses:  # type: ignore
        split_fc = fc.split("_")
        new_fc = f"{split_fc[0]}_{split_fc[1]}"
        area_featureclasses.append(new_fc)

    areas = np.unique(area_featureclasses)
    for a in areas:
        freq = op.countOf(area_featureclasses, a)
        if freq < 3:
            repair_area = a

    repair_msg = f"{repair_area}: Running repair geometry"
    print(repair_msg)
    area_logger.info(repair_msg)

    arcpy.RepairGeometry_management(f"{in_gdb}/{repair_area}_In")

    success_msg = f"{repair_area}: Repair geometry successful. Running Elimination again"
    print(success_msg)
    area_logger.info(success_msg)
