"""
GeoDatabase processing
"""

import sys

import arcpy


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
        logger.info("%s:  Creating GDBs", area)
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
