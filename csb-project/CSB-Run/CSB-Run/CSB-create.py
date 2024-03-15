"""
csb_create.py
"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import operator as op
import os
from pathlib import Path
import re
import sys
import time

import arcpy
import arcpy.management
import arcpy.sa

import numpy as np

# CSB-Run utility functions
import utils  # pylint: disable=import-error

# projection
COORDINATE_STRING = r'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-96.0],PARAMETER["Standard_Parallel_1",29.5],PARAMETER["Standard_Parallel_2",45.5],PARAMETER["Latitude_Of_Origin",23.0],UNIT["Meter",1.0]]'

# projection for elimination
OUTPUT_COORDINATE_SYSTEM_2_ = 'PROJCS["Albers_Conic_Equal_Area",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-96.0],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_origin",23.0],UNIT["Meter",1.0]]'


def csb_process(start_year, end_year, area, creation_dir):
    """Main function that creates CSB datasets, performs elimination, run using multiprocessing"""
    # Get config items, configure logger
    cfg = utils.GetConfig("default")

    LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(
        filename=f"{creation_dir}/log/{area}.log",
        level=logging.DEBUG,  # by default it only log warming or above
        format=LOG_FORMAT,
        filemode="a",
    )  # over write instead of appending
    logger = logging.getLogger()
    error_path = f"{creation_dir}/log/overall_error.txt"

    # Set up list of years covered in history
    year_lst = []
    for year in range(int(start_year), int(end_year) + 1):
        year_lst.append(year)

    # get file name for same area across different years
    year_file_lst = []
    for year in year_lst:
        file_path = f'{cfg["folders"]["split_rasters"]}/{year}/{area}_{year}.TIF'
        # file_obj = Path(file_path).rglob(f"{area}_{year}*.tif")
        # file_lst = [str(x) for x in file_obj]
        # sort_file_lst = []
        # for item in range(len(file_lst)):
        #     path = f"{file_path}/{area}_{year}_{item}.TIF"
        #     sort_file_lst.append(path)
        year_file_lst.append(file_path)

    gdb_created = False
    while not gdb_created:
        try:
            t0 = time.perf_counter()
            print(f"{area}: Creating GDBs")
            logger.info(f"{area}: Creating GDBs")
            arcpy.CreateFileGDB_management(
                out_folder_path=f"{creation_dir}/Vectors_LL",
                out_name=f"{area}_{str(start_year)}-{str(end_year)}.gdb",
                out_version="CURRENT",
            )
            arcpy.CreateFileGDB_management(
                out_folder_path=f"{creation_dir}/Vectors_Out",
                out_name=f"{area}_{str(start_year)}-{str(end_year)}_OUT.gdb",
                out_version="CURRENT",
            )
            arcpy.CreateFileGDB_management(
                out_folder_path=f"{creation_dir}/Vectors_temp",
                out_name=f"{area}_{str(start_year)}-{str(end_year)}_temp.gdb",
                out_version="CURRENT",
            )
            arcpy.CreateFileGDB_management(
                out_folder_path=f"{creation_dir}/Vectors_In/",
                out_name=f"{area}_{str(start_year)}-{str(end_year)}_In.gdb",
                out_version="CURRENT",
            )
            gdb_created = True

        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path, "a")
            f.write("".join(str(item) for item in error_msg))
            f.close()
            sys.exit(0)
        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path, "a")
            # f.write("".join(str(item) for item in error_msg))
            f.write(str(error_msg))
            f.close()
            sys.exit(0)

    print(f"{area}: Start Combine")
    logger.info(f"{area}: Start Combine")
    # for item in range(len(file_lst)):
    # cdl_lst = [path for path in year_file_lst]
    # input_path = ";".join(cdl_lst)
    output_path = f"{creation_dir}/CombineALL/{area}_{start_year}-{end_year}.tif"
    arcpy.gp.Combine_sa(year_file_lst, output_path)  # type: ignore
    logger.info(f"{area}: Combine Done, Adding Field")

    column_list = [field.name for field in arcpy.ListFields(output_path)]  # type: ignore
    while "COUNT0" not in column_list:
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

        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path, "a")
            f.write("".join(str(item) for item in error_msg))
            f.close()
            time.sleep(2)
            print(f"{area}: try again add field")
            logger.info(f"{area}: try again add field")
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
            column_list = [i.name for i in arcpy.ListFields(output_path)]  # type: ignore

        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path, "a")
            f.write("".join(str(item) for item in error_msg))  # type: ignore
            f.close()
            time.sleep(2)
            print(f"{area}: try again add field")
            logger.info(f"{area} try again add field")
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
            column_list = [i.name for i in arcpy.ListFields(output_path)]  # type: ignore

    # generate experession string
    logger.info(f"{area}_{year}: Calculate Field")
    calculate_field_lst = [r"!" + f"{area}_{year}"[0:10] + r"!" for year in year_lst]
    # TODO: switch previous line to the following line
    # calculate_field_lst = [f"!{area[:5]}_{year}!" for year in year_lst]
    cal_expression = f"CountFieldsGreaterThanZero({calculate_field_lst})"
    code = "def CountFieldsGreaterThanZero(fieldList):\n    counter = 0\n    for field in fieldList:\n        if int(field) > 0:\n            counter += 1\n    return counter"
    try:
        arcpy.CalculateField_management(
            in_table=output_path,
            field="COUNT0",
            expression=cal_expression,
            code_block=code,
        )
    except Exception as e:
        error_msg = e.args
        logger.error(error_msg)
        f = open(error_path, "a")
        f.write("".join(str(item) for item in error_msg))
        f.write(r"/n")
        f.close()
        sys.exit(0)
    except:
        error_msg = arcpy.GetMessage(0)
        logger.error(error_msg)
        f = open(error_path, "a")
        f.write("".join(str(item) for item in error_msg))  # type: ignore
        f.write(r"/n")
        f.close()
        sys.exit(0)

    # Start SetNull
    logger.info(f"{area}_{year}: Start SetNull")
    print(f"{area}: Creating Null mask for pixels with < 1.1 years of data...")
    setnull_path = f"{creation_dir}/Combine/{area}_{start_year}-{end_year}_NULL.tif"
    arcpy.gp.SetNull_sa(output_path, output_path, setnull_path, '"COUNT0" < 1.1')  # type: ignore

    # Convert Raster to Vector
    logger.info(f"{area}_{year}: Convert Raster to Vector")
    print(f"{area}: Converting raster to vector polygons...")
    out_feature_ll = f"{creation_dir}/Vectors_LL/{area}_{start_year}-{end_year}.gdb/{area}_{year}_In"
    arcpy.RasterToPolygon_conversion(
        in_raster=setnull_path,
        out_polygon_features=out_feature_ll,
        simplify="NO_SIMPLIFY",
        raster_field="Value",
        create_multipart_features="SINGLE_OUTER_PART",
        max_vertices_per_feature="",
    )

    logger.info(f"{area}_{year}: Projection")
    print(f"{area}: Projecting vector polygons to Albers projection...")
    out_feature_in = f"{creation_dir}/Vectors_In/{area}_{start_year}-{end_year}_In.gdb/{area}_{year}_In"
    arcpy.management.Project(
        in_dataset=out_feature_ll,
        out_dataset=out_feature_in,
        out_coor_system=COORDINATE_STRING,
        transform_method=[],
        in_coor_system="",
        preserve_shape="NO_PRESERVE_SHAPE",
        max_deviation="",
        vertical="NO_VERTICAL",
    )

    t1 = time.perf_counter()
    print(f"Time to finish all the steps before Elimination for {area}: {round((t1 - t0) / 60, 2)} minutes")
    logger.info(f"Time to finish all the steps before Elimination for {area}: {round((t1 - t0) / 60, 2)} minutes")

    logger.info(f"{area}: Elimination")
    print(f"{area}: Elimination")

    eliminate_success = False
    while not eliminate_success:
        print(f"{area}: Running Elimination...")
        try:
            with arcpy.EnvManager(
                scratchWorkspace=f"{creation_dir}/Vectors_temp/{area}_{start_year}-{end_year}_temp.gdb",
                workspace=f"{creation_dir}/Vectors_temp/{area}_{start_year}-{end_year}_temp.gdb",
            ):
                CSBElimination(
                    input_layers=f"{creation_dir}/Vectors_In/{area}_{start_year}-{end_year}_In.gdb",
                    workspace=f"{creation_dir}/Vectors_Out/{area}_{start_year}-{end_year}_OUT.gdb",
                    scratch=f"{creation_dir}/Vectors_temp/{area}_{start_year}-{end_year}_temp.gdb",
                    area=f"{area}",
                )
            eliminate_success = True

        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            print(f"{area}: {error_msg}")
            f = open(error_path, "a")
            f.write("".join(str(item) for item in error_msg))
            f.write(r"/n")
            f.close()
            RepairTopology(
                f"{creation_dir}/Vectors_In/{area}_{start_year}-{end_year}_In.gdb",
                f"{creation_dir}/Vectors_temp/{area}_{start_year}-{end_year}_temp.gdb",
                area,
                logger,
            )
        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path, "a")
            f.write("".join(str(item) for item in error_msg))  # type: ignore
            f.write(r"/n")
            f.close()
            sys.exit(0)

    t2 = time.perf_counter()
    print(f"Elimination for {area}: {round((t2 - t1) / 60, 2)} minutes")
    logger.info(f"Elimination for {area}: {round((t2 - t1) / 60, 2)} minutes")

    # for item in range(len(file_lst)):
    #     logger.info(f"{area}_{item}: Select analysis")
    print(f"{area}: Selecting polygons with Shape_Area > 2 acres and saving to ShapeFile...")
    arcpy.Select_analysis(
        in_features=f"{creation_dir}/Vectors_Out/{area}_{start_year}-{end_year}_OUT.gdb/Out_{area}_{year}_In",
        out_feature_class=f"{creation_dir}/Vectors_Out/{area}_{year}_{start_year}_{end_year}_Out.shp",
        where_clause="Shape_Area > 9000",
    )

    t3 = time.perf_counter()
    print(f"Total time for {area}: {round((t3 - t0) / 60, 2)} minutes")
    logger.info(f"Total time for {area}: {round((t3 - t0) / 60, 2)} minutes")
    return f"Finished {area}"


# Arcgis toolbox code that performs polygon elimination
def CSBElimination(input_layers, workspace, scratch, area):
    """Performs polygon elimination on input layers"""
    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = True

    print(f"{area}:  Starting Elimination")

    for feature_class, name in FeatureClassGenerator(input_layers, "", "POLYGON", "NOT_RECURSIVE"):
        # Process: Make Feature Layer (Make Feature Layer) (management)
        _layer_name = f"{name}"
        with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
            arcpy.management.MakeFeatureLayer(
                in_features=feature_class,
                out_layer=_layer_name,
                where_clause="",
                workspace="",
                field_info="",
            )

        # First set of eliminations for polygons <= 0.25 acres (1 pixel)
        for iteration in range(1, 4):
            print(f"{area}:  Iteration {iteration} selecting polygons with Shape_Area <= 0.25 acres...")
            with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
                selected = arcpy.management.SelectLayerByAttribute(
                    in_layer_or_view=_layer_name,
                    selection_type="NEW_SELECTION",
                    where_clause="Shape_Area <=1012",
                    invert_where_clause="",
                )

            print(f"{area}:  Iteration {iteration} eliminating polygons with Shape_Area <= 0.25 acres...")
            _temp_name = rf"{scratch}\{name}_temp{iteration}"
            if selected:
                with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
                    arcpy.management.Eliminate(
                        in_features=selected,
                        out_feature_class=_temp_name,
                        selection="LENGTH",
                        ex_where_clause="",
                        ex_features="",
                    )

            input2 = f"{name}_temp{iteration}_Layer"
            if selected:
                with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
                    arcpy.management.MakeFeatureLayer(
                        in_features=_temp_name,
                        out_layer=input2,
                        where_clause="",
                        workspace="",
                        field_info="",
                    )
            _layer_name = input2

        # Second set of eliminations for polygons <= 0.5 acres (2 pixels)
        for iteration in range(1, 3):
            if selected:
                print(f"{area}:  Iteration {iteration} selecting polygons with Shape_Area <= 0.5 acres...")
                with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
                    selected_2_ = arcpy.management.SelectLayerByAttribute(
                        in_layer_or_view=_layer_name,
                        selection_type="NEW_SELECTION",
                        where_clause="Shape_Area <= 2024",
                        invert_where_clause="",
                    )

            print(f"{area}:  Iteration {iteration} eliminating polygons with Shape_Area <= 0.5 acres...")
            _temp2_name = rf"{scratch}\{name}_temp2-{iteration}"
            if selected and selected_2_:
                with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
                    arcpy.management.Eliminate(
                        in_features=selected_2_,
                        out_feature_class=_temp2_name,
                        selection="LENGTH",
                        ex_where_clause="",
                        ex_features="",
                    )

            temp2_layer = f"{name}_temp2_Layer"
            if selected and selected_2_:
                arcpy.management.MakeFeatureLayer(
                    in_features=_temp2_name,
                    out_layer=temp2_layer,
                    where_clause="",
                    workspace="",
                    field_info="",
                )
            _layer_name = temp2_layer

        # Third set of eliminations for polygons <= 1 acre
        if selected and selected_2_:
            print(f"{area}:  First selection of polygons with Shape_Area <= 1 acre...")
            selected_3_ = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=_layer_name,
                selection_type="NEW_SELECTION",
                where_clause="Shape_Area <= 4047",
                invert_where_clause="",
            )

        # Process: Eliminate (3) (Eliminate) (management)
        _temp3_name = rf"{scratch}\{name}_temp3-{iteration}"
        if selected and selected_2_ and selected_3_:
            print(f"{area}:  First elimination of polygons with Shape_Area <= 1 acre...")
            arcpy.management.Eliminate(
                in_features=selected_3_,
                out_feature_class=_temp3_name,
                selection="LENGTH",
                ex_where_clause="",
                ex_features="",
            )

        # Process: Make Feature Layer (4) (Make Feature Layer) (management)
        input2_2_ = f"{name}_temp3_Layer"
        if selected and selected_2_ and selected_3_:
            arcpy.management.MakeFeatureLayer(
                in_features=_temp3_name,
                out_layer=input2_2_,
                where_clause="",
                workspace="",
                field_info="",
            )

        # Process: Select Layer By Attribute (4) (Select Layer By Attribute) (management)
        if selected and selected_2_ and selected_3_:
            print(f"{area}:  Second selection of polygons with Shape_Area <= 2 acres...")
            selected_4_ = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=input2_2_,
                selection_type="NEW_SELECTION",
                where_clause="Shape_Area <= 9000",
                invert_where_clause="",
            )

        # Process: Eliminate (4) (Eliminate) (management)
        out_name_ = rf"{workspace}\Out_{name}"
        if selected and selected_2_ and selected_3_ and selected_4_:
            print(f"{area}:  Second elimination of polygons with Shape_Area <= 2 acres...")
            arcpy.management.Eliminate(
                in_features=selected_4_,
                out_feature_class=out_name_,
                selection="LENGTH",
                ex_where_clause="",
                ex_features="",
            )


# FeatureClassGenerator function used by CSBElimination arc toolbox
def FeatureClassGenerator(workspace, wild_card, feature_type, recursive):
    """Generator function that yields feature classes in a workspace
    Used by CSBElimination Arc Toolbox
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


def RepairTopology(in_gdb, temp_gdb, area, area_logger):
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


def sort_key(file_name: str) -> tuple[str, int]:
    """Splits file name into area and year parts, returns tuple for sorting
    Assumes the file name consists of one or more text characters followed by a 1-4 digit number
    """
    num_part_start = re.search(r"\d", file_name).start()
    return (file_name[:num_part_start], int(file_name[num_part_start:]))


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="CSB Create")
    parser.add_argument("start_year", type=int, help="Start year for CSB processing")
    parser.add_argument("end_year", type=int, help="End year for CSB processing")
    parser.add_argument("creation_dir", type=str, help="CSB creation directory")
    parser.add_argument("partial_area", type=str, default="None", help="Partial run area")
    return parser.parse_args()


def main():
    # command line inputs passed by CSB-Run
    # start_year (int): Start year for CSB being generated
    # end_year (int): End year for CSB being generated
    # creation_dir (str): CSB creation directory
    # partial_area (str): Partial run area
    args = parse_arguments()

    # Get Creation and Split_raster paths from csb-default.ini
    # TODO: change to use user specified config file
    cfg = utils.GetConfig("default")
    split_rasters = f'{cfg["folders"]["split_rasters"]}'
    print(f"Split raster folder: {split_rasters}")

    # get list of area files
    file_obj = Path(f"{split_rasters}/{args.start_year}/").rglob("*.tif")
    file_lst = [str(x).split(f"{args.start_year}")[1][1:-1] for x in file_obj]
    file_lst.sort(key=sort_key)
    print(f"{len(file_lst)} split raster files to process.")

    # delete old files from previous run if doing partial run
    if args.partial_area != "None":
        file_lst = [x for x in file_lst if x == args.partial_area]
        csb_yrs = args.creation_dir.split("_")[-3]
        start_year = f"20{csb_yrs[0:2]}"
        end_year = f"20{csb_yrs[2:5]}"
        utils.DeletusGDBus(args.partial_area, args.creation_dir)

    # get number of CPUs to use for processing
    cpu_prct = float(cfg["global"]["cpu_prct"])
    run_cpu = int(round(cpu_prct * os.cpu_count(), 0))
    print(f"Using {run_cpu} CPUs for CSB processing...")

    # Create a list of arguments for each process
    process_args = [(args.start_year, args.end_year, area, args.creation_dir) for area in np.unique(file_lst)]

    # Create a pool of processes and submit each area for processing as a CPU is available
    with ProcessPoolExecutor(max_workers=run_cpu) as executor:
        futures = {executor.submit(csb_process, *args): args for args in process_args}
        completed = 0
        num_areas = len(futures)
        for future in as_completed(futures):
            completed += 1
            try:
                result = future.result()
                print(result)
            except Exception as e:
                area = futures[future][2]
                print(f"CSB sub-unit {area} failed with error: {e}")
            print(f"{completed} of {num_areas} processed ({100.0 * completed / num_areas:.1f}%)")


if __name__ == "__main__":
    main()
