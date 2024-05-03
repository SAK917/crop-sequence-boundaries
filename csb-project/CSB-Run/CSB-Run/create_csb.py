"""
csb_create.py
"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from os import cpu_count
from pathlib import Path
import re
import sys
import time

import arcpy
import arcpy.management
import arcpy.sa
from numpy import unique


# CSB-Run utility functions
from logger import initialize_logger
from gdb import initialize_gdbs, add_field, feature_class_generator, repair_topology
import utils

# projection
COORDINATE_STRING = r'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-96.0],PARAMETER["Standard_Parallel_1",29.5],PARAMETER["Standard_Parallel_2",45.5],PARAMETER["Latitude_Of_Origin",23.0],UNIT["Meter",1.0]]'

# projection for elimination
OUTPUT_COORDINATE_SYSTEM_2_ = 'PROJCS["Albers_Conic_Equal_Area",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-96.0],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_origin",23.0],UNIT["Meter",1.0]]'


def process_csb(start_year, end_year, area, creation_dir):
    """Create a CSB dataset for the specified area and years"""

    # Initialize logger in this functino so that each spawned process has its own logger
    logger = initialize_logger(creation_dir, area)
    error_path = f"{creation_dir}/log/overall_error.txt"

    # Load configuration settings
    cfg = utils.GetConfig("default")

    t0 = time.perf_counter()
    # Set up list of years covered in history
    year_lst = []
    for year in range(int(start_year), int(end_year) + 1):
        year_lst.append(year)

    # get file name for same area across different years
    year_file_lst = []
    for year in year_lst:
        file_path = f'{cfg["folders"]["split_rasters"]}/{year}/{area}_{year}.TIF'
        year_file_lst.append(file_path)

    gdb_name = f"{area}_{str(start_year)}-{str(end_year)}"
    initialize_gdbs(creation_dir, gdb_name, area, logger, error_path)

    # print(f"{area}: Start Combine")
    logger.info("%s:  Starting Combine...", area)
    output_path = f"{creation_dir}/CombineALL/{area}_{start_year}-{end_year}.tif"
    arcpy.gp.Combine_sa(year_file_lst, output_path)  # type: ignore
    logger.info("%s:  Combine done, adding field for Year count...", area)

    # TODO: Check to see if we really need to repeat attempts or if this is a figment of the original cloud processing
    column_list = [field.name for field in arcpy.ListFields(output_path)]  # type: ignore
    attempt_count = 0
    max_attempts = 5  # Set a limit to the number of attempts
    while "COUNT0" not in column_list and attempt_count < max_attempts:
        column_list = add_field(output_path, area, logger, error_path)
        if column_list is None:
            column_list = [i.name for i in arcpy.ListFields(output_path)]  # type: ignore
        attempt_count += 1
    if attempt_count == max_attempts:
        logger.error("%s:  Failed to add 'COUNT0' to the table after %s attempts.", area, attempt_count)

    # generate experession string
    logger.info("%s:  Calculating polygon year counts...", area)
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
    logger.info("%s:  Creating Null mask for pixels with < 1.1 years of data...", area)
    # print(f"{area}: Creating Null mask for pixels with < 1.1 years of data...")
    setnull_path = f"{creation_dir}/Combine/{area}_{start_year}-{end_year}_NULL.tif"
    arcpy.gp.SetNull_sa(output_path, output_path, setnull_path, '"COUNT0" < 1.1')  # type: ignore

    # Convert Raster to Vector
    logger.info("%s:  Converting raster to vector polygons...", area)
    # print(f"{area}: Converting raster to vector polygons...")
    out_feature_ll = f"{creation_dir}/Vectors_LL/{area}_{start_year}-{end_year}.gdb/{area}_{year}_In"
    arcpy.RasterToPolygon_conversion(
        in_raster=setnull_path,
        out_polygon_features=out_feature_ll,
        simplify="NO_SIMPLIFY",
        raster_field="Value",
        create_multipart_features="SINGLE_OUTER_PART",
        max_vertices_per_feature="",
    )

    logger.info("%s:  Projecting vector polygons to Albers projection...", area)
    # print(f"{area}: Projecting vector polygons to Albers projection...")
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
    # print(f"Time to finish all the steps before Elimination for {area}: {round((t1 - t0) / 60, 2)} minutes")
    logger.info("%s:  Pre-processing completed in %s minutes", area, round((t1 - t0) / 60, 2))

    eliminate_success = False
    while not eliminate_success:
        # print(f"{area}: Running Elimination...")
        logger.info("%s:  Running Elimination...", area)
        try:
            with arcpy.EnvManager(
                scratchWorkspace=f"{creation_dir}/Vectors_temp/{area}_{start_year}-{end_year}_temp.gdb",
                workspace=f"{creation_dir}/Vectors_temp/{area}_{start_year}-{end_year}_temp.gdb",
            ):
                csb_elimination(
                    input_layers=f"{creation_dir}/Vectors_In/{area}_{start_year}-{end_year}_In.gdb",
                    workspace=f"{creation_dir}/Vectors_Out/{area}_{start_year}-{end_year}_OUT.gdb",
                    scratch=f"{creation_dir}/Vectors_temp/{area}_{start_year}-{end_year}_temp.gdb",
                    area=f"{area}",
                    logger=logger,
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
            repair_topology(
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
    # print(f"Elimination for {area}: {round((t2 - t1) / 60, 2)} minutes")
    logger.info("%s:  Elimination completed in %s minutes", area, round((t2 - t1) / 60, 2))

    # for item in range(len(file_lst)):
    #     logger.info(f"{area}_{item}: Select analysis")
    # TODO:  Remove this code block since no longer necessary with the new elimination process
    # print(f"{area}: Selecting polygons with Shape_Area > 2 acres and saving to ShapeFile...")
    # arcpy.Select_analysis(
    #     in_features=f"{creation_dir}/Vectors_Out/{area}_{start_year}-{end_year}_OUT.gdb/Out_{area}_{year}_In",
    #     out_feature_class=f"{creation_dir}/Vectors_Out/{area}_{year}_{start_year}_{end_year}_Out.shp",
    #     where_clause="Shape_Area > 9000",
    # )

    t3 = time.perf_counter()
    # print(f"Total time for {area}: {round((t3 - t0) / 60, 2)} minutes")
    logger.info("%s:  CSB generated in %s minutes", area, round((t3 - t0) / 60, 2))
    return f"Finished {area}"


def process_layer(layer_name, shape_area, scratch, logger):
    """Selects polygons <= shape_area, performs an elimination,
    and creates a new feature layer returning the layer name"""

    try:
        new_layer_name = layer_name
        if "_temp" in new_layer_name:
            layer_name = new_layer_name[: new_layer_name.find("_temp")]

        i = 0
        done = False
        while not done:
            i += 1
            # Select CSB polygons that meet size criteria
            logger.debug("   Iteration %s:  Selecting polygons with Shape_Area <= %s m2...", i, shape_area)
            # print(f"   Iteration {i}:  Selecting polygons with Shape_Area <= {shape_area}m2...")
            selected = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=new_layer_name,
                selection_type="NEW_SELECTION",
                where_clause=f"Shape_Area <= {shape_area}",
                invert_where_clause="",
            )

            poly_count = int(arcpy.management.GetCount(selected)[0])  # type: ignore
            if poly_count > 0:
                # Eliminate selected CSB polygons that meet size criteria
                logger.debug("   Iteration %s:  Eliminating %s polygons...", i, poly_count)
                # print(f"   Iteration {i}:  Eliminating {poly_count} polygons...")
                temp_name = rf"{scratch}\{layer_name}_temp_{shape_area}_{i}"
                with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
                    arcpy.management.Eliminate(
                        in_features=selected,
                        out_feature_class=temp_name,
                        selection="LENGTH",
                        ex_where_clause="",
                        ex_features="",
                    )

                # Make a new feature layer from the result
                new_layer_name = f"{layer_name}_temp{i}_Layer"
                logger.debug("   Iteration %s:  Creating new intermediate feature layer %s...", i, new_layer_name)
                # print(f"   Iteration {i}:  Creating new intermediate feature layer {new_layer_name}...")
                with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
                    arcpy.management.MakeFeatureLayer(
                        in_features=temp_name,
                        out_layer=new_layer_name,
                        where_clause="",
                        workspace="",
                        field_info="",
                    )
            else:
                print(f"   Iteration {i}:  No polygons <= {shape_area}m2 selected, skipping to next Shape_Area...")
                done = True

        return new_layer_name

    except Exception as e:
        print(f"An error occurred during processing: {e}")
        return None


def csb_elimination(input_layers, workspace, scratch, area, logger):
    """Performs polygon elimination on input layers"""
    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = True

    # Data structure that defines the eliminations to be performed
    # The first number is the area in square meters approximately equal to a specified acreage,
    # and the second number is the number of iterations to eliminate using the area
    # For example, the first elimination of (1012, 3) will eliminate polygons <= 0.25 acres 3 times
    # 0.22, 0.44, 0.67, 0.89, 1.11, 1.33, 1.56 acres
    elimination_areas = [1000, 1900, 2800, 3700, 4600, 5500, 6500]

    # print(f"{area}:  Starting Elimination")
    logger.info("%s:  Starting Elimination", area)

    for feature_class, layer_name in feature_class_generator(input_layers, "", "POLYGON", "NOT_RECURSIVE"):
        with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
            arcpy.management.MakeFeatureLayer(
                in_features=feature_class,
                out_layer=layer_name,
                where_clause="",
                workspace="",
                field_info="",
            )

        # Perform the eliminations by looping through the eliminations list
        for size in elimination_areas:
            logger.info("%s:  Eliminating polygons <= %s m2 in %s...", area, size, layer_name)
            # print(f"{area}:  Eliminating polygons <= {size}m2 in {layer_name}...")
            layer_name = process_layer(layer_name, size, scratch, logger)


def sort_key(file_name: str) -> tuple[str, int]:
    """Splits file name into area and year parts, returns tuple for sorting
    Assumes the file name consists of one or more text characters followed by a 1-4 digit number
    """
    num_part_start = re.search(r"\d", file_name).start()  # type: ignore
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
    """Create CSB polygons from source CDL data
    User specifies the start and end years to use
    NOTE: Source CDL data for the years specified must be present
    """
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
    # logger.debug("Split raster folder: %s", split_rasters)

    # get list of area files
    file_obj = Path(f"{split_rasters}/{args.start_year}/").rglob("*.tif")
    file_lst = [str(x).split(f"{args.start_year}")[1][1:-1] for x in file_obj]
    file_lst.sort(key=sort_key)
    print(f"{len(file_lst)} split raster files to process.")

    # TODO: Delete this code block of no longer necessary (partial run functionality removed)
    # delete old files from previous run if doing partial run
    # if args.partial_area != "None":
    #     file_lst = [x for x in file_lst if x == args.partial_area]
    #     csb_yrs = args.creation_dir.split("_")[-3]
    #     start_year = f"20{csb_yrs[0:2]}"
    #     end_year = f"20{csb_yrs[2:5]}"
    #     utils.DeletusGDBus(args.partial_area, args.creation_dir)

    # get number of CPUs to use for processing
    cpu_prct = float(cfg["global"]["cpu_prct"])
    run_cpu = int(round(cpu_prct * cpu_count()))  # type: ignore
    print(f"Using {run_cpu} CPUs for CSB processing...")

    # Create a list of arguments for each process
    process_args = [(args.start_year, args.end_year, area, args.creation_dir) for area in unique(file_lst)]

    # Create a pool of processes and submit each area for processing as a CPU is available
    with ProcessPoolExecutor(max_workers=run_cpu) as executor:
        futures = {executor.submit(process_csb, *args): args for args in process_args}
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
