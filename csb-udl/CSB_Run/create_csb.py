"""
csb_create.py
"""

import argparse
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
import math
from os import cpu_count
from pathlib import Path
import re
import sys
import time

import arcpy
import arcpy.management
import arcpy.sa

# CSB-Run utility functions
from logger import initialize_logger
from gdb import initialize_gdbs, add_field, feature_class_generator, repair_topology
import utils

# projection
COORDINATE_STRING = r'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-96.0],PARAMETER["Standard_Parallel_1",29.5],PARAMETER["Standard_Parallel_2",45.5],PARAMETER["Latitude_Of_Origin",23.0],UNIT["Meter",1.0]]'

# projection for elimination
OUTPUT_COORDINATE_SYSTEM_2_ = 'PROJCS["Albers_Conic_Equal_Area",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-96.0],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_origin",23.0],UNIT["Meter",1.0]]'


def process_csb(config: ProcessingConfig) -> str:
    """Create a CSB dataset for the specified area and years"""
    creation_dir = Path(config.creation_dir)
    error_path = creation_dir / "log/overall_error.txt"

    processor = CSBProcessor(config, logger)
    try:
        processor.process()
        return f"Finished {config.area}"
    except Exception as e:
        error_path.write_text(str(e))
        raise ProcessingError(f"Failed to process {config.area}") from e


def process_layer(layer_name: str, last_iteration_name: str, shape_area: int, scratch: str, logger) -> str:
    """Selects polygons <= shape_area, performs an elimination,
    and creates a new feature layer returning the layer name"""

    try:
        if last_iteration_name:
            new_layer_name = last_iteration_name
        else:
            new_layer_name = layer_name

        iteration = 0
        previous_poly_count = math.inf
        done = False
        while not done:
            iteration += 1
            # Select CSB polygons that meet size criteria
            logger.debug("  Iteration %s: Selecting polygons with Shape_Area <= %sm2...", iteration, shape_area)
            polys_to_eliminate = arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=new_layer_name,
                selection_type="NEW_SELECTION",
                where_clause=f"Shape_Area <= {shape_area}",
                invert_where_clause="",
            )
            poly_count = int(arcpy.management.GetCount(polys_to_eliminate)[0])  # type: ignore

            # if there are polygons that cannot be eliminated (i.e., previous_poly_count == poly_count), then done
            if poly_count > 0 and (previous_poly_count > poly_count):
                # Eliminate selected CSB polygons that meet size criteria
                logger.debug("  Iteration %s: Eliminating %s polygons...", iteration, poly_count)
                temp_name = rf"{scratch}\{layer_name}_{shape_area}_{iteration}"
                with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
                    arcpy.management.Eliminate(
                        in_features=polys_to_eliminate,
                        out_feature_class=temp_name,
                        selection="LENGTH",
                        ex_where_clause="",
                        ex_features="",
                    )

                # Make a new feature layer from the result
                new_layer_name = f"{layer_name}_{shape_area}_{iteration}_Layer"
                logger.debug("  Iteration %s: Creating new intermediate feature layer %s...", iteration, new_layer_name)
                with arcpy.EnvManager(outputCoordinateSystem=OUTPUT_COORDINATE_SYSTEM_2_):
                    arcpy.management.MakeFeatureLayer(
                        in_features=temp_name,
                        out_layer=new_layer_name,
                        where_clause="",
                        workspace="",
                        field_info="",
                    )
                previous_poly_count = poly_count
            else:
                logger.debug(
                    "  Iteration %s: No polygons <= %sm2 selected, skipping to next Shape_Area.", iteration, shape_area
                )
                done = True
        return new_layer_name

    except Exception as e:
        logger.error(f"An error occurred while eliminating area {shape_area}, iteration #{iteration}: {e}")
        return ""


def csb_elimination(input_layers, workspace, scratch, area, logger):
    """Performs polygon elimination on input layers"""
    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = True

    # List of the eliminations to be performed.  Each number is an area in square meters approximately
    # equal to blocks of 1, 2, 3, etc. pixels in the source raster data, rounded up to the nearest
    # 100 square meters to account for potential spatial inaccuracies in the raster to vector
    # conversion.  The areas represent the following approximate acreages:
    # 0.22, 0.44, 0.67, 0.89, 1.11, 1.33, 1.56 and 2.00 acres
    elimination_areas = [1000, 1900, 2800, 3700, 4600, 5500, 6500, 8100]

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
        last_iteration_name = layer_name
        for size in elimination_areas:
            logger.debug("%s:  Eliminating polygons <= %sm2...", area, size)
            last_iteration_name = process_layer(layer_name, last_iteration_name, size, scratch, logger)


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
    cfg = utils.get_config("default")
    split_rasters = f'{cfg["folders"]["split_rasters"]}'
    print(f"\nSplit raster folder: {split_rasters}")
    # logger.debug("Split raster folder: %s", split_rasters)

    # Create list of sub-units to process
    file_obj = Path(f"{split_rasters}/{args.start_year}/").rglob("*.tif")
    file_lst = [str(x).split(f"{args.start_year}")[1][1:-1] for x in file_obj]
    # sort the list of sub-units numerically to facilitate progress tracking
    file_lst.sort(key=sort_key)
    print(f"{len(file_lst)} split raster files to process.")

    # Get number of CPUs to use for processing
    cpu_prct = float(cfg["global"]["cpu_prct"])
    run_cpu = math.floor(cpu_prct * cpu_count())  # type: ignore
    print(f"Using {run_cpu} CPUs for CSB processing...\n")

    # Create a list of arguments for each process
    process_args = [(args.start_year, args.end_year, area, args.creation_dir) for area in file_lst]

    # Create a pool of processes and submit each sub-unit for processing as a CPU is available
    with ProcessPoolExecutor(max_workers=run_cpu) as executor:
        future_args = {executor.submit(process_csb, *args): args for args in process_args}
        futures = set(future_args.keys())
        completed = 0
        num_areas = len(futures)
        while futures:
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                completed += 1
                try:
                    result = future.result()
                    # print(result)
                except Exception as e:
                    area = future_args[future][2]
                    print(f"CSB sub-unit {area} failed with error: {e}")
                print(f"{completed} of {num_areas} processed ({100.0 * completed / num_areas:.1f}%)")


if __name__ == "__main__":
    main()
