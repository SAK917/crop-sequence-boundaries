"""
CSB Creation Module - Version 2

This module handles the creation of Crop Sequence Boundary (CSB) datasets
using a parallel processing approach.
"""

import argparse
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
import math
from os import cpu_count
from pathlib import Path
import re
import sys
from typing import List, Tuple, Optional

import arcpy

from CSB_Run.csb.processing import CSBProcessor, ProcessingConfig
from CSB_Run.csb.types import ProcessingError
from CSB_Run.csb.constants import ALBERS_COORDINATE_SYSTEM, ELIMINATION_COORDINATE_SYSTEM, DEFAULT_ELIMINATION_AREAS
from logger_v2 import initialize_logger
from csb_utils_v2 import get_config


def create_processing_config(
    start_year: int,
    end_year: int,
    area: str,
    creation_dir: Path,
) -> ProcessingConfig:
    """Create processing configuration

    Args:
        start_year: Start year for processing
        end_year: End year for processing
        area: Area identifier
        creation_dir: Base directory for processing

    Returns:
        Configured ProcessingConfig instance
    """
    return ProcessingConfig(
        start_year=start_year,
        end_year=end_year,
        area=area,
        creation_dir=creation_dir,
        coordinate_system=ALBERS_COORDINATE_SYSTEM,
        elimination_coordinate_system=ELIMINATION_COORDINATE_SYSTEM,
        elimination_areas=DEFAULT_ELIMINATION_AREAS,
    )


def process_csb(start_year: int, end_year: int, area: str, creation_dir: str) -> str:
    """Process a single CSB area

    Args:
        start_year: Start year for processing
        end_year: End year for processing
        area: Area identifier
        creation_dir: Base directory for processing

    Returns:
        Status message

    Raises:
        ProcessingError: If processing fails
    """
    # Initialize logger for this process
    logger = initialize_logger(creation_dir, area)
    error_path = Path(creation_dir) / "log/overall_error.txt"

    try:
        # Create configuration
        config = create_processing_config(
            start_year=start_year, end_year=end_year, area=area, creation_dir=Path(creation_dir)
        )

        # Process the CSB
        processor = CSBProcessor(config, logger)
        processor.process()

        return f"Finished {area}"

    except Exception as e:
        error_msg = f"Processing failed for area {area}: {e}"
        logger.error(error_msg)
        error_path.write_text(error_msg)
        raise ProcessingError(f"Failed to process {area}") from e


def sort_key(file_name: str) -> Tuple[str, int]:
    """Extract sort key from filename

    Args:
        file_name: Name of file to parse

    Returns:
        Tuple of (text_part, number_part)

    Raises:
        ValueError: If filename format is invalid
    """
    match = re.search(r"(\D+)(\d+)", file_name)
    if not match:
        raise ValueError(f"Invalid filename format: {file_name}")
    return (match.group(1), int(match.group(2)))


def get_area_list(split_rasters: Path, start_year: int) -> List[str]:
    """Get list of areas to process

    Args:
        split_rasters: Path to split raster directory
        start_year: Start year for processing

    Returns:
        List of area identifiers

    Raises:
        FileNotFoundError: If raster directory not found
    """
    raster_path = split_rasters / str(start_year)
    if not raster_path.exists():
        raise FileNotFoundError(f"Raster directory not found: {raster_path}")

    file_list = [p.stem.split(f"_{start_year}")[0] for p in raster_path.glob("*.tif")]
    file_list.sort(key=sort_key)
    return file_list


def setup_processing(args: argparse.Namespace) -> Tuple[List[str], int]:
    """Set up processing parameters

    Args:
        args: Command line arguments

    Returns:
        Tuple of (area_list, cpu_count)

    Raises:
        ConfigurationError: If configuration is invalid
    """
    # Load configuration
    cfg = get_config("default")
    split_rasters = Path(cfg["folders"]["split_rasters"])
    print(f"\nSplit raster folder: {split_rasters}")

    # Get areas to process
    areas = get_area_list(split_rasters, args.start_year)
    print(f"{len(areas)} split raster files to process.")

    # Calculate CPU count
    try:
        cpu_percent = float(cfg["global"]["cpu_prct"])
        if not 0 < cpu_percent <= 1:
            raise ValueError("cpu_prct must be between 0 and 1")
    except (KeyError, ValueError) as e:
        print(f"Invalid CPU configuration: {e}")
        cpu_percent = 0.75  # Default to 75%

    cpu_count_val = max(1, math.floor(cpu_percent * cpu_count()))
    print(f"Using {cpu_count_val} CPUs for CSB processing...\n")

    return areas, cpu_count_val


def process_areas(areas: List[str], args: argparse.Namespace, cpu_count_val: int) -> None:
    """Process all areas using parallel execution

    Args:
        areas: List of areas to process
        args: Command line arguments
        cpu_count_val: Number of CPUs to use
    """
    process_args = [(args.start_year, args.end_year, area, args.creation_dir) for area in areas]

    with ProcessPoolExecutor(max_workers=cpu_count_val) as executor:
        future_to_args = {executor.submit(process_csb, *args): args for args in process_args}

        completed = 0
        futures = set(future_to_args.keys())
        total = len(futures)

        while futures:
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                completed += 1
                try:
                    result = future.result()
                    print(result)
                except Exception as e:
                    area = future_to_args[future][2]
                    print(f"CSB sub-unit {area} failed with error: {e}")
                print(f"{completed} of {total} processed " f"({100.0 * completed / total:.1f}%)")


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments

    Returns:
        Parsed arguments

    Raises:
        SystemExit: If arguments are invalid
    """
    parser = argparse.ArgumentParser(description="Create Cropland Stability Boundary (CSB) datasets")
    parser.add_argument("start_year", type=int, help="Start year for CSB processing")
    parser.add_argument("end_year", type=int, help="End year for CSB processing")
    parser.add_argument("creation_dir", type=str, help="CSB creation directory")
    parser.add_argument("partial_area", type=str, default="None", help="Process single area (optional)")

    args = parser.parse_args()

    # Validate arguments
    if args.end_year < args.start_year:
        parser.error("End year must be greater than or equal to start year")

    if not Path(args.creation_dir).exists():
        parser.error(f"Creation directory does not exist: {args.creation_dir}")

    return args


def main() -> Optional[str]:
    """Main entry point

    Returns:
        Error message if processing failed, None otherwise
    """
    try:
        args = parse_arguments()

        # Setup processing
        areas, cpu_count_val = setup_processing(args)

        # Handle partial area processing
        if args.partial_area != "None":
            if args.partial_area not in areas:
                raise ValueError(f"Invalid partial area: {args.partial_area}")
            areas = [args.partial_area]

        # Process areas
        process_areas(areas, args, cpu_count_val)

        return None

    except Exception as e:
        error_msg = f"CSB processing failed: {e}"
        print(error_msg)
        return error_msg


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
