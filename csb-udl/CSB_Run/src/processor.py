"""Core CSB processing logic"""

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, List
import time
import arcpy
import arcpy.management
import arcpy.sa
from logging import Logger

from CSB_Run.csb.constants import ALBERS_COORDINATE_SYSTEM, ELIMINATION_COORDINATE_SYSTEM, DEFAULT_ELIMINATION_AREAS
from CSB_Run.csb.errors import handle_arcpy_errors, ProcessingError
from CSB_Run.csb.elimination import PolygonEliminator, EliminationConfig
from CSB_Run.csb.types import GDBPath, FeatureClass


@dataclass
class ProcessingConfig:
    """Configuration for CSB processing"""

    start_year: int
    end_year: int
    area: str
    creation_dir: Path
    error_path: Path  # Add this field
    coordinate_system: str = ALBERS_COORDINATE_SYSTEM
    elimination_coordinate_system: str = ELIMINATION_COORDINATE_SYSTEM
    elimination_areas: Sequence[int] = DEFAULT_ELIMINATION_AREAS

    def __post_init__(self):
        """Ensure error path exists"""
        self.error_path = self.creation_dir / "log/overall_error.txt"
        self.error_path.parent.mkdir(parents=True, exist_ok=True)


class CSBProcessor:
    """Handles core CSB data processing logic"""

    def __init__(self, config: ProcessingConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.error_path = config.creation_dir / "log/overall_error.txt"

    def process(self) -> None:
        """Main processing method"""
        t0 = time.perf_counter()
        self.logger.info(
            "%s: Initializing CSB processing (%s-%s)", self.config.area, self.config.start_year, self.config.end_year
        )

        try:
            # Get year files
            year_files = self._get_year_files()

            # Initialize GDBs
            gdb_name = self._initialize_gdbs(year_files)

            # Combine rasters
            self.logger.debug("%s: Generating unique sequences (combine)...", self.config.area)
            combined_raster = self._combine_rasters(year_files)

            # Add and calculate COUNT0 field
            self._add_count_field(combined_raster)

            # Create null mask
            self.logger.debug("%s: Creating Null mask for pixels with < 1.1 years of data...", self.config.area)
            null_raster = self._create_null_mask(combined_raster)

            # Convert to vectors and project
            self.logger.debug("%s: Converting raster to vector polygons...", self.config.area)
            vector_polygons = self._convert_to_vectors(null_raster)

            # Perform eliminations with retry logic
            self._eliminate_polygons(vector_polygons)

            t1 = time.perf_counter()
            self.logger.info("%s: CSB generated in %s minutes", self.config.area, round((t1 - t0) / 60, 2))

        except Exception as e:
            self.logger.error("Processing failed: %s", e)
            self.error_path.write_text(str(e))
            raise ProcessingError("CSB processing failed") from e

    def _get_year_files(self) -> List[Path]:
        """Get list of raster files for each year"""
        year_files = []
        for year in range(self.config.start_year, self.config.end_year + 1):
            file_path = self.config.creation_dir / str(year) / f"{self.config.area}_{year}.TIF"
            if not file_path.exists():
                raise ProcessingError(f"Missing year file: {file_path}")
            year_files.append(file_path)
        return year_files

    def _initialize_gdbs(self, year_files: List[Path]) -> str:
        """Initialize geodatabases"""
        gdb_name = f"{self.config.area}_" f"{self.config.start_year}-{self.config.end_year}"

        initialize_gdbs(self.config.creation_dir, gdb_name, self.config.area, self.logger, self.error_path)
        return gdb_name

    def _combine_rasters(self, year_files: List[Path]) -> Path:
        """Combine raster files"""
        output_path = (
            self.config.creation_dir / "CombineALL" / f"{self.config.area}_{self.config.start_year}-"
            f"{self.config.end_year}.tif"
        )

        with handle_arcpy_errors(self.logger, "combine_rasters"):
            arcpy.gp.Combine_sa([str(f) for f in year_files], str(output_path))

        return output_path

    def _add_count_field(self, raster: Path) -> None:
        """Add and calculate the COUNT0 field"""
        attempt_count = 0
        max_attempts = 5

        while attempt_count < max_attempts:
            column_list = add_field(str(raster), self.config.area, self.logger, self.error_path)
            if column_list and "COUNT0" in column_list:
                break
            attempt_count += 1

        if attempt_count == max_attempts:
            raise ProcessingError(f"{self.config.area}: Failed to add 'COUNT0' field after {max_attempts} attempts")

        # Calculate field values
        calculate_field_lst = [
            f"!{self.config.area[:5]}_{year}!" for year in range(self.config.start_year, self.config.end_year + 1)
        ]

        cal_expression = f"CountFieldsGreaterThanZero({calculate_field_lst})"
        code_block = """
def CountFieldsGreaterThanZero(fieldList):
    counter = 0
    for field in fieldList:
        if int(field) > 0:
            counter += 1
    return counter
"""

        with handle_arcpy_errors(self.logger, "calculate_field"):
            arcpy.CalculateField_management(
                in_table=str(raster), field="COUNT0", expression=cal_expression, code_block=code_block
            )

    def _create_null_mask(self, raster: Path) -> Path:
        """Create Null mask for pixels with < 1.1 years of data"""
        output_path = (
            self.config.creation_dir / "Combine" / f"{self.config.area}_{self.config.start_year}-"
            f"{self.config.end_year}_NULL.tif"
        )

        with handle_arcpy_errors(self.logger, "create_null_mask"):
            arcpy.gp.SetNull_sa(str(raster), str(raster), str(output_path), '"COUNT0" < 1.1')

        return output_path

    def _convert_to_vectors(self, raster: Path) -> Path:
        """Convert raster to vector polygons and project to Albers"""
        # Convert to polygons
        out_feature_ll = (
            self.config.creation_dir / "Vectors_LL" / f"{self.config.area}_{self.config.start_year}-"
            f"{self.config.end_year}.gdb" / f"{self.config.area}_In"
        )

        with handle_arcpy_errors(self.logger, "raster_to_polygon"):
            arcpy.RasterToPolygon_conversion(
                in_raster=str(raster),
                out_polygon_features=str(out_feature_ll),
                simplify="NO_SIMPLIFY",
                raster_field="Value",
                create_multipart_features="SINGLE_OUTER_PART",
            )

        # Project to Albers
        out_feature_in = (
            self.config.creation_dir / "Vectors_In" / f"{self.config.area}_{self.config.start_year}-"
            f"{self.config.end_year}_In.gdb" / f"{self.config.area}_In"
        )

        with handle_arcpy_errors(self.logger, "project"):
            arcpy.management.Project(
                in_dataset=str(out_feature_ll),
                out_dataset=str(out_feature_in),
                out_coor_system=self.config.coordinate_system,
                transform_method=[],
                preserve_shape="NO_PRESERVE_SHAPE",
            )

        return out_feature_in

    def _eliminate_polygons(self, vectors: Path) -> None:
        """Perform polygon elimination with retry logic"""
        max_retries = 5  # Match original retry count
        retry_count = 0
        eliminate_success = False

        while not eliminate_success and retry_count < max_retries:
            try:
                scratch_gdb = (
                    self.config.creation_dir / "Vectors_temp" / f"{self.config.area}_{self.config.start_year}-"
                    f"{self.config.end_year}_temp.gdb"
                )

                with arcpy.EnvManager(scratchWorkspace=str(scratch_gdb), workspace=str(scratch_gdb)):
                    elim_config = EliminationConfig(
                        input_layers=vectors.parent.parent,
                        workspace=self.config.creation_dir
                        / "Vectors_Out"
                        / f"{self.config.area}_{self.config.start_year}-"
                        f"{self.config.end_year}_OUT.gdb",
                        scratch=scratch_gdb,
                        area=self.config.area,
                        coordinate_system=self.config.elimination_coordinate_system,
                        elimination_areas=self.config.elimination_areas,
                        max_retries=max_retries - retry_count,  # Adjust remaining retries
                    )

                    eliminator = PolygonEliminator(elim_config, self.logger)
                    eliminator.eliminate()
                    eliminate_success = True

            except Exception as e:
                retry_count += 1
                error_msg = f"{self.config.area}: Elimination failed (attempt {retry_count}): {e}"
                self.logger.error(error_msg)
                self.config.error_path.write_text(error_msg)

                if retry_count < max_retries:
                    # Try to repair topology and continue
                    from gdb_v2 import repair_topology

                    repair_topology(str(vectors.parent.parent), str(scratch_gdb), self.config.area, self.logger)
                else:
                    raise ProcessingError(f"Elimination failed after {max_retries} attempts") from e
