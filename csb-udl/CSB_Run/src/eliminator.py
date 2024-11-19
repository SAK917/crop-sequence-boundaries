"""Polygon elimination functionality"""

from dataclasses import dataclass
from pathlib import Path
import math
import time
from typing import Optional, Generator, Tuple
import arcpy
from logging import Logger

from csb.constants import ELIMINATION_COORDINATE_SYSTEM, DEFAULT_ELIMINATION_AREAS
from csb.errors import handle_arcpy_errors, ProcessingError
from csb.types import FeatureClass


@dataclass
class EliminationConfig:
    """Configuration for polygon elimination"""

    input_layers: Path
    workspace: Path
    scratch: Path
    area: str
    coordinate_system: str  # Remove default
    elimination_areas: tuple[int, ...] = DEFAULT_ELIMINATION_AREAS
    max_retries: int = 3
    retry_delay: float = 5.0

    def __post_init__(self):
        """Validate coordinate system"""
        if self.coordinate_system != ELIMINATION_COORDINATE_SYSTEM:
            raise ValueError(f"Invalid coordinate system for elimination. " f"Must use ELIMINATION_COORDINATE_SYSTEM")


class PolygonEliminator:
    """Handles polygon elimination operations"""

    def __init__(self, config: EliminationConfig, logger: Logger):
        self.config = config
        self.logger = logger

    def eliminate(self) -> None:
        """Perform polygon elimination with retry logic"""
        self.logger.info("%s: Starting polygon elimination process...", self.config.area)

        arcpy.env.overwriteOutput = True
        retry_count = 0

        while retry_count < self.config.max_retries:
            try:
                with arcpy.EnvManager(
                    scratchWorkspace=str(self.config.scratch),
                    workspace=str(self.config.scratch),
                    outputCoordinateSystem=self.config.coordinate_system,
                ):
                    self._process_layers()
                return  # Success

            except Exception as e:
                retry_count += 1
                if retry_count < self.config.max_retries:
                    self.logger.warning(
                        "%s: Elimination attempt %d failed: %s. Retrying in %d seconds...",
                        self.config.area,
                        retry_count,
                        str(e),
                        self.config.retry_delay,
                    )
                    time.sleep(self.config.retry_delay)
                else:
                    self.logger.error(
                        "%s: Elimination failed after %d attempts: %s", self.config.area, retry_count, str(e)
                    )
                    raise ProcessingError(f"Elimination failed after {retry_count} attempts") from e

    def _process_layers(self) -> None:
        """Process each feature layer"""
        feature_layers = list(self._get_feature_layers())
        if not feature_layers:
            raise ProcessingError(f"{self.config.area}: No feature layers found in {self.config.input_layers}")

        for feature_class, layer_name in feature_layers:
            self.logger.debug("%s: Processing layer %s...", self.config.area, layer_name)

            last_iteration_name = layer_name
            for size in self.config.elimination_areas:
                self.logger.debug("%s: Eliminating polygons <= %sm2...", self.config.area, size)
                result = self._process_elimination(layer_name, last_iteration_name, size)
                if result:
                    last_iteration_name = result
                else:
                    self.logger.warning("%s: Skipping size %sm2 due to processing error", self.config.area, size)

    def _get_feature_layers(self) -> Generator[Tuple[FeatureClass, str], None, None]:
        """Generate feature layers from input

        Yields:
            Tuple of (feature class path, layer name)

        Raises:
            ProcessingError: If feature layer creation fails
        """
        with handle_arcpy_errors(self.logger, "list_feature_classes"):
            for fc in arcpy.ListFeatureClasses(feature_type="POLYGON"):
                layer_name = Path(fc).stem
                try:
                    with handle_arcpy_errors(self.logger, f"make_feature_layer_{layer_name}"):
                        arcpy.management.MakeFeatureLayer(in_features=fc, out_layer=layer_name)
                        yield fc, layer_name
                except ProcessingError:
                    self.logger.warning(
                        "%s: Skipping feature class %s due to layer creation error", self.config.area, fc
                    )
                    continue

    def _process_elimination(self, layer_name: str, last_iteration_name: str, shape_area: int) -> Optional[str]:
        """Process one elimination iteration with retry logic

        Args:
            layer_name: Base layer name
            last_iteration_name: Name from previous iteration
            shape_area: Area threshold for elimination

        Returns:
            New layer name if successful, None otherwise
        """
        retry_count = 0
        while retry_count < self.config.max_retries:
            try:
                return self._eliminate_iteration(layer_name, last_iteration_name, shape_area)
            except Exception as e:
                retry_count += 1
                if retry_count < self.config.max_retries:
                    self.logger.warning(
                        "%s: Elimination iteration failed (attempt %d): %s", self.config.area, retry_count, str(e)
                    )
                    time.sleep(self.config.retry_delay)
                else:
                    self.logger.error(
                        "%s: Elimination iteration failed after %d attempts: %s", self.config.area, retry_count, str(e)
                    )
                    return None

    def _eliminate_iteration(self, layer_name: str, last_iteration_name: str, shape_area: int) -> Optional[str]:
        """Execute single elimination iteration

        Args:
            layer_name: Base layer name
            last_iteration_name: Name from previous iteration
            shape_area: Area threshold for elimination

        Returns:
            New layer name if successful, None otherwise

        Raises:
            ProcessingError: If elimination fails
        """
        new_layer_name = last_iteration_name or layer_name
        iteration = 0
        previous_poly_count = math.inf

        while True:
            iteration += 1

            # Select polygons to eliminate
            with handle_arcpy_errors(self.logger, f"select_polygons_{iteration}"):
                polys = arcpy.management.SelectLayerByAttribute(
                    in_layer_or_view=new_layer_name,
                    selection_type="NEW_SELECTION",
                    where_clause=f"Shape_Area <= {shape_area}",
                )
                poly_count = int(arcpy.management.GetCount(polys)[0])

            if poly_count == 0 or poly_count >= previous_poly_count:
                break

            # Eliminate selected polygons
            temp_name = f"{self.config.scratch}/{layer_name}_{shape_area}_{iteration}"
            new_layer_name = f"{layer_name}_{shape_area}_{iteration}_Layer"

            with handle_arcpy_errors(self.logger, f"eliminate_polygons_{iteration}"):
                arcpy.management.Eliminate(in_features=polys, out_feature_class=temp_name, selection="LENGTH")

                arcpy.management.MakeFeatureLayer(in_features=temp_name, out_layer=new_layer_name)

            previous_poly_count = poly_count

            self.logger.debug(
                "%s: Iteration %d eliminated %d polygons", self.config.area, iteration, previous_poly_count - poly_count
            )

        return new_layer_name
