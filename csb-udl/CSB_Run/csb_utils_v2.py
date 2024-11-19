"""Configuration and initialization utilities"""

from configparser import ConfigParser, ExtendedInterpolation
from pathlib import Path
import datetime as dt
from typing import Optional, List, Dict, Any

from csb.types import CSBError


class ConfigurationError(CSBError):
    """Error in configuration handling"""

    pass


class RunFolderError(CSBError):
    """Error in run folder handling"""

    pass


def get_config(config_name: str = "default") -> ConfigParser:
    """Get configuration settings

    Args:
        config_name: Name of config file (default: "default")

    Returns:
        ConfigParser with loaded settings

    Raises:
        ConfigurationError: If config file cannot be loaded
    """
    config_dir = Path.cwd() / "config"

    if config_name == "default":
        config_file = config_dir / "csb_default.ini"
    else:
        config_file = config_dir / f"{config_name}.ini"

    if not config_file.exists():
        raise ConfigurationError(f"Config file not found: {config_file}")

    config = ConfigParser(interpolation=ExtendedInterpolation())
    try:
        config.read(config_file)
        return config
    except Exception as e:
        raise ConfigurationError(f"Failed to load config file: {e}") from e


def validate_config(config: ConfigParser) -> None:
    """Validate configuration settings

    Args:
        config: Configuration to validate

    Raises:
        ConfigurationError: If configuration is invalid
    """
    required_sections = ["global", "folders"]
    required_settings = {
        "global": ["version", "python_env", "cpu_prct"],
        "folders": ["code", "data", "raw_data", "split_rasters", "log"],
    }

    # Check required sections
    missing_sections = [s for s in required_sections if s not in config.sections()]
    if missing_sections:
        raise ConfigurationError(f"Missing required sections: {missing_sections}")

    # Check required settings
    for section, settings in required_settings.items():
        missing_settings = [s for s in settings if s not in config[section]]
        if missing_settings:
            raise ConfigurationError(f"Missing required settings in section {section}: {missing_settings}")

    # Validate CPU percentage
    try:
        cpu_prct = float(config["global"]["cpu_prct"])
        if not 0 < cpu_prct <= 1:
            raise ConfigurationError("cpu_prct must be between 0 and 1")
    except ValueError as e:
        raise ConfigurationError("cpu_prct must be a valid float") from e


def get_run_folder(workflow: str, start_year: int, end_year: int) -> Path:
    """Get the run folder path for given workflow and years

    Args:
        workflow: Workflow name ("prep" or "distribute")
        start_year: Start year
        end_year: End year

    Returns:
        Path to run folder

    Raises:
        RunFolderError: If run folder cannot be determined
    """
    if workflow not in ("prep", "distribute"):
        raise RunFolderError(f"Invalid workflow: {workflow}")

    try:
        cfg = get_config()
        validate_config(cfg)

        data_path = Path(cfg["folders"]["data"]) / f"v{cfg['global']['version']}"

        if workflow == "prep":
            create_path = data_path / "Creation"
            prefix = "create"
        else:  # distribute
            create_path = data_path / "Prep"
            prefix = "prep"

        # Find latest matching run folder
        pattern = f"{prefix}_{str(start_year)[2:]}{str(end_year)[2:]}_*"
        matching_folders = sorted(
            [f for f in create_path.glob(pattern) if not f.name.endswith("BAD")],
            key=lambda p: dt.datetime.strptime(p.name.split("_")[2], "%Y%m%d"),
        )

        if not matching_folders:
            raise RunFolderError(f"No {workflow} directory found for years {start_year}-{end_year}")

        return matching_folders[-1]

    except ConfigurationError as e:
        raise RunFolderError("Configuration error") from e
    except Exception as e:
        raise RunFolderError(f"Failed to get run folder: {e}") from e


def build_folders(creation_dir: Path, workflow: str) -> Path:
    """Create processing folders for CSB run

    Args:
        creation_dir: Base creation directory
        workflow: Workflow type

    Returns:
        Path to run directory

    Raises:
        RunFolderError: If folder creation fails
    """
    if workflow not in ("create", "create_test", "prep", "distribute"):
        raise RunFolderError(f"Invalid workflow: {workflow}")

    try:
        # Determine folder structure
        if workflow.startswith("create"):
            folders = [
                "Combine",
                "CombineAll",
                "Merge",
                "Vectors_In",
                "Vectors_LL",
                "Vectors_Out",
                "Vectors_temp",
                "log",
                "Raster_Out",
            ]
        elif workflow == "prep":
            folders = ["National_Subregion_gdb", "Subregion_gdb", "National_gdb", "log"]
        else:  # distribute
            folders = ["National_Final_gdb", "State_gdb", "State", "log", "State/tif_state_extent"]

        # Create folders
        for folder in folders:
            folder_path = creation_dir / folder
            folder_path.mkdir(parents=True, exist_ok=True)

        return creation_dir

    except Exception as e:
        raise RunFolderError(f"Failed to create folders: {e}") from e


def get_version_folder(base_dir: Path, run_folder: str) -> Path:
    """Get versioned run folder

    Args:
        base_dir: Base directory
        run_folder: Run folder name

    Returns:
        Path to versioned run folder
    """
    # Get existing versions
    existing = [f for f in base_dir.glob(f"{run_folder}*")]
    if not existing:
        return base_dir / f"{run_folder}1"

    # Get highest version
    versions = [int(f.name.replace(run_folder, "")) for f in existing if f.name.replace(run_folder, "").isdigit()]
    next_version = max(versions, default=0) + 1

    return base_dir / f"{run_folder}{next_version}"
