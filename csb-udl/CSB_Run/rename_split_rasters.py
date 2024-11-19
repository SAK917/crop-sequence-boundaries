""" Rename split rasters to match the naming convention required by CSB-Project.
"""
import os


def rename_split_rasters() -> None:
    """Iterates through each subfolder (year) of the split rasters folder and renames
    all of the files to conform with the format the USDA code expects."""
    split_rasters = r"CSB-Data\v2.5\Split-Rasters"
    for root, _, files in os.walk(split_rasters):
        for file in files:
            filename, extension = os.path.splitext(file)
            subregion = filename.split("cdls")[1].split(".")[0]
            year = os.path.basename(root)
            new_filename = f"CONUS_{subregion}_{year}{extension}"
            os.rename(os.path.join(root, file), os.path.join(root, new_filename))


if __name__ == "__main__":
    rename_split_rasters()
