"""
Configuration and initialization
"""

import shutil
import os
from configparser import ConfigParser, ExtendedInterpolation
import datetime as dt


def get_args(sys_argv):
    """Get arguments from sys.argv in CSB-Run.py"""
    # need minimum number of arguments
    if len(sys_argv) < 4:
        print("Missing arguments. Please provide <workflow> <startYear> <endYear>")
        print("Or for partial run <workflow>_partial <directory> <area>")
        # logging
    else:
        # get command line arguments
        workflow = sys_argv[1]
        batch_size = None
        config_file = None

        # get that partial run fun
        if workflow == "create_partial":
            directory = sys_argv[2]
            area = sys_argv[3]
            return workflow, directory, area, batch_size, config_file
        else:
            start_year = sys_argv[2]
            end_year = sys_argv[3]
            return workflow, start_year, end_year, batch_size, config_file


def get_config(config_arg):
    """Get the configuration file name or return default name 'csb_default.ini' if none provided"""
    config_dir = f"{os.getcwd()}\\config"

    if config_arg == "default":
        config_file = f"{config_dir}\\csb_default.ini"
    else:
        config_file = f"{config_dir}\\{config_arg}"

    config = ConfigParser(interpolation=ExtendedInterpolation())
    # config.read_file(open(config_file))
    config.read(config_file)

    return config


def set_run_params(config, args):
    """Set the run parameters based on command line and config file"""

    # ArcGIS python version
    arcgis_env = config["global"]["python_env"]
    print(f"Python env: {arcgis_env}")

    # CSB-Data directory
    data_dir = config["folders"]["data"]

    # create run name e.g create_1421
    runname_params = f"{args[0]}_{args[1][-2:]}{args[2][-2:]}"

    # make path to CSB workflow data folder
    scripts = {
        "create": "create_csb.py",
        "prep": "prep_csb.py",
        "distribute": "distribute_csb.py",
        "create_partial": "CSB-create_partial.py",
    }
    workflow = args[0]

    script = f"{os.getcwd()}\\CSB-Run\\CSB-Run\\{scripts[workflow]}"

    run_date = dt.datetime.today().strftime("%Y%m%d")
    runname = f"{runname_params}_{run_date}_"

    # TODO:  This is a hack to do variable substitution in the config file.
    #       It should be done automagically by the parser using
    #       configparser.ExtendedInterpolation
    try:
        creation_dir = config[workflow][f"{workflow}_folder"]
        creation_dir = creation_dir.replace("<runname>", runname)
    except:
        if workflow == "create_partial":
            creation_dir = config["create"]["create_folder"]
        else:
            creation_dir = config["prep"]["prep_folder"]
            creation_dir = creation_dir.replace("<runname>", runname)

    creation_dir = creation_dir.replace("<data>", data_dir)
    creation_dir = creation_dir.replace("<version>", config["global"]["version"])
    # check if there is a partial run!
    if workflow == "create_partial":
        creation_dir = creation_dir.replace("<runname>", args[1])
        partial_area = args[2]
    else:
        partial_area = None

    return arcgis_env, script, creation_dir, partial_area


def build_folders(creation_dir, workflow):
    """Create processing folders required for the CSB run using config file specified root folders"""

    creation_folders = [
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
    prep_folders = ["National_Subregion_gdb", "Subregion_gdb", "National_gdb", "log"]
    distribute_folders = ["National_Final_gdb", "State_gdb", "State", "log", "State/tif_state_extent"]

    # TODO:  Convert to use pathlib for path manipulation
    run_folder = creation_dir.split("/")[-1]
    base_dir = creation_dir.replace(run_folder, "")
    files = [f for f in os.listdir(base_dir) if f.startswith(run_folder)]

    # get folder name
    if len(files) > 0:
        # condition for picking right file
        version = [int(f.split("_")[-1]) for f in files if f.startswith(run_folder)]

        new_version = max(version) + 1

    else:
        new_version = 1

    # actual run directory name
    run_dir = f"{base_dir}{run_folder}{new_version}"

    # build appropriate folders
    if workflow == "create" or workflow == "create_test":
        try:
            os.makedirs(run_dir)
        except:
            pass
        # create subfolders
        if os.path.exists(run_dir):
            for f in creation_folders:
                try:
                    os.mkdir(f"{run_dir}/{f}")
                except:
                    pass
        print(f"Directory built: {run_dir}")

    elif workflow == "prep":
        try:
            os.makedirs(run_dir)
        except:
            pass

        if os.path.exists(run_dir):
            for f in prep_folders:
                try:
                    os.mkdir(f"{run_dir}/{f}")
                except:
                    pass

    elif workflow == "distribute":
        try:
            os.makedirs(run_dir)
        except:
            pass

        if os.path.exists(run_dir):
            for f in distribute_folders:
                try:
                    os.mkdir(f"{run_dir}/{f}")
                except:
                    pass

    elif workflow == "create_partial":
        run_dir = creation_dir

    else:
        print(f'"{workflow}" is not a valid workflow. Choose "create", "prep", or "distribute"')

    return run_dir


def get_run_folder(workflow, start_year, end_year):
    """Determine which creation run folder to use for given CSB prep parameters"""

    cfg = get_config("default")
    data_path = f"{cfg['folders']['data']}/v{cfg['global']['version']}"

    if workflow == "prep":
        create_path = f"{data_path}/Creation"
        prefix = "create"
    elif workflow == "distribute":
        create_path = f"{data_path}/Prep"
        prefix = "prep"

    files_prefix = f"{prefix}_{str(start_year)[2:]}{str(end_year)[2:]}_"
    files = [f for f in os.listdir(create_path) if f.startswith(files_prefix) and not f.endswith("BAD")]
    file_date_list = []
    for f in files:
        file_list = f.split("_")
        file_date = file_list[2]
        file_date = dt.datetime.strptime(file_date, "%Y%m%d")
        file_date_list.append(file_date)

    latest_date = max(file_date_list)
    latest_indeces = [i for i, x in enumerate(file_date_list) if x == latest_date]
    latest_files = [files[i] for i in latest_indeces]
    # this assumes the latest is the last in the list from os.listdir
    if len(files) > 0:
        run_path = f"{create_path}/{latest_files[-1]}"
        return run_path
    else:
        print("No create directory found for given years")
        quit()


def delete_gdbs(area, directory):
    """Delete folders and GDBs for a specified processing subunit"""

    # relevant folders are in create directory currently
    creation_folders = ["Combine", "CombineAll", "Merge", "Vectors_In", "Vectors_LL", "Vectors_Out", "Vectors_temp"]

    print(f"Deleting old files for {area}")

    for folder in creation_folders:
        check_folder = f"{directory}/{folder}"
        for f in os.listdir(check_folder):
            if f.startswith(f"{area}_"):
                if f.endswith(".gdb"):
                    shutil.rmtree(f"{check_folder}/{f}")
                else:
                    os.remove(f"{check_folder}/{f}")


# determine multiprocessing batch size
# TODO: Eliminate this function as not needed with now multiprocessing algorithm
# def GetBatch(workflow, batch_size):
#     # needs work, set to defaults currently
#     cpu_count = multiprocessing.cpu_count()
#     cfg = GetConfig("default")
#     cpu_perc = cfg["global"]["cpu_perc"]
#     run_cpu = int(round(cpu_perc * cpu_count, 0))
#     # print(run_cpu)
#     return run_cpu
