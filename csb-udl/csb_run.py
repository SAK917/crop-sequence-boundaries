"""Entry point for running CSB processing scripts
"""

import os
import sys
import subprocess
import importlib  # CSB-Run util functions

utils = importlib.import_module("CSB-Run.utils")

# get command line args, 3 minimum required, missing args None
# input args: workflow, startYear and endYear
args = utils.get_args(sys.argv)

# get CSB-Run config file, default csb_default.ini
print(f"Config file: {os.getcwd()}\\config\\csb_default.ini")
cfg = utils.get_config("default")

# set parameters for CSB-Run
python_path, script, creation_dir, partial = utils.set_run_params(cfg, args)

# build the folders for csb creation/prep run
# args[0] = workflow
run_dir = utils.build_folders(creation_dir, args[0])

# run the CSB workflow script
# args[1] = startYear, args[2] = endYear
p = subprocess.run([python_path, script, args[1], args[2], run_dir, str(partial)], check=False)
