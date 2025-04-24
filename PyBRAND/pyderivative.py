import argparse
import sys
import os
from pathlib import Path
import yaml
import pickle as pkl


class PyDerivative():
    def __init__(self):
        # parse input arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('-p', '--parameters', type=str, required=True)
        parser.add_argument('-d', '--data', type=str, required=True)
        args = vars(parser.parse_args())
        parameter_path = Path(args["parameters"])
        
        with open(parameter_path, 'r') as f:
            self.parameters = yaml.safe_load(f)["parameters"]

        data_path = Path(args["data"])
        #check if the data file exists 
        if not os.path.exists(data_path):
            print(f"Data file {self.data} does not exist.")
            sys.exit(1)

        with open(data_path, 'rb') as f:
            self.data = pkl.load(f)



