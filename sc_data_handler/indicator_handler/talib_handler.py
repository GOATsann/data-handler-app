import talib
from talib.abstract import Function as _abstractFunction
from typing import Tuple, Callable, Dict
import numpy as np
import json
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, "extra_ind_info.json")

extra_ind_info = {}
with open(file_path, "r") as f:
    extra_ind_info = json.load(f)


class TaLibIndicatorHandler:
    def get_abstractFunction(self, indicator_name: str) -> Tuple[Callable, Dict]:
        try:
            return _abstractFunction(indicator_name), None
        except Exception as e:
            return None, {"error": str(e)}

    def get_indicator(self, indicator_name: str = "", inputs: dict = {}, **kwargs):
        # you can find possible kwargs by looking at the indicator info
        # Input must have atleast one of open, high, low, close, volume
        if not any(
            [key in inputs for key in ["open", "high", "low", "close", "volume"]]
        ):
            return {
                "error": "Input must have atleast one of open, high, low, close, volume"
            }
        indicator_func, err = self.get_abstractFunction(indicator_name)

        if err:
            return indicator_func
        # ensure lists are in numpy.ndarray format
        for key in inputs:
            inputs[key] = np.array(inputs[key])
        return indicator_func(inputs, **kwargs)

    def get_item_info(self, indicator_name: str):
        func, err = self.get_abstractFunction(indicator_name)
        if err:
            return func
        info = func.info
        # convert float values in info to string,
        # if not the client may convert them to ints if they only have a decimal point 0
        parameters = info["parameters"]
        input_names = info["input_names"]
        for key in parameters:
            if isinstance(parameters[key], float):
                parameters[key] = str(parameters[key])
        for key in input_names:
            if isinstance(input_names[key], float):
                input_names[key] = str(input_names[key])
        info["parameters"] = parameters
        info["input_names"] = input_names
        return info

    def get_lib_descriptions(self):
        descriptions = {}
        for group, names in talib.get_function_groups().items():
            descriptions[group] = {}
            for name in names:
                descriptions[group][name] = self.get_item_info(name)
                descriptions[group][name].update(
                    extra_ind_info.get(group, {}).get(name, {})
                )
        return descriptions
