from sta_etl.plugin_handler.etl_collector import Collector

import pandas as pd
import numpy as np
from geopy.distance import distance as geopy_distance
import math

@Collector
class Plugin_SimpleProjection():
    """
    This plugin extract and aggregate information for further analysis.
    """
    def __init__(self):
        """
        The class init function. This function holds only information
        about the plugin itself. In that way we can always load the plugin
        without initiating further variables and member functions
        """
        self._plugin_config = {
            "plugin_name": "Simple_Projection",
            "plugin_dependencies": ["simple_distances", "gps"],
            "plugin_description": """
            This plugin calculates simple projections on distances, velocities and other
            quantities.
            """,
            "leaf_name": "simple_projection"
        }

    def __del__(self):
        """
        At this point, adjust the destructor of your plugin to remove unnecessary
        objects from RAM. In that way we can keep the RAM usage low.
        :return: None
        """
        pass

    def init(self):
        """
        The "true" init is used here to setup the plugin. At this point, a dictionary
        (self._data_dict) is created which holds data which are required that this plugin runs through.
        (see set_plugin_data(...) for more information). If self._data_dict is not set
        externally, it could also mean that there are no requirements for data sources.
        Have a look at the processing instruction of this plugin to verify its function.

        .. note::
            - self._data_dict is always a dictionary which can be empty if not data are
              required by this plugin
            - self._proc_success is always False initially. Set to True if processing is
              successful to notify the PluginLoader about the outcome.
            - self._proc_result is initially None and becomes a pandas DataFrame or any
              other data storage object. It is mandatory that the PluginLoader understands
              how to handle the result and write it to the underlying storage facility.
        :return: None
        """
        self._data_dict = {}
        self._proc_success = False
        self._proc_result = None

    def get_result(self):
        """
        A return function for this plugin to transfer processed data the the PluginLoader.

        This plugin returns None or pd.DataFrame as result. The plugin handler needs to
        understand return object for creating the correct database entry and handle storage
        of the plugin result on disk. See i_process(...) in loader.py for handling the result.

        :return: Pandas DataFrame or None
        """
        return self._proc_result

    def get_processing_success(self):
        """
        Reports the processing status back to the PluginLoader. This variable is set to False
        by default and needs to be set to True if processing of the plugin is successful.
        :return: bool
        """
        return self._proc_success

    def get_plugin_config(self):
        """
        Standard function: Return
        :return: A dictionary with the plugin configuration
        """
        return self._plugin_config

    def print_plugin_config(self):
        """
        This one is just presenting the initial plugin configuration inside or outside this
        plugin to users.
        .. todo: This function uses Python print(...) right now. Change to logging soon.

        :return: None
        """
        print("<-----------")
        print(f"Plugin name {self._plugin_config.get('name')}")
        print(f"Plugin dependencies: {self._plugin_config.get('plugin_dependencies')}")
        print(f"Plugin produces leaf name (aka data asset): {self._plugin_config.get('leaf_name')}")
        print(f"Plugin description:")
        print(self._plugin_config.get('plugin_description'))
        print("<-----------")

    def set_plugin_data(self, data_dict={}):
        """
        A function to set the necessary data as a dictionary. The dictionary self._data_dict
        is set before when running init(...) but have to set dictionary data beforehand when
        your code below requires it for running.

        :param data_dict: dictionary
            A dictionary with data objects which can be understood by the processor code
            below.
        :return: None
        """

        self._data_dict = data_dict

    def run(self):
        """
        A data processor can be sometimes more complicated. So you are supposed to use
        run(...) as call for starting the processing instruction. You might like to put
        control mechanism to it check the correct behavior of the plugin processor code.

        .. note::
            All processing instruction, helper functions,... are in the scope of "private"
            of this plugin processor class. Therefore, stick to the _<name> convention when
            defining names in your plugins.

        :return: None
        """
        #Run individual steps of the data processing:
        self._processer()


    def _processer(self):
        """
        The main function which is used in this plugin to process data
        :return:
        """
        #Fetch all important data for calculations:
        sdistances = self.data_dict.get("simple_distances")
        sgps = self.data_dict.get("gps")

        final = {"tot_dist_geodasic": [sdistances["dist_geodasic"].sum()],
                 "tot_dist_euclidiac": [sdistances["dist_euclidiac"].sum()],
                 "tot_duration": [sdistances["duration"].sum()],
                 "median_velocity_geodasic": [sdistances["velocity_geodasic"].median()],
                 "mean_velocity_geodasic": [sdistances["velocity_geodasic"].mean()],
                 "median_velocity_euclidic": [sdistances["velocity_euclidic"].median()],
                 "mean_velocity_euclidic": [sdistances["velocity_euclidic"].mean()]
                 }

        sgps["altitudeDiff"] = sgps["altitude"].shift(1) - sgps["altitude"]

        #print(sgps["altitudeDiff"].to_list())
        # self.df_result = pd.DataFrame(data=results)
        pos = sgps[(sgps["altitudeDiff"] > 0)]
        neg = sgps[(sgps["altitudeDiff"] < 0)]
        pos_sum = pos["altitudeDiff"].sum()
        neg_sum = neg["altitudeDiff"].sum()
        final["altitude_up"] = [pos_sum]
        final["altitude_dw"] = [neg_sum]

        final["max_velocity_geodasic"] = [sdistances["velocity_geodasic"].describe()["max"]]
        final["m75p_velocity_geodasic"] = [sdistances["velocity_geodasic"].describe()["75%"]]
        final["m50p_velocity_geodasic"] = [sdistances["velocity_geodasic"].describe()["50%"]]
        final["m25p_velocity_geodasic"] = [sdistances["velocity_geodasic"].describe()["25%"]]
        final["min_velocity_geodasic"] = [sdistances["velocity_geodasic"].describe()["min"]]
        final["std_velocity_geodasic"] = [sdistances["velocity_geodasic"].describe()["std"]]
        #final["mean_velocity_geodasic"] = [sdistances["velocity_geodasic"].describe()["mean"]]

        final["max_velocity_euclidic"] = [sdistances["velocity_euclidic"].describe()["max"]]
        final["m75p_velocity_euclidic"] = [sdistances["velocity_euclidic"].describe()["75%"]]
        final["m50p_velocity_euclidic"] = [sdistances["velocity_euclidic"].describe()["50%"]]
        final["m25p_velocity_euclidic"] = [sdistances["velocity_euclidic"].describe()["25%"]]
        final["min_velocity_euclidic"] = [sdistances["velocity_euclidic"].describe()["min"]]
        final["std_velocity_euclidic"] = [sdistances["velocity_euclidic"].describe()["std"]]
        #final["mean_velocity_geodasic"] = [sdistances["velocity_geodasic"].describe()["mean"]]

        self._proc_result = pd.DataFrame(data=final)

        #print(self.df_result)
        # if you make it to here:
        self._proc_success = True
