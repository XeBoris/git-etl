from sta_etl.plugin_handler.etl_collector import Collector

import pandas as pd
import numpy as np
from geopy.distance import distance as geopy_distance
import math

@Collector
class Plugin_SimpleDistance():
    """
    This is a simple distance plugin to calculate individual time and position
    differences.
    """
    def __init__(self):
        """
        The class init function. This function holds only information
        about the plugin itself. In that way we can always load the plugin
        without initiating further variables and member functions
        """
        self._plugin_config = {
            "plugin_name": "Simple_Distance_Calculator",
            "plugin_dependencies": ["gps"],
            "plugin_description": """
            This is a simple distance plugin to calculate individual time and position
            differences.
            """,
            "leaf_name": "simple_distances"
        }

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
        gps_data = self.data_dict.get("gps")

        # Your calculations start here:
        #collecter:
        dt_list = []
        dx_list = []
        dxz_list = []
        time_list = []
        gps0 = None
        time0 = None

        # Iterate over rows, extract, transform, append
        for row in gps_data.T.iteritems():
            # Extract the first row
            i_gps = (row[1]["latitude"], row[1]["longitude"], row[1]["altitude"])
            i_time = row[1]["timestamp"]
            time_list.append(i_time)
            # Set the gps/time to zero
            if gps0 is None:
                gps0 = i_gps
            if time0 is None:
                time0 = i_time

            # time difference:
            #dt = (i_time - time0).total_seconds()
            dt = (i_time - time0)

            # geodasic distance (x/y)
            dx = geopy_distance(i_gps[:2], gps0[:2]).m

            # Euclidian distance
            dxz = math.sqrt(dx ** 2 + (i_gps[2] - gps0[2]) ** 2)

            dt_list.append(dt)
            dx_list.append(dx)
            dxz_list.append(dxz)

            gps0 = i_gps
            time0 = i_time

        # post processing:
        dt_cumsum_list = np.cumsum(dt_list)
        dx_cumsum_list = np.cumsum(dx_list)
        dxz_cumsum_list = np.cumsum(dxz_list)

        dv_geodasic = [i / j if j > 0 else 0 for i, j in zip(dx_list, dt_list)]
        dv_euclidian = [i / j if j > 0 else 0 for i, j in zip(dxz_list, dt_list)]

        # Extract information from the calculations for the final dataframe:
        results = {
            'timestamp': time_list,
            'duration': dt_list,
            'duration_sum': dt_cumsum_list,
            'dist_geodasic': dx_list,
            'dist_euclidiac': dxz_list,
            'dist_geodasic_sum': dx_cumsum_list,
            'dist_euclidiac_sum': dxz_cumsum_list,
            'velocity_geodasic': dv_geodasic,
            'velocity_euclidic': dv_euclidian
        }

        self._proc_result = pd.DataFrame(data=results)


        #if you make it to here:
        self._proc_success = True
