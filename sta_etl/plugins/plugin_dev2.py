from sta_etl.plugin_handler.etl_collector import Collector

import pandas as pd
import numpy as np


@Collector
class Plugin_Dev2():
    """
    This is template development plugin for proving the processing chain right with
    depending plugins. This plugin is part of Plugin_Dev1(...) and can be used together
    such as:

    :Example:
        sta_cli process --hash 697b5d35 --type "Plugin_Dev2"

    This will trigger a processing chain based on the module dependency of Plugin_Dev2
    to Plugin_Dev1, create random data and stores the outcome in the STA database core.

    As a developer you can set
    - self._proc_success (True or False)
    - self._proc_success (None or pd.DataFrame)
    to simulate processing a processing chain along the way.

    """
    def __init__(self):
        """
        The class init function. This function holds only information
        about the plugin itself. In that way we can always load the plugin
        without initiating further variables and member functions.
        """
        self._plugin_config = {
            "plugin_name": "Plugin_Developement2",
            "plugin_dependencies": ["gps", "devel1"],
            "plugin_description": """
            This is development plugin (2) to prove functioning of the processing
            architecture of STA.
            """,
            "leaf_name": "devel2"
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
        In this template plugin, we hold the processing instruction in this function.

        The aim of this plugin to simulate the handling of processing dependencies.
        :return:
        """

        # Fetch all important data for calculations (Example):
        # Use always .get(...) for self._data_dict to be in control of the existence
        # of the data object.
        gps_data = self._data_dict.get("gps")

        # Implement you code here! We will just show the plugin configuration:
        self.print_plugin_config()

        # Create a fake result:
        # As a developer you will use this section to simulate if a plugin processing
        # chain is successful and what is reported back to the PluginLoader.
        df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)),
                          columns=["devel2-A", "devel2-B", "devel2-C", "devel2-D"])

        # Fake results:
        self._proc_result = df
        self._proc_success = True
