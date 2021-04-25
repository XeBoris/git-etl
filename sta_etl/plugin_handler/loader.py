from sta_etl.plugin_handler.etl_collector import Collector, NameCollector, ClassCollector
from sta_etl.plugins.plugin_dummy import Plugin_Dummy
from sta_etl.plugins.plugin_dev1 import Plugin_Dev1
from sta_etl.plugins.plugin_dev2 import Plugin_Dev2
from sta_etl.plugins.plugin_aggregates import Plugin_SimpleProjection
from sta_etl.plugins.plugin_simple_distances import Plugin_SimpleDistance

import re
import pandas as pd

class PluginLoader():
    """
    This is PluginLoader(...) - This class handles all registered plugins to this tool
    and manages required connections with the sta_core to register processed plugins
    and read/write data from/to disk.

    .. note::
        A new plugin is registered in the following steps:
        1) Copy a template from the plugins directory
        1.1) adjust class name according to name convention: Plugin_<class plugin name>
        1.2) In __init__(...): plugin_name according to Plugin_<plugin name>
             not necessarily the same name such as class name. (mandatory)
        1.3) In __init__(...): plugin_dependencies to add all leaf_names which are
             required for processing the plugin. A leaf name describes
             the data asset of the plugin itself. Leaf names are unique across all plugins
             and use a string, only numbers and characters. (mandatory)
        1.4) In __init__(...): plugin_description is a longer description of your plugin
             purpose. You may like to describe it a bit that humans understand your idea
             better. (optional)
        1.5) In __init__(...): leaf_name of this plugin (see plugin_dependencies)
        2) Add a plugin import ON TOP OF THIS FILE such as:
            from sta_etl.plugins.pluyin_<file name> import Plugin_<class plugin name>

    .. todo:
        - Replace Python print(...) by logging soon.
        - Add a plugin check instance to verify the registered plugins in the plugin
          collector
    """

    def __init__(self):
        """
        PluginLoader constructor.

        .. note::
            self.dbh: LoaderPlugin has to handle the STA core. The dbh object is passed
                to LoaderPlugin to make this happen.
            self.all_leaves: A list of available leaves based on the registered plugins.
                This is set by a evaluation of a helper function get_all_existing_leaf_names(...)
                in this class. Read up there if interested.
            self.leaf_name_to_plugin_name: A dictionary which holds the information from
                the (unique) leaf name back to the chosen plugin name.
                This can be become more complicated of course in the future. Implement this
                in get_all_existing_leaf_names(...) later.
            self.overwrite: A bool to control if you are up to re-create a plugin again.


        """
        self.dbh = None
        self.all_leaves = []
        self.leaf_name_to_plugin_name = {}
        self.overwrite = False
        self.get_all_existing_leaf_names()

        # clean up
        # self.track_hash = None
        # self.existing_leaves = None
        # self.existing_leave_names = None
        # self.plugins_to_process = None

    def get_all_existing_leaf_names(self):
        """
        This function collects all existing leaf names from all
        registered plugins in the plugin collector. If a plugin is
        requested to be processed but it is not contained in this list
        something is wrong! The function is called as part of the init
        process of PluginLoader(...)
        :return: None
        """
        self.all_leaves = []
        self.leaf_name_to_plugin_name = {}
        for i_plugin in NameCollector:
            # print(i_plugin)
            it = ClassCollector[i_plugin]
            k = it.get_plugin_config()
            self.all_leaves.append(it.get_plugin_config().get("leaf_name"))
            self.leaf_name_to_plugin_name[it.get_plugin_config().get("leaf_name")] = i_plugin
            del it

    def set_processor_plugins(self, plugins=None):
        """
        Plugin names set externally to specify which plugins are going to be
        processed. We will simply process all available plugins if nothing else
        is set from outside.

        .. todo:
            - In the future, we do not want to process all existing plugins simply
              by their occurrence.

        :param plugins: str
            A string of plugin names which are going to be processed. The string
            can contain ',' or '-' or '!' to separate plugin names.
        :return:
        """
        if plugins is None:
            self.plugins_to_process = self.get_all_plugins()
            print(f"Plugins {self.plugins_to_process} are available.")
        elif isinstance(plugins, str):
            split_set = ",|-|!"
            plugins = re.split(split_set, plugins)
            f_plugins = []
            for i_plugin in plugins:
                if i_plugin.find("Plugin_") == -1:
                    f_plugins.append(f"Plugin_{i_plugin}")
                else:
                    f_plugins.append(f"{i_plugin}")

            if set(f_plugins).issubset(self.get_all_plugins()):
                self.plugins_to_process = f_plugins
                print(f"Plugins {self.plugins_to_process} are available.")
            else:
                print(f"One or all plugins of {f_plugins} are not contained")
                print(f"in the list of registered plugins {self.get_all_plugins()}.")
                exit()

    def allow_overwrite(self):
        """
        If activated, you are allowed to reprocess and re-write the plugins which are
        existing already.
        :return:
        """
        self.overwrite = True

    def get_all_plugins(self):
        """
        Extract all available plugins/leaves in the "plugins" folder.
        :return:
        """
        return NameCollector

    def set_database_handler(self, dbh):
        """
        Handover a database handler such it is defined within in db_handler.py
        We are going to use the db_handler.py here. In case we want to change
        to some other handler, mind that you need to adjust the member functions.

        :param dbh: A database handler from db_handler.py
        :return: -
        """
        self.dbh = dbh

    # def set_track_by_hash(self, track_hash):
    #     """
    #     The PluginLoader can process many plugins. Therefore, this member
    #     function setup your track information to which the database
    #     handler will interact with
    #     :param track_hash: A hash (string)
    #     :return:
    #     """
    #     self.track_hash = track_hash

    # def set_existing_leaves_for_track(self, existing_leaves):
    #     """
    #     Prepare the PluginLoader with existing leaves.
    #     :param existing_leaves:
    #     :return:
    #     """
    #     self.existing_leaves = existing_leaves
    #     self.existing_leaf_names = list(existing_leaves.keys())

    def _read_leaf_data(self, required_leaves):
        """
        This helper function allows you to handle data requests from sta core.
        All it needs to get a list of leaves from the 'leaf' description in the
        underlying database.

        .. note::
            Right now, Pandas dataframes are used as exchange objects. We can change
            this at any point, but then we need to communicate this to this function.
            as well. Otherwise, the sta-core handler tries to load data always as
            pandas dataframes.

            Only for private usage! Stick to the _

        :param required_leaves: list
            A list of dictionaries. Each dictionary must contain 'name' and 'leaf_hash'
            to communicate with sta-core.
        :return: dictionary
            A dictionary with objects which are representing the data from the storage
            facility.
        """
        leaves_db = {}

        for i_leaf in required_leaves:
            i_leaf_name = i_leaf.get("name")
            i_leaf_hash = i_leaf.get("leaf_hash")

            df_i = self.dbh.read_leaf(directory=i_leaf_name,
                                      leaf_hash=i_leaf_hash,
                                      leaf_type="DataFrame")
            leaves_db[i_leaf_name] = df_i

        return leaves_db

    def _evaluate(self, existing_leaves, depending_leaves, leaf_name):
        """
        This function evaluates under certain logic what plugins/leaves are required
        to process the chosen plugin. A chosen plugin may require other requisites which
        must be processed first.

        .. note::
            Only for private usage! Stick to the _

        :param existing_leaves: list
            A list of existing leaves which are occurring because of the plugin collector
            which are collected.
        :param depending_leaves: list
            A list of module dependencies which come from the plugin dependency configuration
        :param leaf_name: str
            The leaf name of the plugin of choice to process.
        :return: bool or list
            If bool:
                - True: Good question...
                - False: Something went wrong, a plugin/leaf_name is missing. Processing can not happen.
            If list:
                - A list of missing plugins which we need to process first.
        """

        if leaf_name not in self.all_leaves:
            # The requested leaf name for processing does not exists
            # in the list all existing plugins which were fetched by
            # the plugin collector at the beginning.
            return False

        collect_missing_leaves = []
        for i_dep in depending_leaves:
            if i_dep not in existing_leaves:
                #print("K: ", self.leaf_name_to_plugin_name[i_dep])
                i_dep_plugin = ClassCollector[self.leaf_name_to_plugin_name[i_dep]]
                i_leaf_config = i_dep_plugin.get_plugin_config()
                i_dep_dependencies = i_leaf_config.get("plugin_dependencies")
                ret = self._evaluate(existing_leaves=existing_leaves,
                                     depending_leaves=i_dep_dependencies,
                                     leaf_name=i_dep)

                del i_dep_plugin

                if isinstance(ret, list):
                    collect_missing_leaves.extend(ret)

                collect_missing_leaves.append(i_dep)
        #print("Missing plugins", collect_missing_leaves)

        # check if all missing plugins existing in all registered plugins:
        check = all(item in self.all_leaves for item in collect_missing_leaves)
        if check is False:
            return False
        else:
            return collect_missing_leaves

    def process_branch(self, track_hash):
        """
        Holds the logic and process executive for processing a branch
        with given set (or all) plugins which apply here.
        :param track_hash:
        :return:
        """

        # If plugin pre-setting done is not correctly we archive it by
        # executing a function to set simply all registered plugins as pre-set.
        if self.plugins_to_process is None:
            self.set_processor_plugins()

        # Get information for existing branches for that the track hash:
        branch_existing_leaves = self.dbh.get_all_leaves_for_track(track_hash=track_hash)
        branch_existing_leaves_names = [i.get("name") for i in branch_existing_leaves.values()]

        # We run through the list of required plugins and decide if we process it or not and
        # if we need to process more plugins along the way.
        for i_plugin in self.plugins_to_process:
            print("2process: ", i_plugin)
            print("--------------------")

            # Get the right plugin for processing from the class collector:
            process_obj = ClassCollector[i_plugin]

            # fetch the specific plugin configuration:
            leaf_config = process_obj.get_plugin_config()
            plugin_dependencies = leaf_config.get("plugin_dependencies")
            leaf_name = leaf_config.get("leaf_name")

            ev_plugins = self._evaluate(existing_leaves=branch_existing_leaves_names,
                                        depending_leaves=plugin_dependencies,
                                        leaf_name=leaf_name)
            if ev_plugins is False:
                print("We skip here")
                continue

            if isinstance(ev_plugins, list):
                for sub_i_plugin in ev_plugins:
                    print("We need to process first", sub_i_plugin)
                    print(self.leaf_name_to_plugin_name[sub_i_plugin])
                    #self.i_process(process_obj, track_hash)
                    process_obj_helper = ClassCollector[self.leaf_name_to_plugin_name[sub_i_plugin]]
                    self.i_process(plugin_obj=process_obj_helper,
                                   track_hash=track_hash)
                    del process_obj_helper

            print("Let's process", i_plugin)
            # todo:
            # self.overwrite needs to be handled!
            process_status = self.i_process(process_obj, track_hash)

            del process_obj



    def i_process(self, plugin_obj, track_hash):
        """
        The i_process(...) function handles the full processing cycle once it is decided
        if processing should be executed. The full processing cycle contains the following
        steps:
        1) Register it to the database to avoid collision with other processing applications
        2) Run the "true" init function plugin_obj.init()
        3) Init the process instruction inside the plugin plugin_obj.process(...)
        4) Fetch the result from the plugin and write it back to the database for update

        .. note::
            This is not a strictly private function. You could use it even from outside.

        :param plugin_obj: object
            Initiated plugin from the plugin collector
        :param track_hash: str
            the track hash such it is used in the database by sta-core
        :return: bool
            Return the processing status of the underlying plugin
        """

        # Get the leaf configuration once again before starting:
        leaf_config = plugin_obj.get_plugin_config()
        leaf_name = leaf_config.get("leaf_name")
        plugin_dependencies = leaf_config.get("plugin_dependencies")

        # Make a cross-check with the database if requested plugin is already processed
        # or if another process is handling it right now.
        existing_branch = self.dbh.read_branch(key="track_hash", attribute=track_hash)[0]
        db_leaf_info = [i for i in existing_branch.get("leaf").values() if i.get("name") == leaf_name]
        if len(db_leaf_info) == 1:
            db_leaf_status = db_leaf_info[0].get("status")
        else:
            db_leaf_status = None

        if db_leaf_status == "processed" or db_leaf_status == "processing":
            print("nothing to process")
            return False

        # Use the information from the database about the plugin storage location:
        required_leaves = [i for i in existing_branch.get("leaf").values() if i.get("name") in plugin_dependencies]
        data_dict = self._read_leaf_data(required_leaves=required_leaves)

        # Create the leaf configuration at first and register it to the database
        obj_definition = ["None"]
        leaf_config_status = "processing"
        leaf_config_final = self.dbh.create_leaf_config(leaf_name=leaf_name,
                                                    track_hash=track_hash,
                                                    columns=obj_definition,
                                                    status=leaf_config_status)

        r = self.dbh.write_leaf(track_hash=track_hash,
                                leaf_config=leaf_config_final,
                                leaf=None,
                                leaf_type="ConfigWrite"
                                )

        # Let's do the processing:
        # todo: Try and catch would be nice...
        plugin_obj.init()
        plugin_obj.set_plugin_data(data_dict=data_dict)
        plugin_obj.run()

        # fetch processor status:
        process_status = plugin_obj.get_processing_success()
        process_result = plugin_obj.get_result()
        if process_status is True:
            leaf_config_status = "processed"
        else:
            leaf_config_status = "retry"

        # Get to the final leaf configuration:
        if process_result is not None and isinstance(process_result, pd.DataFrame):
            obj_definition = list(process_result.columns)
            leaf_type = "DataFrame"
            obj_df = process_result
        else:
            obj_definition = []
            leaf_type = "ConfigWrite"
            obj_df = None

        leaf_config_final = self.dbh.create_leaf_config(leaf_name=leaf_name,
                                                        track_hash=track_hash,
                                                        columns=obj_definition,
                                                        status=leaf_config_status)

        r = self.dbh.write_leaf(track_hash=track_hash,
                                leaf_config=leaf_config_final,
                                leaf=obj_df,
                                leaf_type=leaf_type
                                )

        return process_status
