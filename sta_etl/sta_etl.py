from sta_etl.plugin_handler.loader import PluginLoader
from sta_core import DataBaseHandler
import datetime

def list_plugins():
    pl = PluginLoader()
    all_available_plugins = pl.get_all_plugins()
    return all_available_plugins

def cli_proc(track_hash, db_info, plugins=None):
    print(track_hash)



    dbh = DataBaseHandler(db_type=db_info["db_type"])
    dbh.set_db_path(db_path=db_info["db_path"])
    dbh.set_db_name(db_name=db_info["db_name"])

    db_exists = dbh.get_database_exists()
    if db_exists is False:
        print(f"Database {db_info['db_name']} does not exists")
        exit()
    else:
        print(f"Database {db_info['db_name']} does exists")

    user_tracks = dbh.read_branch(key="user_hash", attribute=db_info["db_hash"])

    # Plugin Loader:
    pl = PluginLoader()
    pl.set_database_handler(dbh=dbh)

#    all_available_plugins = pl.get_all_plugins()
    # print(all_available_plugins)

    pl.set_processor_plugins(plugins=plugins)

    for i_track in user_tracks:
        i_track_hash = i_track.get("track_hash")

        if i_track_hash != track_hash:
            continue

        print("")
        print(i_track)
        print("")


        pl.process_branch(i_track_hash)

    exit()
    # for i_track in user_tracks:
    #     i_track_hash = i_track.get("track_hash")
    #
    #     if i_track_hash != track_hash:
    #         continue
    #
    #     print("")
    #     print(i_track)
    #     print("")
    #
    #     existing_leaves = dbh.get_all_leaves_for_track(track_hash=i_track_hash)
    #     if existing_leaves is None:
    #         #if existing_leaves is none, there are no leaves in this track
    #         continue
    #
    #     existing_leaf_names = list(existing_leaves.keys())
    #     print(existing_leaf_names)
    #
    #     pl.set_existing_leaves_for_track(existing_leaves=existing_leaves)
    #     #pl.set_track_by_hash(track_hash=i_track_hash)
    #     pl.allow_overwrite()
    #
    #     for i_plugin in all_available_plugins:




            # error codes:
            # 2: leaf exits but no overwriting is allowed
            # 1: leaf can not be processed due to missing processed leaves or raw data
            # 0: OK
            # -1: Something went wrong
            # error_code = pl.process(i_plugin, track_hash)


#
# db_type = "FileDataBase"
# db_path = "/home/koenig/testtestDB/"
# db_name = "testtestDB"
#
# dbh = DataBaseHandler(db_type=db_type)
# dbh.set_db_path(db_path=db_path)
# dbh.set_db_name(db_name=db_name)
#
# db_exists = dbh.get_database_exists()
# if db_exists is False:
#     print(f"Database {db_name} does not exists")
#     exit()
# else:
#     print(f"Database {db_name} does exists")
#
#
# user_name = "koenigStrava"
#
# user_entry = dbh.search_user(user=user_name, by="username")
#
# user_hash = user_entry[0].get("user_hash")
# print(user_name, user_hash)
#
# user_tracks = dbh.read_branch(key="user_hash", attribute=user_hash)
#
# overwrite = False
#
# pl = PluginLoader()
# pl.set_database_handler(dbh=dbh)
#
# all_available_plugins = pl.get_all_plugins()
#
# print(all_available_plugins)
#
# print(user_tracks)
# # exit()
#
# beg = datetime.datetime(2021,2,1, 0, 0, 0)
# end = datetime.datetime(2021,3,1, 0, 0, 0)
#
# for i_track in user_tracks:
#
#     i_track_beg = datetime.datetime.fromtimestamp(i_track.get("start_time") / 1e3)
#     i_track_end = datetime.datetime.fromtimestamp(i_track.get("end_time") / 1e3)
#
#     if i_track_beg < beg or i_track_end > end:
#         continue
#
#     i_track_hash = i_track.get("track_hash")
#
#     print("----------------------------------------")
#     print("Track:")
#     print(i_track)
#     print("----------------------------------------")
#
#
#     existing_leaves = dbh.get_all_leaves_for_track(track_hash=i_track_hash)
#     if existing_leaves is None:
#         #if existing_leaves is none, there are no leaves in this track
#         continue
#
#     existing_leaf_names = list(existing_leaves.keys())
#
#
#     print("Existing leaves:", existing_leaf_names, i_track_hash)
#
#     # continue
#
#     pl.set_existing_leaves_for_track(existing_leaves=existing_leaves)
#     pl.set_track_by_hash(track_hash=i_track_hash)
#     pl.allow_overwrite()
#
#     for i_plugin in all_available_plugins:
#         # error codes:
#         # 2: leaf exits but no overwriting is allowed
#         # 1: leaf can not be processed due to missing processed leaves or raw data
#         # 0: OK
#         # -1: Something went wrong
#         error_code = pl.process(i_plugin)
