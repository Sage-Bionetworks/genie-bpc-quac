
import pytz
from datetime import datetime

import synapseclient
import synapseutils
import pandas as pd
import numpy as np

syn = synapseclient.Synapse()
syn.login()


def is_synapse_table(synid):
    if synid is None:
        return False

    entity = syn.get(synid, downloadFile=False)
    file_type = entity.concreteType

    if file_type is None:
        return False

    if "table" in file_type:
        return True

    return False


def get_synid_file_name(synids):
    file_names = []
    # HACK: syn.get() does not accept a list of Synapse IDs
    if isinstance(synids, str):
        synids = [synids]
    for synid in synids:
        ent = syn.get(synid)
        file_names.append(ent.name)

    return file_names


def get_synid_from_table(synid, condition=None, with_names=False):
    query = f"SELECT id, name FROM {synid}"
    if condition is not None:
        query += f" WHERE {condition}"

    res = syn.tableQuery(query).asDataFrame()
    ids = res["id"].tolist()

    if with_names:
        names = get_synid_file_name(ids)
        return dict(zip(names, ids))

    return ids


def get_data(synid, version=None, sheet=1):
    """
    Retrieves data from a Synapse table or file based on the given Synapse ID, version, and sheet number.

    :param synid: The Synapse ID of the table or file.
    :param version: The version of the table (default is None).
    :param sheet: The sheet number for Excel files (default is 1).
    :return: The retrieved data as a DataFrame.
    """
    if is_synapse_table(synid):
        if version is None:
            query = f"SELECT * FROM {synid}"
        else:
            query = f"SELECT * FROM {synid}.{version}"

        results = syn.tableQuery(query)
        data = results.asDataFrame()
    else:
        ent = syn.get(synid, version=version)
        print(ent.name)
        if ent.name.endswith(".xlsx"):
            data = pd.read_excel(ent.path, sheet_name=sheet)
        else:
            data = pd.read_csv(ent.path, low_memory=False)

    return data


def get_data_filtered_table(
    synid, column_name, col_value, select=None, version=None, exact=None
):
    where_clause = ""
    for i in range(len(column_name)):
        if i == 0:
            where_clause = "WHERE"
        else:
            where_clause += " AND"

        if exact[i]:
            where_clause += f" {column_name[i]} = '{col_value[i]}'"
        else:
            where_clause += f" {column_name[i]} LIKE '%{col_value[i]}%'"

    select_clause = "*"
    if select is not None:
        select_clause = ", ".join(select)

    if version is None:
        query = f"SELECT {select_clause} FROM {synid} {where_clause}"
    else:
        if isinstance(version, list):
            version = version[0]
        query = f"SELECT {select_clause} FROM {synid}.{version} {where_clause}"

    results = syn.tableQuery(query)
    data = results.asDataFrame()

    return data


def get_data_filtered_file(
    synid, column_name, col_value, select=None, version=None, exact=None
):
    ent = syn.get(synid, version=version)
    data = pd.read_csv(ent.path, na_values=[""])

    idx_filter = []
    for i in range(len(column_name)):
        if exact[i]:
            idx_new = data[data[column_name[i]] == col_value[i]].index.tolist()
        else:
            idx_new = data[
                data[column_name[i]].str.contains(col_value[i], regex=False)
            ].index.tolist()

        idx_filter += idx_new

    idx_filter = list(set(idx_filter))

    if len(idx_filter) > 0:
        data = data.loc[idx_filter]
    else:
        data = None

    if select is not None:
        data = data[select]

    return data


def get_data_filtered(
    synid, column_name, col_value, select=None, version=None, exact=None
):
    if is_synapse_table(synid):
        data = get_data_filtered_table(
            synid, column_name, col_value, select, version, exact
        )
    else:
        data = get_data_filtered_file(
            synid, column_name, col_value, select, version, exact
        )

    return data


def get_entities_from_table(synapse_id, condition=None):
    if condition is None:
        query = f"SELECT id FROM {synapse_id}"
    else:
        query = f"SELECT id FROM {synapse_id} WHERE {condition}"

    results = syn.tableQuery(query)
    res = results.asDataFrame()

    return res["id"].tolist()


def get_synapse_folder_children(
    synapse_id,
    include_types=["folder", "file", "table", "link", "entityview", "dockerrepo"],
):
    children = syn.getChildren(synapse_id, includeTypes=include_types)
    children_dict = {child["name"]: child["id"] for child in children}

    return children_dict

from functools import cache

# TODO clean up
@cache
def get_folder_synid_from_path(synid_folder_root: str, paths: list):
    # synid_folder_current = synid_folder_root
    # subfolders = path.split("/")

    # for subfolder in subfolders:
    #     synid_folder_children = get_synapse_folder_children(
    #         synid_folder_current, include_types=["folder"]
    #     )

    #     if subfolder not in synid_folder_children:
    #         return None

    #     synid_folder_current = synid_folder_children[subfolder]

    # return synid_folder_current
    folder_hiearchy = synapseutils.walk(syn, synid_folder_root)
    for dirpath, _, _ in folder_hiearchy:
        if dirpath[0].endswith(paths):
            return dirpath[1]
    return None

# TODO Clean up
@cache
def get_file_synid_from_path(synid_folder_root: str, paths: list, file_name: str):
    # path_parts = path.split("/")
    # file_name = path_parts[-1]
    # path_abbrev = "/".join(path_parts[:-1])

    # synid_folder_dest = get_folder_synid_from_path(synid_folder_root, path_abbrev)
    # synid_folder_children = get_synapse_folder_children(
    #     synid_folder_dest, include_types=["file"]
    # )

    # if file_name not in synid_folder_children:
    #     return None
    print(file_name)
    print(synid_folder_root)
    folder_hiearchy = synapseutils.walk(syn, synid_folder_root)
    for dirpath, dirname, files in folder_hiearchy:
        # print(dirpath)
        if dirpath[0].endswith(paths):
            for filename, synid in files:
                if filename == file_name:
                    synid_file = synid
                    return synid_file
    return None


# helper functions
def get_added(current, previous):
    return list(set(current) - set(previous))


def get_removed(current, previous):
    return list(set(previous) - set(current))


def get_vector_index(array_index, nrow):
    if len(array_index) != 2:
        return None

    row_index, col_index = array_index

    if row_index > nrow:
        return None

    vector_index = (col_index - 1) * nrow + row_index
    return vector_index


def count_not_empty(vector, exclude=np.nan, na_strings=np.nan):
    mod = vector.copy()

    if not pd.isna(exclude):
        mod = mod[~mod.isin([exclude])]

    idx_na = mod.isna() | mod.isin([na_strings])
    return len(mod) - sum(idx_na)


def is_empty(vector, exclude=np.nan, na_strings=np.nan):
    return count_not_empty(vector, exclude, na_strings) == 0


def is_not_empty(vector, exclude=np.nan, na_strings=np.nan):
    return bool(count_not_empty(vector, exclude, na_strings))


def fraction_empty(vector, na_strings=np.nan):
    return sum(vector.isin([na_strings])) / len(vector)


def is_double(x):
    try:
        float(x)
        return True
    except ValueError:
        return False


def infer_data_type(x):
    if is_double(x):
        return "numeric"
    return "character"


def get_columns_added(data_current, data_previous):
    return list(set(data_current.columns) - set(data_previous.columns))


def get_columns_removed(data_current, data_previous):
    return list(set(data_previous.columns) - set(data_current.columns))


def get_column_elements_added(data_current, data_previous, column_name):
    return list(set(data_current[column_name]) - set(data_previous[column_name]))


def get_column_elements_removed(data_current, data_previous, column_name):
    return list(set(data_previous[column_name]) - set(data_current[column_name]))


def get_date_as_string(
    ts, ts_format="%Y-%m-%dT%H:%M:%S", tz_current=None, tz_target="US/Pacific"
):
    if tz_current is None:
        tz_current = datetime.now().astimezone().tzinfo

    ts_posix = datetime.strptime(ts, ts_format).replace(
        tzinfo=pytz.timezone(tz_current)
    )
    ts_mod = ts_posix.astimezone(pytz.timezone(tz_target)).strftime("%Y-%m-%d")

    return ts_mod


def is_timestamp_format_correct(timestamps, formats=["%Y-%m-%d %H:%M:%S"]):
    for ts in timestamps:
        for fmt in formats:
            try:
                datetime.strptime(ts, fmt)
                return True
            except ValueError:
                pass
    return False


def is_date_format_correct(dates, formats=["%Y-%m-%d"]):
    return is_timestamp_format_correct(dates, formats)


def get_auth_token(path):
    with open(path, "r") as file:
        lines = file.readlines()

    for line in lines:
        if line.startswith("authtoken = "):
            token = line.split(" ")[2].strip()
            return token

    return None


def synLogin(auth=None, silent=True):
    secret = os.getenv("SCHEDULED_JOB_SECRETS")

    if secret:
        syn = synapseclient.login(
            silent=silent, authToken=json.loads(secret)["SYNAPSE_AUTH_TOKEN"]
        )
    elif auth is None or auth == "~/.synapseConfig":
        syn = synapseclient.login(silent=silent)
    else:
        token = auth

        if auth.endswith(".synapseConfig"):
            token = get_auth_token(auth)

            if token is None:
                return False

        try:
            syn = synapseclient.login(authToken=token, silent=silent)
        except Exception:
            return False

    return True


def save_to_synapse(
    path,
    parent_id,
    file_name=None,
    prov_name=None,
    prov_desc=None,
    prov_used=None,
    prov_exec=None,
):
    if file_name is None:
        file_name = path

    file = synapseclient.File(path=path, parentId=parent_id, name=file_name)

    if (
        prov_name is not None
        or prov_desc is not None
        or prov_used is not None
        or prov_exec is not None
    ):
        act = synapseclient.Activity(
            name=prov_name, description=prov_desc, used=prov_used, executed=prov_exec
        )
        file = syn.store(file, activity=act)
    else:
        file = syn.store(file)

    return file.id


def is_synapse_entity_csv(synapse_id):
    try:
        data = syn.get(synapse_id, downloadFile=False)
        return data.concreteType == "org.sagebionetworks.repo.model.FileEntity"
    except Exception:
        return False


def now(time_only=False, tz="US/Pacific"):
    current_time = datetime.datetime.now(pytz.timezone(tz))

    if time_only:
        return current_time.strftime("%H:%M:%S")

    return current_time.strftime("%Y-%m-%d %H:%M:%S")


def wait_if_not(cond, msg=""):
    if not cond:
        print(msg)
        print("Press control-C to exit and try again.")

        while True:
            pass


def capitalize(str):
    return str[0].upper() + str[1:].lower()


def trim_string(str):
    return str.strip()


def merge_last_elements(x, delim):
    return [x[0], delim.join(x[1:])]


def strsplit_first(x, split):
    unmerged = x.split(split)
    remerge = merge_last_elements(unmerged, split)

    return remerge


def parse_mapping(str):
    clean = trim_string(str.replace('"', ""))
    splt = strsplit_first(clean.split("|")[0], ",")

    codes = [trim_string(x[0]) for x in splt]
    values = [trim_string(x[1]) for x in splt]
    mapping = pd.DataFrame({"codes": codes, "values": values})

    return mapping


def parse_phase_from_cohort(cohort):
    if cohort.endswith("2"):
        return [cohort[:-1], 2]
    elif cohort in ["RENAL", "MELANOMA", "OVARIAN", "ESOPHAGO"]:
        return [cohort, 2]

    return [cohort, 1]
