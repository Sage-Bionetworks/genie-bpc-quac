import datetime
import os

import numpy as np
import pandas as pd
import synapseclient
import yaml

from utils import (
    get_synid_file_name,
    get_data_filtered,
    get_data,
    get_file_synid_from_path,
    get_synid_from_table,
    get_synapse_folder_children,
    get_columns_removed,
    get_folder_synid_from_path,
    get_columns_added,
    is_empty,
    infer_data_type,
    get_added,
    is_timestamp_format_correct,
    is_date_format_correct,
    parse_phase_from_cohort,
    fraction_empty,
    parse_mapping,
    is_synapse_entity_csv,
)

syn = synapseclient.login()
syn.login()

# global variables ------------------------------------
workdir = "."
if not os.path.exists("config.yaml"):
    workdir = "/usr/local/src/myscripts"
with open(os.path.join(workdir, "config.yaml"), "r") as stream:
    TOP_CONFIG = yaml.safe_load(stream)

# list of functions ------------------------------------
def get_check_functions(labels):
    """
    Gather all functions corresponding to numbered checks.

    Parameters:
    labels: Labels corresponding to checks

    Returns:
    List containing implemented check functions in addition
    to associated metadata.
    """
    fxns = {}
    for label in labels:
        fxns[label] = globals()[label]
    return fxns


def update_config_for_comparison_report(config):
    """
    Update the configuration global variables with information
    from external references for the comparison report.

    Parameters:
    config: Raw configuration object loaded from config.yaml

    Returns:
    Configuration object augmented by information for external references
    """
    # update for comparison reports
    query = f"SELECT * FROM {config['synapse']['version_date']['id']}"
    comp = pd.DataFrame(syn.tableQuery(query).asDataFrame())

    config["comparison"] = {}
    for i in range(len(comp)):
        config["comparison"][comp.iloc[i]["cohort"]] = {
            "previous": comp.iloc[i]["previous_date"],
            "current": comp.iloc[i]["current_date"],
        }

    return config


def update_config_for_release_report(config):
    """
    Update the configuration global variables with information
    from external references for the release report.

    Parameters:
    config: Raw configuration object loaded from config.yaml

    Returns:
    Configuration object augmented by information for external references
    """
    # update for release comparisons
    query = f"SELECT * FROM {config['synapse']['version_release']['id']} WHERE current = 'true'"
    rel = pd.DataFrame(syn.tableQuery(query).asDataFrame())

    config["release"] = {}
    for i in range(len(rel)):
        previous = None
        if pd.notna(rel.iloc[i]["previous_version"]):
            previous = (
                f"{rel.iloc[i]['previous_version']}-{rel.iloc[i]['previous_type']}"
            )
        current = f"{rel.iloc[i]['release_version']}-{rel.iloc[i]['release_type']}"
        config["release"][rel.iloc[i]["cohort"]] = {
            "previous": previous,
            "current": current,
        }

    return config


def format_output(
    value,
    output_format="log",
    cohort=None,
    site=None,
    synid=None,
    patient_id=None,
    instrument=None,
    instance=None,
    column_name=None,
    check_no=None,
    infer_site=False,
):
    """
    Format output of check functions according to requested format.

    Parameters:
    value: vector of problematic values
    output_format: format in which to return problematic values
    cohort: cohort over which the check is performed
    site: site over which the check is performed
    synid: synapse ID of the file over which the check is perfomed
    column_name: name of the column from which the problematic values
    were drawn
    check_no: check reference number

    Returns:
    formatted output of problematic values:
              log: one row per value formatted as cohort,site, synid,
              synid_name, column_name, value,
                    check_no, description, action
    """
    if len(value) == 0:
        return None

    res = []

    if output_format == "log":
        # get main input file data
        if synid is None:
            print("Warning: no file Synapse ID specified.  Setting file name to NA. \n")
            synid_name = None
        else:
            synid_name = ";".join(get_synid_file_name(synid))

        # get check metadata info
        if check_no is None:
            print(
                "Warning: no check number specified for logging.  Returning NAs \
                  for description and action. \n"
            )
            ref = {"description": None, "action": None}

        else:
            ref = {
                "description": TOP_CONFIG["checks"][check_no]["description"],
                "action": TOP_CONFIG["checks"][check_no]["action"],
            }

        # setup output row for error logging
        labels = [
            "cohort",
            "site",
            "synapse_id",
            "synapse_name",
            "patient_id",
            "instrument",
            "instance",
            "column_name",
            "value",
            "check_no",
            "description",
            "request",
        ]

        # extract site from value, when appropriate
        if site is None and infer_site:
            site = [v.split("-")[1] for v in value]

        if len(value) > 1:
            res = pd.DataFrame(
                list(
                    zip(
                        [cohort] * len(value),
                        [site] * len(value),
                        [synid] * len(value),
                        [synid_name] * len(value),
                        [patient_id] * len(value),
                        [instrument] * len(value),
                        [instance] * len(value),
                        [column_name] * len(value),
                        value,
                        [check_no] * len(value),
                        [ref["description"]] * len(value),
                        [ref["action"]] * len(value),
                    )
                ),
                columns=labels,
            )
        else:
            res = pd.DataFrame(
                list(
                    zip(
                        [cohort],
                        [site],
                        [synid],
                        [synid_name],
                        [patient_id],
                        [instrument],
                        [instance],
                        [column_name],
                        value,
                        [check_no],
                        [ref["description"]],
                        [ref["action"]],
                    )
                ),
                columns=labels,
            )

    else:
        # if output format not recognized, just return the
        res = value

    return res


# helper functions ----------------------------------


def get_row_key(
    idx,
    data,
    column_names=[
        TOP_CONFIG["column_name"]["patient_id"],
        TOP_CONFIG["column_name"]["instrument"],
        TOP_CONFIG["column_name"]["instance"],
    ],
):
    if len(idx):
        value = data.loc[idx, column_names].apply(lambda x: "|".join(x), axis=1)
        column_name = "|".join(column_names)
    else:
        value = []
        column_name = []
    return {"value": value, "column_name": column_name}


# main genie functions ------------------------------


def get_main_genie_ids(synid_table_sample, patient=True, sample=True):
    if patient and sample:
        query = f"SELECT PATIENT_ID, SAMPLE_ID FROM {synid_table_sample}"
    elif patient:
        query = f"SELECT DISTINCT PATIENT_ID FROM {synid_table_sample}"
    elif sample:
        query = f"SELECT SAMPLE_ID FROM {synid_table_sample}"
    else:
        return None
    res = pd.DataFrame(syn.tableQuery(query).asDataFrame())
    return res


# bpc functions -------------------------------------


def get_bpc_synid_prissmm(
    synid_table_prissmm,
    cohort,
    file_name=["Import Template", "Data Dictionary non-PHI"],
):
    query = f"SELECT id FROM {synid_table_prissmm} WHERE cohort = '{cohort}' ORDER BY name DESC LIMIT 1"
    synid_folder_prissmm = (
        syn.tableQuery(query).asDataFrame().values[0][0]
    )
    synid_prissmm_children = list(
        syn.getChildren(synid_folder_prissmm)
    )
    synid_prissmm_children_ids = {
        child["name"]: child["id"] for child in synid_prissmm_children
    }
    return synid_prissmm_children_ids[file_name]


def get_bpc_data_upload(
    cohort,
    site,
    report,
    synid_file_data1,
    synid_file_data2=None,
    synid_file_header1=None,
    synid_file_header2=None,
):
    data = []
    data1 = []
    data2 = []

    # get data 1 (default, should always be specified)
    ent = syn.get(synid_file_data1)
    data1 = pd.read_csv(ent.path, na_values="", dtype=str)

    # check for header1
    if synid_file_header1:
        ent = syn.get(synid_file_header1)
        data1.columns = pd.read_csv(ent.path, na_values="", dtype=str).values[0]

    # check for data 2
    if synid_file_data2:
        ent = syn.get(synid_file_data2)
        data2 = pd.read_csv(ent.path, na_values="", dtype=str)

    # check for header2
    if synid_file_header2:
        ent = syn.get(synid_file_header2)
        data2.columns = pd.read_csv(ent.path, na_values="", dtype=str).values[0]

    if synid_file_data2:
        data = pd.merge(
            data1,
            data2,
            on=["record_id", "redcap_repeat_instrument", "redcap_repeat_instance"],
        )
    else:
        data = data1

    return data


def get_bpc_version_at_date(
    synid_table,
    date_version,
    ts_format="%Y-%m-%dT%H:%M:%OS",
    tz_current="UTC",
    tz_target="US/Pacific",
):
    rest_cmd = f"/entity/{synid_table}/version?limit=50"
    version_history_list = syn.restGET(rest_cmd)
    version_history = pd.DataFrame(version_history_list["results"])
    version_history["date_cut"] = version_history["modifiedOn"].apply(
        lambda x: datetime.datetime.strptime(x, ts_format)
        .astimezone(datetime.timezone(tz_target))
        .strftime("%Y-%m-%d")
    )
    idx = version_history[version_history["date_cut"] == date_version].index
    if len(idx) == 0:
        return None
    version_no = version_history.loc[idx, "versionNumber"].values[0]
    return version_no


# bpc functions -------------------------------------


def get_bpc_data_table(config, synid_table, cohort, site=np.nan, select=np.nan, previous=False):
    version_no = np.nan

    if previous:
        version_date = config["comparison"][cohort]["previous"]
        version_no = get_bpc_version_at_date(
            synid_table=synid_table, date_version=version_date
        )

    if pd.isna(site):
        data = get_data_filtered(
            synid_table,
            column_name="cohort",
            col_value=cohort,
            select=select,
            version=version_no,
            exact=True,
        )
    else:
        data = get_data_filtered(
            synid_table,
            column_name=["cohort", "record_id"],
            col_value=[cohort, f"-{site}-"],
            select=select,
            version=version_no,
            exact=[True, False],
        )

    return data


def get_bpc_data_release(cohort, site, version, file_name, current=False):
    old_accepted_path = f"{cohort}/{version}/{cohort}_{version}_clinical_data"
    new_path = f"{cohort}/{version}/clinical_data"
    if not current:
        synid_file = get_file_synid_from_path(
            synid_folder_root=TOP_CONFIG["synapse"]["release"]["id"],
            paths=(old_accepted_path,new_path),
            file_name=file_name
        )
    else:
        synid_file = get_file_synid_from_path(
            synid_folder_root="syn50876969",
            paths=(old_accepted_path,new_path),
            file_name=file_name
        )
    # import synapseutils
    # print(TOP_CONFIG["synapse"]["release"]["id"])
    # folder_hiearchy = synapseutils.walk(syn, TOP_CONFIG["synapse"]["release"]["id"])
    # for dirpath, dirname, files in folder_hiearchy:
    #     print(dirpath)
    #     if dirpath[0].endswith((old_accepted_path, new_path)):
    #         for filename, synid in files:
    #             if filename == file_name:
    #                 synid_file = synid
    #                 break
    # print(synid_file)
    data = get_data(synid_file)
    # print(data)
    if not pd.isna(site):
        data = data[data["record_id"].str.contains(site)]

    return data


def get_bpc_data(config, cohort, site, report, obj=None, current=False):
    if report in ["upload", "masking"]:
        return get_bpc_data_upload(
            cohort,
            site,
            synid_file_data1=obj["data1"],
            synid_file_data2=obj["data2"],
            synid_file_header1=obj["header1"],
            synid_file_header2=obj["header2"],
        )

    if report in ["table", "comparison"]:
        return get_bpc_data_table(
            config=config,
            cohort=cohort,
            site=site,
            synid_table=obj["synid_table"],
            select=obj["select"],
            previous=obj["previous"],
        )

    if report in ["release"]:
        return get_bpc_data_release(
            cohort=cohort, site=site, version=obj["version"], file_name=obj["file_name"], current=current
        )

    return None


def get_bpc_index_missing_sample(
    data, instrument=TOP_CONFIG["instrument_name"]["panel"], na_strings=["NA", ""]
):
    idx = data[
        (data[TOP_CONFIG["column_name"]["instrument"]] == instrument)
        & (
            data[TOP_CONFIG["column_name"]["sample_id"]].isna()
            | data[TOP_CONFIG["column_name"]["sample_id"]].isin(na_strings)
        )
    ].index
    return idx


def get_bpc_patient_sample_added_removed(
    config, cohort, site, report, check_patient, check_added, account_for_retracted=False
):
    column_name = ""
    table_name = ""
    synid_entity_source = None
    results = {}
    retracted = []
    if check_patient:
        column_name = config["column_name"]["patient_id"]
        table_name = config["table_name"]["patient_id"]
        file_name = config["file_name"]["patient_file"]

        if account_for_retracted:
            retracted = get_retracted_patients(cohort)
    else:
        column_name = config["column_name"]["sample_id"]
        table_name = config["table_name"]["sample_id"]
        file_name = config["file_name"]["sample_file"]

        if account_for_retracted:
            retracted = get_retracted_samples(cohort)

    if report == "upload":
        synid_tables = get_synid_from_table(
            config["synapse"]["tables_view"]["id"],
            condition="double_curated = false",
            with_names=True,
        )
        synid_table = str(synid_tables[table_name])
        data_previous = get_bpc_data(
            config=config,
            cohort=cohort,
            site=site,
            report="table",
            obj={"synid_table": synid_table, "previous": False, "select": None},
        )

        obj_upload = config["uploads"][cohort][site]
        synid_entity_source = obj_upload["data1"]
        data_current_irr = get_bpc_data(
            config=config, cohort=cohort, site=site, report=report, obj=obj_upload,
        )
        data_current = data_current_irr[
            ~data_current_irr[config["column_name"]["patient_id"]].str.contains(
                "[_-]2$", regex=True
            )
        ]
    elif report in ["table", "comparison"]:
        synid_tables = get_synid_from_table(
            config["synapse"]["tables_view"]["id"],
            condition="double_curated = false",
            with_names=True,
        )
        synid_entity_source = str(synid_tables[table_name])

        data_current = get_bpc_data(
            config=config,
            cohort=cohort,
            site=site,
            report=report,
            obj={"synid_table": synid_entity_source, "previous": False, "select": None}
        )
        data_previous = get_bpc_data(
            cohort=cohort,
            site=site,
            report=report,
            obj={"synid_table": synid_entity_source, "previous": True, "select": None},
        )
    elif report == "release":
        version_current = config["release"][cohort]["current"]
        version_previous = config["release"][cohort]["previous"]

        path = f"{cohort}/{version_current}/{cohort}_{version_current}_clinical_data"
        new_path = f"{cohort}/{version_current}/clinical_data"
        synid_entity_source = get_file_synid_from_path(
            synid_folder_root=config["synapse"]["release"]["id"],
            paths=(path, new_path),
            file_name=file_name
        )

        if version_previous is None:
            return None

        data_current = get_bpc_data(
            config=config,
            cohort=cohort,
            site=site,
            report=report,
            obj={"version": version_current, "file_name": file_name},
            current=True
        )
        data_previous = get_bpc_data(
            config=config,
            cohort=cohort,
            site=site,
            report=report,
            obj={"version": version_previous, "file_name": file_name},
        )
    else:
        return None

    if check_added:
        results["ids"] = list(
            set(data_current[column_name]) - set(data_previous[column_name]) - {None}
        )
    else:
        results["ids"] = list(
            set(data_previous[column_name])
            - set(retracted)
            - set(data_current[column_name])
            - {None}
        )

    results["column_name"] = column_name
    results["synid_entity_source"] = synid_entity_source
    return results


def get_bpc_instrument_of_variable(variable_name, cohort):
    synid_dd = get_bpc_synid_prissmm(
        synid_table_prissmm=TOP_CONFIG["synapse"]["prissmm"]["id"],
        cohort=cohort,
        file_name="Data Dictionary non-PHI",
    )
    dd = get_data(synid_dd)
    instrument = dd["Form Name"][dd["Variable / Field Name"] == variable_name]
    return instrument


def get_bpc_sor_data_type_single(var_name, sor=None):
    if sor is None:
        sor = get_data(TOP_CONFIG["synapse"]["sor"]["id"], sheet=2)
    if len(sor[sor["VARNAME"] == var_name]) == 0:
        return np.nan
    data_type = sor[sor["VARNAME"] == var_name]["DATA.TYPE"].unique().lower()
    map_dt = TOP_CONFIG["maps"]["data_type"]
    if data_type in map_dt:
        return map_dt[data_type]
    return data_type


def get_bpc_sor_data_type(var_name, sor=None):
    data_types = []
    for i in range(len(var_name)):
        data_types.append(get_bpc_sor_data_type_single(var_name[i], sor))
    return data_types


def get_bpc_table_synapse_ids():
    query = f"SELECT id, name FROM {TOP_CONFIG['synapse']['tables_view']['id']} WHERE double_curated = 'false'"
    table_info = syn.tableQuery(query).asDataFrame()
    return dict(zip(table_info["name"], table_info["id"]))


def get_bpc_table_instrument(synapse_id):
    query = f"SELECT form FROM {TOP_CONFIG['synapse']['tables_view']['id']} WHERE double_curated = 'false' AND id = '{synapse_id}'"
    form = syn.tableQuery(query).asDataFrame()
    return form["form"].values[0]


def get_bpc_set_view(config, cohort, report, version=np.nan):
    view = None
    if report in ["table", "comparison"]:
        query = f"SELECT id, primary_key, form FROM {config['synapse']['tables_view']['id']} WHERE double_curated = 'false'"
        view = syn.tableQuery(query).asDataFrame()
    elif report == "release":
        if pd.isna(version):
            version = config["release"][cohort]["current"]
        # path = f"{cohort}/{version}/{cohort}_{version}_clinical_data"
        old_accepted_path = f"{cohort}/{version}/{cohort}_{version}_clinical_data"
        new_path = f"{cohort}/{version}/clinical_data"

        synid_folder = get_folder_synid_from_path(
            synid_folder_root=config["synapse"]["release"]["id"],
            paths=(old_accepted_path, new_path)
        )
        # TODO fix this issue between newer releases being in staging
        # TODO and older releases being in a separate folder
        if synid_folder is None:
            synid_folder = get_folder_synid_from_path(
                synid_folder_root="syn50876969",
                paths=(old_accepted_path, new_path)
            )
        if synid_folder is None:
            raise ValueError("fix script")
        raw = get_synapse_folder_children(
            synapse_id=synid_folder, include_types=["file"]
        )
        view = pd.DataFrame(
            {
                "id": raw.values(),
                "primary_key": "cohort, record_id, redcap_repeat_instance",
                "form": list(raw.keys()),
            }
        )
    return view


def get_bpc_pair(config, cohort, site, report, synid_entity_source):
    data = {}
    if report in ["table", "comparison"]:
        data["current"] = get_bpc_data(
            config=config,
            cohort=cohort,
            site=site,
            report=report,
            obj={
                "synid_table": synid_entity_source,
                "select": np.nan,
                "previous": False,
            },
            current=True
        )
        data["previous"] = get_bpc_data(
            config=config,
            cohort=cohort,
            site=site,
            report=report,
            obj={
                "synid_table": synid_entity_source,
                "select": np.nan,
                "previous": True,
            },
        )
    elif report == "release":
        version_current = config["release"][cohort]["current"]
        version_previous = config["release"][cohort]["previous"]
        file_name = syn.get(synid_entity_source, downloadFile=False).name
        data["current"] = get_bpc_data(
            config=config,
            cohort=cohort,
            site=site,
            report=report,
            obj={"version": version_current, "file_name": file_name},
            current=True
        )
        data["previous"] = get_bpc_data(
            config=config,
            cohort=cohort,
            site=site,
            report=report,
            obj={"version": version_previous, "file_name": file_name},
        )
    return data


def get_bpc_curation_year(cohort, site):
    synid_table_curation = TOP_CONFIG["synapse"]["curation"]["id"]
    query = f"SELECT MAX(curation_dt) FROM {synid_table_curation} WHERE cohort = '{cohort}' AND redcap_data_access_group = '{site}'"
    dt = syn.tableQuery(query).asDataFrame().iloc[0, 0]
    return dt[:4]


def get_bpc_case_count(data):
    patient_id = data[TOP_CONFIG["column_name"]["patient_id"]]
    n_total = len(patient_id.unique())
    n_irr = len(patient_id[patient_id.str.contains("[-_]2$")].unique())
    n_current = n_total - n_irr
    return n_current


def get_retracted_patients(cohort):
    query = f"SELECT record_id FROM {TOP_CONFIG['synapse']['rm_pat']['id']} WHERE {cohort} = 'true'"
    retracted = syn.tableQuery(query).asDataFrame()["record_id"].tolist()
    return retracted if retracted else None


def get_retracted_samples(cohort):
    query = f"SELECT SAMPLE_ID FROM {TOP_CONFIG['synapse']['rm_sam']['id']} WHERE {cohort} = 'true'"
    retracted = syn.tableQuery(query).asDataFrame()["SAMPLE_ID"].tolist()
    return retracted if retracted else None


def get_hemonc_from_ncit(code_ncit):
    synid_table_map = TOP_CONFIG["synapse"]["map"]["id"]
    query = f"SELECT HemOnc_code FROM {synid_table_map} WHERE NCIT = 'C{code_ncit}'"
    code_hemonc = syn.tableQuery(query).asDataFrame()["HemOnc_code"].tolist()
    return int(code_hemonc[0]) if code_hemonc else None


def get_bpc_from_ncit(codes_ncit):
    names_bpc = []
    synid_table_map = TOP_CONFIG["synapse"]["map"]["id"]
    query = f"SELECT BPC FROM {synid_table_map} WHERE NCIT = 'C{code_ncit}'"
    for code_ncit in codes_ncit:
        name_bpc = syn.tableQuery(query).asDataFrame()["BPC"].tolist()
        names_bpc.append(name_bpc[0] if name_bpc else None)
    return names_bpc


def get_hemonc_fda_approval_year(code_hemonc):
    if pd.isna(code_hemonc):
        return None
    year = 0
    synid_table_rel = TOP_CONFIG["synapse"]["relationship"]["id"]
    synid_table_con = TOP_CONFIG["synapse"]["concept"]["id"]
    query = f"SELECT concept_code_2 FROM {synid_table_rel} WHERE vocabulary_id_1 = 'HemOnc' AND relationship_id = 'Was FDA approved yr' AND concept_code_1 = {code_hemonc}"
    concept_code_2 = syn.tableQuery(query).asDataFrame()["concept_code_2"].tolist()
    if not concept_code_2:
        return None
    for concept_code in concept_code_2:
        query = f"SELECT concept_name FROM {synid_table_con} WHERE concept_code = {concept_code}"
        year_code = syn.tableQuery(query).asDataFrame()["concept_name"].tolist()
        year = max(year, int(year_code[0]))
    return year if year else None


def col_import_template_added(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    synid_template = get_bpc_synid_prissmm(
        config["synapse"]["prissmm"]["id"], cohort, file_name="Import Template"
    )

    data_template = get_data(synid_template)
    data_upload = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    results = get_columns_added(data_current=data_upload, data_previous=data_template)
    output = format_output(
        value=results,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=results,
        synid=obj_upload["data1"],
        check_no=1,
    )

    return output


def col_import_template_removed(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    synid_template = get_bpc_synid_prissmm(
        config["synapse"]["prissmm"]["id"], cohort, file_name="Import Template"
    )

    data_template = get_data(synid_template)
    data_upload = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    results = get_columns_removed(data_current=data_upload, data_previous=data_template)
    output = format_output(
        value=results,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=results,
        synid=obj_upload["data1"],
        check_no=2,
    )

    return output


def patient_added(config, cohort, site, report, output_format="log"):
    results = get_bpc_patient_sample_added_removed(
        config=config, cohort=cohort, site=site, report=report, check_patient=True, check_added=True
    )
    output = format_output(
        value=results["ids"],
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=results["column_name"],
        synid=results["synid_entity_source"],
        check_no=3,
        infer_site=True,
    )

    return output


def patient_removed(config, cohort, site, report, output_format="log"):
    results = get_bpc_patient_sample_added_removed(
        config=config, cohort=cohort, site=site, report=report, check_patient=True, check_added=False
    )
    output = format_output(
        value=results["ids"],
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=results["column_name"],
        synid=results["synid_entity_source"],
        check_no=4,
        infer_site=True,
    )

    return output


def sample_added(config, cohort, site, report, output_format="log"):
    results = get_bpc_patient_sample_added_removed(
        config=config, cohort=cohort, site=site, report=report, check_patient=False, check_added=True
    )
    output = format_output(
        results["ids"],
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=results["column_name"],
        synid=results["synid_entity_source"],
        check_no=5,
        infer_site=True,
    )
    return output


def sample_removed(config, cohort, site, report, output_format="log"):
    results = get_bpc_patient_sample_added_removed(
        config=config, cohort=cohort, site=site, report=report, check_patient=False, check_added=False
    )
    output = format_output(
        results["ids"],
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=results["column_name"],
        synid=results["synid_entity_source"],
        check_no=6,
        infer_site=True,
    )
    return output


def empty_row(config, cohort, site, report, output_format="log"):
    objs = []
    output = []

    if report == "upload":
        objs.append(config["uploads"][cohort][site])
    elif report == "table":
        synid_table_all = get_bpc_table_synapse_ids()
        for i in range(len(synid_table_all)):
            objs.append(
                {
                    "synid_table": str(synid_table_all[i]),
                    "previous": False,
                    "select": None,
                }
            )
    else:
        return None

    for obj in objs:
        data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj)

        complete_cols = [col for col in data.columns if "complete" in col]
        redcap_cols = [
            config["column_name"]["patient_id"],
            config["column_name"]["instrument"],
            config["column_name"]["instance"],
        ]
        exclude = complete_cols + redcap_cols

        res = data.apply(
            lambda row: is_empty(row, na_strings=["NA", ""], exclude=exclude), axis=1
        )
        idx = res[res].index

        output.append(
            format_output(
                value=[None] * len(idx),
                cohort=cohort,
                site=site,
                output_format=output_format,
                column_name=None,
                synid=obj["data1"] if obj["data1"] is not None else obj["synid_table"],
                patient_id=data.loc[idx, config["column_name"]["patient_id"]],
                instrument=get_bpc_table_instrument(obj["synid_table"])
                if obj["data1"] is None
                else data["redcap_repeat_instrument"][idx],
                instance=data.loc[idx, config["column_name"]["instance"]],
                check_no=7,
                infer_site=False,
            )
        )

    return output


def missing_sample_id(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    idx = get_bpc_index_missing_sample(data=data)

    output = format_output(
        value=data.loc[idx, config["column_name"]["sample_id"]],
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=config["column_name"]["sample_id"],
        synid=obj_upload["data1"],
        patient_id=data.loc[idx, config["column_name"]["patient_id"]],
        instrument=data.loc[idx, config["column_name"]["instrument"]],
        instance=data.loc[idx, config["column_name"]["instance"]],
        check_no=8,
        infer_site=False,
    )
    return output


def col_empty(config, cohort, site, report, output_format="log"):
    objs = []
    output = []

    if report == "upload":
        objs.append(config["uploads"][cohort][site])
    elif report == "table":
        synid_table_all = get_bpc_table_synapse_ids()
        for i in range(len(synid_table_all)):
            objs.append(
                {
                    "synid_table": str(synid_table_all[i]),
                    "previous": False,
                    "select": None,
                }
            )
    else:
        return None

    for obj in objs:
        data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj)

        is_col_empty = data.apply(lambda col: is_empty(col), axis=0)
        col_root_empty = [
            col.split("___")[0] for col in is_col_empty[is_col_empty].index
        ]
        col_root_not_empty = [
            col.split("___")[0] for col in is_col_empty[~is_col_empty].index
        ]
        col_empty = list(set(col_root_empty) - set(col_root_not_empty))

        instrument = None
        if obj["data1"] is None:
            instrument = get_bpc_table_instrument(obj["synid_table"])

        output.append(
            format_output(
                value=col_empty,
                cohort=cohort,
                site=site,
                output_format=output_format,
                column_name=col_empty,
                synid=obj["data1"] if obj["data1"] is not None else obj["synid_table"],
                patient_id=None,
                instrument=instrument,
                instance=None,
                check_no=9,
                infer_site=False,
            )
        )
    return output


def col_data_type_dd_mismatch(config, cohort, site, report, output_format="log"):
    return None


def col_entry_data_type_dd_mismatch(config, cohort, site, report, output_format="log"):
    return None


def col_data_type_sor_mismatch(config, cohort, site, report, output_format="log"):
    objs = []
    output = []

    # read sor
    sor = get_data(config["synapse"]["sor"]["id"], sheet=2)

    # gather data objects
    if report == "upload":
        objs.append(config["uploads"][cohort][site])
    elif report == "table":
        synid_table_all = get_bpc_table_synapse_ids()
        for i in range(len(synid_table_all)):
            objs.append(
                {
                    "synid_table": str(synid_table_all[i]),
                    "previous": False,
                    "select": None,
                }
            )
    else:
        return None

    for obj in objs:
        data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj)

        # remove allowed non-integer values
        for var in config["noninteger_values"].keys():
            if var in data.columns:
                data.loc[data[var].isin(config["noninteger_values"][var]), var] = None

        # variable data types
        type_inf = config["maps"]["data_type"][infer_data_type(data)]
        type_sor = get_bpc_sor_data_type(var_name=data.columns, sor=sor)
        idx = (type_sor == "numeric") & (type_inf == "character")

        # format output
        output.append(
            format_output(
                value=data.columns[idx],
                cohort=cohort,
                site=site,
                output_format=output_format,
                column_name=None,
                synid=obj["data1"] if obj["data1"] is not None else obj["synid_table"],
                patient_id=None,
                instrument=get_bpc_table_instrument(obj["synid_table"])
                if obj["data1"] is None
                else None,
                instance=None,
                check_no=12,
                infer_site=False,
            )
        )
    return output


def col_entry_data_type_sor_mismatch(config, cohort, site, report, output_format="log"):
    objs = []
    output = []

    # read sor and upload file
    sor = get_data(config["synapse"]["sor"]["id"], sheet=2)

    # gather data objects
    if report == "upload":
        objs.append(config["uploads"][cohort][site])
    elif report == "table":
        synid_table_all = get_bpc_table_synapse_ids()
        for i in range(len(synid_table_all)):
            objs.append(
                {
                    "synid_table": str(synid_table_all[i]),
                    "previous": False,
                    "select": None,
                }
            )
    else:
        return None

    for obj in objs:
        data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj)

        # remove allowed non-integer values
        for var in config["noninteger_values"].keys():
            if var in data.columns:
                data.loc[data[var].isin(config["noninteger_values"][var]), var] = None

        # variable data types from scope of release
        type_sor = get_bpc_sor_data_type(var_name=data.columns, sor=sor)
        type_sor.index = data.columns

        for i in range(data.shape[1]):
            type_sor_col = type_sor[data.columns[i]]

            if pd.notnull(type_sor_col) and type_sor_col == "numeric":
                type_inf = config["maps"]["data_type"][infer_data_type(data.iloc[:, i])]
                idx = type_inf == "character"

                # format output
                output.append(
                    format_output(
                        value=data.iloc[idx, i],
                        cohort=cohort,
                        site=site,
                        output_format=output_format,
                        column_name=data.columns[i],
                        synid=obj["data1"]
                        if obj["data1"] is not None
                        else obj["synid_table"],
                        patient_id=data["record_id"][idx],
                        instrument=get_bpc_table_instrument(obj["synid_table"])
                        if obj["data1"] is None
                        else data["redcap_repeat_instrument"][idx],
                        instance=data["redcap_repeat_instance"][idx],
                        check_no=13,
                        infer_site=False,
                    )
                )
    return output


def no_mapped_diag(config, cohort, site, report, output_format="log"):
    if report == "upload":
        obj = config["uploads"][cohort][site]
    elif report == "table":
        synid_table_all = get_bpc_table_synapse_ids()
        obj = {
            "synid_table": str(synid_table_all[config["table_name"]["sample_id"]]),
            "previous": False,
            "select": None,
        }
    else:
        return None

    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj)

    if report == "upload":
        res = data[
            (data[config["column_name"]["oncotree_code"]].isna())
            & (data["redcap_repeat_instrument"] == config["instrument_name"]["panel"])
        ]
    elif report == "table":
        res = data[data[config["column_name"]["oncotree_code"]].isna()]

    output = format_output(
        value=res[config["column_name"]["sample_id"]],
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=config["column_name"]["oncotree_code"],
        synid=obj["synid_table"] if obj["data1"] is None else obj["data1"],
        patient_id=res[config["column_name"]["patient_id"]],
        instrument=config["instrument_name"]["panel"],
        instance=res[config["column_name"]["instance"]],
        check_no=14,
        infer_site=False,
    )

    return output


def rows_added(config, cohort, site, report, output_format="log"):
    view = get_bpc_set_view(config, cohort, report)
    output = pd.DataFrame()
    synid_entity_source = ""
    for i in range(len(view)):
        if report in ["table", "comparison"]:
            synid_entity_source = view["id"][i]
            data = get_bpc_pair(config, cohort, site, report, synid_entity_source)
            primary_keys = view["primary_key"][i].split(", ")
        elif report == "release":
            synid_entity_source = view["id"][i]
            data = get_bpc_pair(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                synid_entity_source=synid_entity_source,
            )
            primary_keys = list(
                set(view["primary_key"][i].split(", ")).intersection(
                    set(data["current"].columns)
                )
            )
        cur_primary_key_values = data["current"][primary_keys].agg('-'.join, axis=1)
        prev_primary_key_values = data["previous"][primary_keys].agg('-'.join, axis=1)
        data_added = data["current"][
            ~cur_primary_key_values.isin(prev_primary_key_values)
        ][primary_keys]
        if not data_added.empty:
            # TODO use pd.concat
            output = output.append(
                format_output(
                    value=[None] * len(data_added),
                    cohort=cohort,
                    site=site,
                    output_format=output_format,
                    column_name=None,
                    synid=synid_entity_source,
                    patient_id=data_added["record_id"],
                    instrument=view["form"][i],
                    instance=data_added["redcap_repeat_instance"]
                    if "redcap_repeat_instance" in data_added
                    else None,
                    check_no=15,
                    infer_site=False,
                )
            )

    return output


def rows_removed(config, cohort, site, report, output_format="log", debug=False):
    view = get_bpc_set_view(config, cohort, report)
    output = pd.DataFrame()
    synid_entity_source = ""

    for i in range(len(view)):
        if report in ["table", "comparison"]:
            synid_entity_source = view["id"][i]
            data = get_bpc_pair(config, cohort, site, report, synid_entity_source)
            primary_keys = view["primary_key"][i].split(", ")
        elif report == "release":
            synid_entity_source = view["id"][i]
            data = get_bpc_pair(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                synid_entity_source=synid_entity_source,
            )
            primary_keys = list(
                set(view["primary_key"][i].split(", ")).intersection(
                    set(data["current"].columns)
                )
            )
        cur_primary_key_values = data["current"][primary_keys].agg('-'.join, axis=1)
        prev_primary_key_values = data["previous"][primary_keys].agg('-'.join, axis=1)
        data_removed = data["previous"][
            ~prev_primary_key_values.isin(cur_primary_key_values)
        ][primary_keys]
        # data_removed = data["previous"][
        #     ~data["previous"][primary_keys].isin(data["current"][primary_keys])
        # ][primary_keys]

        if not data_removed.empty:
            output = output.append(
                format_output(
                    value=[None] * len(data_removed),
                    cohort=cohort,
                    site=site,
                    output_format=output_format,
                    column_name=None,
                    synid=synid_entity_source,
                    patient_id=data_removed["record_id"],
                    instrument=view["form"][i],
                    instance=data_removed["redcap_repeat_instance"]
                    if "redcap_repeat_instance" in data_removed
                    else None,
                    check_no=16,
                    infer_site=False,
                )
            )

    return output


def sample_not_in_main_genie(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    mg_ids = get_main_genie_ids(
        config["synapse"]["genie_sample"]["id"], patient=True, sample=True
    )
    bpc_data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    bpc_sids = bpc_data["cpt_genie_sample_id"][
        ~bpc_data["cpt_genie_sample_id"].isna()
        & ~bpc_data["cpt_genie_sample_id"].str.contains("[-_]2$")
    ]

    bpc_not_mg_sid = get_added(bpc_sids, mg_ids["SAMPLE_ID"])
    bpc_not_mg_pid = []
    if len(bpc_not_mg_sid):
        bpc_not_mg_pid = bpc_data[
            bpc_data[config["column_name"]["sample_id"]].isin(bpc_not_mg_sid)
        ][config["column_name"]["patient_id"]].tolist()

    output = format_output(
        value=bpc_not_mg_sid,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name="cpt_genie_sample_id",
        synid=obj_upload["data1"],
        patient_id=bpc_not_mg_pid,
        instrument=None,
        instance=None,
        check_no=17,
        infer_site=False,
    )

    return output


def patient_not_in_main_genie(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    mg_ids = get_main_genie_ids(
        config["synapse"]["genie_sample"]["id"], patient=True, sample=False
    )
    bpc_data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    bpc_pids = bpc_data["record_id"][~bpc_data["record_id"].str.contains("[-_]2$")]

    bpc_not_mg_pid = get_added(bpc_pids, mg_ids["PATIENT_ID"])

    output = format_output(
        value=bpc_not_mg_pid,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name="record_id",
        synid=obj_upload["data1"],
        patient_id=bpc_not_mg_pid,
        instrument=None,
        instance=None,
        check_no=18,
        infer_site=False,
    )

    return output


def col_data_datetime_format_mismatch(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    synid_file_dd = get_bpc_synid_prissmm(
        synid_table_prissmm=config["synapse"]["prissmm"]["id"],
        cohort=cohort,
        file_name="Data Dictionary non-PHI",
    )
    dd = get_data(synid_file_dd)
    column_names = list(
        set(data.columns).intersection(
            set(
                dd[
                    dd["Text Validation Type OR Show Slider Number"].str.contains(
                        "^datetime_"
                    )
                ]["Variable / Field Name"]
            )
        )
    )

    res = {}
    for column_name in column_names:
        res[column_name] = all(
            is_timestamp_format_correct(
                data[column_name], formats=["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]
            )
        )

    values = [k for k, v in res.items() if not v]
    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=values,
        synid=obj_upload["data1"],
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=19,
        infer_site=False,
    )

    return output


def col_entry_datetime_format_mismatch(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    synid_file_dd = get_bpc_synid_prissmm(
        synid_table_prissmm=config["synapse"]["prissmm"]["id"],
        cohort=cohort,
        file_name="Data Dictionary non-PHI",
    )
    dd = get_data(synid_file_dd)
    column_names = list(
        set(data.columns).intersection(
            set(
                dd[
                    dd["Text Validation Type OR Show Slider Number"].str.contains(
                        "^datetime_"
                    )
                ]["Variable / Field Name"]
            )
        )
    )

    res = pd.DataFrame(False, index=data.index, columns=column_names)
    for column_name in column_names:
        res[column_name] = data[column_name].apply(
            lambda x: is_timestamp_format_correct(
                x, formats=["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]
            )
        )

    idx_values = res[res == False].stack().index.tolist()
    values = [data.loc[idx[0], idx[1]] for idx in idx_values]

    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=[idx[1] for idx in idx_values],
        synid=obj_upload["data1"],
        patient_id=data.loc[[idx[0] for idx in idx_values], "record_id"].tolist(),
        instrument=data.loc[
            [idx[0] for idx in idx_values], "redcap_repeat_instrument"
        ].tolist(),
        instance=data.loc[
            [idx[0] for idx in idx_values], "redcap_repeat_instance"
        ].tolist(),
        check_no=20,
        infer_site=False,
    )

    return output


def col_data_date_format_mismatch(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    synid_file_dd = get_bpc_synid_prissmm(
        synid_table_prissmm=config["synapse"]["prissmm"]["id"],
        cohort=cohort,
        file_name="Data Dictionary non-PHI",
    )
    dd = get_data(synid_file_dd)
    column_names = list(
        set(data.columns).intersection(
            set(
                dd[
                    dd["Text Validation Type OR Show Slider Number"].str.contains(
                        "^date_"
                    )
                ]["Variable / Field Name"]
            )
        )
    )

    res = {}
    for column_name in column_names:
        res[column_name] = (
            data[column_name]
            .apply(lambda x: is_date_format_correct(x, formats="%Y-%m-%d"))
            .all()
        )

    values = [k for k, v in res.items() if not v]
    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=values,
        synid=obj_upload["data1"],
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=21,
        infer_site=False,
    )

    return output


def col_entry_date_format_mismatch(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    synid_file_dd = get_bpc_synid_prissmm(
        synid_table_prissmm=config["synapse"]["prissmm"]["id"],
        cohort=cohort,
        file_name="Data Dictionary non-PHI",
    )
    dd = get_data(synid_file_dd)
    column_names = list(
        set(data.columns).intersection(
            set(
                dd[
                    dd["Text Validation Type OR Show Slider Number"].str.contains(
                        "^date_"
                    )
                ]["Variable / Field Name"]
            )
        )
    )

    res = pd.DataFrame(False, index=data.index, columns=column_names)
    for column_name in column_names:
        res[column_name] = data[column_name].apply(
            lambda x: is_date_format_correct(x, formats="%Y-%m-%d")
        )

    idx_values = res[res == False].stack().index.tolist()
    values = [data.loc[idx[0], idx[1]] for idx in idx_values]

    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=[idx[1] for idx in idx_values],
        synid=obj_upload["data1"],
        patient_id=data.loc[[idx[0] for idx in idx_values], "record_id"].tolist(),
        instrument=data.loc[
            [idx[0] for idx in idx_values], "redcap_repeat_instrument"
        ].tolist(),
        instance=data.loc[
            [idx[0] for idx in idx_values], "redcap_repeat_instance"
        ].tolist(),
        check_no=22,
        infer_site=False,
    )

    return output


def col_empty_but_required(
    config, cohort, site, report, output_format="log", exclude=["qa_full_reviewer_dual"]
):
    synid_file_dd = get_bpc_synid_prissmm(
        synid_table_prissmm=config["synapse"]["prissmm"]["id"],
        cohort=cohort,
        file_name="Data Dictionary non-PHI",
    )
    dd = get_data(synid_file_dd)

    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    col_req = dd[
        dd["Required Field?"] == "y" & ~dd["Variable / Field Name"].isin(exclude)
    ]["Variable / Field Name"].tolist()

    is_col_empty = data.apply(lambda x: x.count() == 0)
    col_root_empty = [name.split("___")[0] for name in is_col_empty[is_col_empty].index]
    col_root_not_empty = [
        name.split("___")[0] for name in is_col_empty[~is_col_empty].index
    ]
    col_empty = list(set(col_root_empty) - set(col_root_not_empty))

    output = format_output(
        value=list(set(col_req).intersection(set(col_empty))),
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=None,
        synid=obj_upload["data1"],
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=23,
        infer_site=False,
    )

    return output


def col_table_not_sor(config, cohort, site, report, output_format="log"):
    sor = get_data(config["synapse"]["sor"]["id"], sheet=2)
    sor_variables = (
        sor[
            (
                sor["TYPE"].isin(
                    ["Curated", "Project GENIE Tier 1 data", "Tumor Registry"]
                )
            )
        ]["VARNAME"]
        .unique()
        .tolist()
    )

    query = f"SELECT id FROM {config['synapse']['tables_view']['id']} WHERE double_curated = 'false'"
    synid_tables = syn.tableQuery(query, includeRowIdAndRowVersion=False)["id"].tolist()

    table_variables = []
    for synid_table in synid_tables:
        schema = syn.get(synid_table)
        cols = [x["name"] for x in syn.getTableColumns(schema)]
        table_variables.extend(cols)

    values = list(set(table_variables) - set(sor_variables))
    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=values,
        synid=config["synapse"]["tables_view"]["id"],
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=24,
        infer_site=False,
    )

    return output


def col_sor_not_table(config, cohort, site, report, output_format="log"):
    sor = get_data(config["synapse"]["sor"]["id"], sheet=2)
    sor_variables = (
        sor[
            (
                sor["TYPE"].isin(
                    ["Curated", "Project GENIE Tier 1 data", "Tumor Registry"]
                )
            )
        ]["VARNAME"]
        .unique()
        .tolist()
    )

    query = f"SELECT id FROM {config['synapse']['tables_view']['id']} WHERE double_curated = 'false'"
    synid_tables = syn.tableQuery(query, includeRowIdAndRowVersion=False)["id"].tolist()

    table_variables = []
    for synid_table in synid_tables:
        schema = syn.get(synid_table)
        cols = [x["name"] for x in syn.getTableColumns(schema)]
        table_variables.extend(cols)

    values = list(set(sor_variables) - set(table_variables))
    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=values,
        synid=config["synapse"]["tables_view"]["id"],
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=25,
        infer_site=False,
    )

    return output


def patient_marked_removed_from_bpc(config, cohort, site, report, output_format="log"):
    if report == "upload":
        obj = config["uploads"][cohort][site]
    elif report == "table":
        synid_table_all = get_bpc_table_synapse_ids()
        obj = {
            "synid_table": str(synid_table_all[config["table_name"]["patient_id"]]),
            "previous": False,
            "select": None,
        }
    else:
        return None

    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj)

    query = f"SELECT record_id FROM {config['synapse']['rm_pat']['id']} WHERE {cohort} = 'true'"
    pat_rm = syn.tableQuery(query, includeRowIdAndRowVersion=False)[
        "record_id"
    ].tolist()

    values = list(
        set(data[config["column_name"]["patient_id"]]).intersection(set(pat_rm))
    )
    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=config["column_name"]["patient_id"],
        synid=obj["data1"] if "data1" in obj else obj["synid_table"],
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=26,
        infer_site=False,
    )

    return output


def sample_marked_removed_from_bpc(config, cohort, site, report, output_format="log"):
    if report == "upload":
        obj = config["uploads"][cohort][site]
    elif report == "table":
        synid_table_all = get_bpc_table_synapse_ids()
        obj = {
            "synid_table": str(synid_table_all[config["table_name"]["sample_id"]]),
            "previous": False,
            "select": None,
        }
    else:
        return None

    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj)

    query = f"SELECT SAMPLE_ID FROM {config['synapse']['rm_sam']['id']} WHERE {cohort} = 'true'"
    sam_rm = syn.tableQuery(query, includeRowIdAndRowVersion=False)[
        "SAMPLE_ID"
    ].tolist()

    values = list(
        set(data[config["column_name"]["sample_id"]]).intersection(set(sam_rm))
    )
    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=config["column_name"]["sample_id"],
        synid=obj["data1"] if "data1" in obj else obj["synid_table"],
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=27,
        infer_site=False,
    )

    return output


def patient_count_too_small(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    n_current = get_bpc_case_count(data)

    phase_cohort_list = parse_phase_from_cohort(cohort)
    cohort_without_phase = phase_cohort_list[0]
    phase = phase_cohort_list[1]
    query = f"SELECT target_cases FROM {config['synapse']['target_count']['id']} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}"
    query_result = syn.tableQuery(query, includeRowIdAndRowVersion=False)[
        "target_cases"
    ].tolist()
    n_target = 0
    if len(query_result):
        n_target = query_result[0]

    output = None
    if n_current < n_target:
        output = format_output(
            value=f"{n_current}-->{n_target} (need {n_target - n_current} more)",
            cohort=cohort,
            site=site,
            output_format=output_format,
            column_name=config["column_name"]["patient_id"],
            synid=obj_upload["data1"],
            patient_id=None,
            instrument=None,
            instance=None,
            check_no=28,
            infer_site=False,
        )

    return output


def investigational_drug_duration(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data_upload = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    code_masked = config["maps"]["drug"]["investigational_drug"]

    results = pd.DataFrame()
    for i in range(1, 6):
        temp = data_upload[
            (data_upload["redcap_repeat_instrument"] == "ca_directed_drugs")
            & (data_upload[f"drugs_drug_{i}"] == code_masked)
            & (
                data_upload[f"drugs_startdt_int_{i}"]
                != data_upload[f"drugs_enddt_int_{i}"]
            )
        ][["record_id", "redcap_repeat_instrument", "redcap_repeat_instance"]]
        temp["column_name"] = f"drugs_enddt_int_{i}"
        results = pd.concat([results, temp])

    output = format_output(
        value=[None] * len(results),
        cohort=cohort,
        site=site,
        synid=obj_upload["data1"],
        patient_id=results["record_id"],
        instrument=results["redcap_repeat_instrument"],
        instance=results["redcap_repeat_instance"],
        column_name=results["column_name"],
        check_no=29,
        infer_site=False,
    )

    return output


def investigational_drug_other_name(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data_upload = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    code_masked = config["maps"]["drug"]["investigational_drug"]

    results = pd.DataFrame()
    for i in range(1, 6):
        temp = data_upload[
            (data_upload["redcap_repeat_instrument"] == "ca_directed_drugs")
            & (data_upload[f"drugs_drug_{i}"] == code_masked)
            & pd.notnull(data_upload[f"drugs_drug_oth_{i}"])
        ][["record_id", "redcap_repeat_instrument", "redcap_repeat_instance"]]
        temp["column_name"] = f"drugs_drug_oth_{i}"
        results = pd.concat([results, temp])

    output = format_output(
        value=[None] * len(results),
        cohort=cohort,
        site=site,
        synid=obj_upload["data1"],
        patient_id=results["record_id"],
        instrument=results["redcap_repeat_instrument"],
        instance=results["redcap_repeat_instance"],
        column_name=results["column_name"],
        check_no=30,
        infer_site=False,
    )

    return output


def investigational_drug_not_ct(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data_upload = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    code_masked = config["maps"]["drug"]["investigational_drug"]
    code_yes = config["maps"]["drug"]["ct_yes"]

    results = pd.DataFrame()
    for i in range(1, 6):
        temp = data_upload[
            (data_upload["redcap_repeat_instrument"] == "ca_directed_drugs")
            & (data_upload[f"drugs_drug_{i}"] == code_masked)
            & (data_upload["drugs_ct_yn"] != code_yes)
        ][["record_id", "redcap_repeat_instrument", "redcap_repeat_instance"]]
        temp["column_name"] = f"drugs_drug_{i}"
        results = pd.concat([results, temp])

    output = format_output(
        value=[None] * len(results),
        cohort=cohort,
        site=site,
        synid=obj_upload["data1"],
        patient_id=results["record_id"],
        instrument=results["redcap_repeat_instrument"],
        instance=results["redcap_repeat_instance"],
        column_name=results["column_name"],
        check_no=31,
        infer_site=False,
    )

    return output


def drug_not_fda_approved(config, cohort, site, report, output_format="log"):
    code_drug = []
    fda_status = {}
    results = pd.DataFrame()

    year_curation = get_bpc_curation_year(cohort, site)

    obj_upload = config["uploads"][cohort][site]
    data_upload = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    code_masked = config["maps"]["drug"]["investigational_drug"]

    for i in range(1, 6):
        code_drug_i = (
            data_upload[
                (data_upload["redcap_repeat_instrument"] == "ca_directed_drugs")
                & (data_upload[f"drugs_drug_{i}"] != code_masked)
            ][f"drugs_drug_{i}"]
            .unique()
            .tolist()
        )
        code_drug.extend(code_drug_i)

    for code_ncit in code_drug:
        code_hemonc = get_hemonc_from_ncit(code_ncit)
        year_fda = get_hemonc_fda_approval_year(code_hemonc)

        if pd.isnull(year_fda):
            fda_status[code_ncit] = "unknown"
        elif year_curation <= year_fda:
            fda_status[code_ncit] = "unapproved"
        else:
            fda_status[code_ncit] = "approved"

    code_unapproved = [k for k, v in fda_status.items() if v != "approved"]
    for i in range(1, 6):
        temp = data_upload[
            (data_upload["redcap_repeat_instrument"] == "ca_directed_drugs")
            & data_upload[f"drugs_drug_{i}"].isin(code_unapproved)
        ][
            [
                "record_id",
                "redcap_repeat_instrument",
                "redcap_repeat_instance",
                f"drugs_drug_{i}",
            ]
        ]
        temp["column_name"] = f"drugs_drug_{i}"
        temp.rename(columns={f"drugs_drug_{i}": "drug_name"}, inplace=True)
        results = pd.concat([results, temp])

    output = format_output(
        value=results["drug_name"].tolist(),
        cohort=cohort,
        site=site,
        synid=obj_upload["data1"],
        patient_id=results["record_id"],
        instrument=results["redcap_repeat_instrument"],
        instance=results["redcap_repeat_instance"],
        column_name=results["column_name"],
        check_no=32,
        infer_site=False,
    )

    return output


def irr_sample(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    mg_ids = get_main_genie_ids(
        config["synapse"]["genie_sample"]["id"], patient=True, sample=True
    )
    bpc_data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    bpc_sids = bpc_data["cpt_genie_sample_id"][
        bpc_data["cpt_genie_sample_id"].str.contains("[-_]2$")
    ]
    bpc_not_mg_sid = get_added(bpc_sids, mg_ids["SAMPLE_ID"])
    bpc_not_mg_pid = pd.DataFrame()

    if len(bpc_not_mg_sid):
        bpc_not_mg_pid = mg_ids[mg_ids["SAMPLE_ID"].isin(bpc_not_mg_sid)][
            ["PATIENT_ID"]
        ]

    output = format_output(
        value=bpc_not_mg_sid,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name="cpt_genie_sample_id",
        synid=obj_upload["data1"],
        patient_id=bpc_not_mg_pid,
        instrument=None,
        instance=None,
        check_no=17,
        infer_site=False,
    )

    return output


def irr_patient(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    mg_ids = get_main_genie_ids(
        config["synapse"]["genie_sample"]["id"], patient=True, sample=False
    )
    bpc_data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    bpc_pids = bpc_data["record_id"][bpc_data["record_id"].str.contains("[-_]2$")]
    bpc_not_mg_pid = get_added(bpc_pids, mg_ids["PATIENT_ID"])

    output = format_output(
        value=bpc_not_mg_pid,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name="record_id",
        synid=obj_upload["data1"],
        patient_id=bpc_not_mg_pid,
        instrument=None,
        instance=None,
        check_no=18,
        infer_site=False,
    )

    return output


def col_empty_site_not_others(config, cohort, site, report, output_format="log"):
    objs = []
    output = []
    sites = [site]

    if pd.isnull(site):
        sites = list(
            set(config["uploads"][cohort].keys()) - set(config["constants"]["sage"])
        )

    synid_table_all = get_bpc_table_synapse_ids()
    for i in range(len(synid_table_all)):
        objs.append(
            {"synid_table": str(synid_table_all[i]), "previous": False, "select": None}
        )

    for obj in objs:
        data = get_bpc_data(config=config, cohort=cohort, site=None, report=report, obj=obj)

        for index_site in sites:
            data_index_site = data[data["record_id"].str.contains(index_site)]
            data_other_site = data[~data["record_id"].str.contains(index_site)]
            idx_index_site = data_index_site.columns[
                data_index_site.isnull().all()
            ].tolist()
            idx_other_site = data_other_site.columns[
                data_other_site.isnull().all()
            ].tolist()
            idx_highlight = list(set(idx_index_site) - set(idx_other_site))

            output.append(
                format_output(
                    value=idx_highlight,
                    cohort=cohort,
                    site=index_site,
                    output_format=output_format,
                    column_name=idx_highlight,
                    synid=obj["data1"]
                    if obj["data1"] is not None
                    else obj["synid_table"],
                    patient_id=None,
                    instrument=get_bpc_table_instrument(obj["synid_table"])
                    if obj["data1"] is None
                    else data["redcap_repeat_instrument"][idx_highlight],
                    instance=None,
                    check_no=35,
                    infer_site=False,
                )
            )

    return output


def col_five_perc_inc_missing(config, cohort, site, report, output_format="log"):
    output = []

    synid_view_current = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["current"]
    )
    synid_view_previous = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["previous"]
    )
    synid_view_all = synid_view_current[
        synid_view_current["form"].isin(synid_view_previous["form"])
    ]

    for i in range(len(synid_view_all)):
        if report == "table" or report == "comparison":
            data_curr = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "synid_table": str(synid_view_all["id"].iloc[i]),
                    "previous": False,
                    "select": None,
                },
                current=True
            )
            data_prev = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "synid_table": str(synid_view_all["id"].iloc[i]),
                    "previous": True,
                    "select": None,
                },
            )
        elif report == "release":
            data_curr = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "version": config["release"][cohort]["current"],
                    "file_name": synid_view_all["form"].iloc[i],
                },
                current=True
            )
            data_prev = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "version": config["release"][cohort]["previous"],
                    "file_name": synid_view_all["form"].iloc[i],
                },
            )

        f_miss_curr = data_curr.apply(fraction_empty)
        f_miss_prev = data_prev.apply(fraction_empty)
        vars = f_miss_curr.index.intersection(f_miss_prev.index)
        idx_diff = (f_miss_curr.loc[vars] - f_miss_prev.loc[vars]) > config[
            "thresholds"
        ]["fraction_missing"]

        if idx_diff.any():
            output.append(
                format_output(
                    # value=data_curr.columns[idx_diff],
                    value=vars.to_series()[idx_diff],
                    cohort=cohort,
                    site=site,
                    output_format=output_format,
                    # column_name=data_curr.columns[idx_diff],
                    column_name=vars.to_series()[idx_diff],
                    synid=synid_view_all["id"].iloc[i],
                    patient_id=None,
                    instrument=None,
                    instance=None,
                    check_no=36,
                    infer_site=False,
                )
            )

    return output


def col_five_perc_dec_missing(config, cohort, site, report, output_format="log"):
    output = []

    synid_view_current = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["current"]
    )
    synid_view_previous = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["previous"]
    )
    synid_view_all = synid_view_current[
        synid_view_current["form"].isin(synid_view_previous["form"])
    ]

    for i in range(len(synid_view_all)):
        if report == "table" or report == "comparison":
            data_curr = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "synid_table": str(synid_view_all["id"].iloc[i]),
                    "previous": False,
                    "select": None,
                },
            )
            data_prev = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "synid_table": str(synid_view_all["id"].iloc[i]),
                    "previous": True,
                    "select": None,
                },
            )
        elif report == "release":
            data_curr = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "version": config["release"][cohort]["current"],
                    "file_name": synid_view_all["form"].iloc[i],
                },
            )
            data_prev = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "version": config["release"][cohort]["previous"],
                    "file_name": synid_view_all["form"].iloc[i],
                },
            )

        f_miss_curr = data_curr.apply(fraction_empty)
        f_miss_prev = data_prev.apply(fraction_empty)

        vars = f_miss_curr.index.intersection(f_miss_prev.index)
        idx_diff = (f_miss_prev.loc[vars] - f_miss_curr.loc[vars]) > config[
            "thresholds"
        ]["fraction_missing"]

        if idx_diff.any():
            output.append(
                format_output(
                    value=vars.to_series()[idx_diff],
                    cohort=cohort,
                    site=site,
                    output_format=output_format,
                    column_name=vars.to_series()[idx_diff],
                    synid=synid_view_all["id"].iloc[i],
                    patient_id=None,
                    instrument=None,
                    instance=None,
                    check_no=37,
                    infer_site=False,
                )
            )

    return output


def col_removed(config, cohort, site, report, output_format="log"):
    output = []

    synid_view_current = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["current"]
    )
    synid_view_previous = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["previous"]
    )
    synid_view_all = synid_view_current[
        synid_view_current["form"].isin(synid_view_previous["form"])
    ]

    for i in range(len(synid_view_all)):
        if report == "table" or report == "comparison":
            data_curr = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "synid_table": str(synid_view_all["id"].iloc[i]),
                    "previous": False,
                    "select": None,
                },
            )
            data_prev = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "synid_table": str(synid_view_all["id"].iloc[i]),
                    "previous": True,
                    "select": None,
                },
            )
        elif report == "release":
            data_curr = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "version": config["release"][cohort]["current"],
                    "file_name": synid_view_all["form"].iloc[i],
                },
            )
            data_prev = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "version": config["release"][cohort]["previous"],
                    "file_name": synid_view_all["form"].iloc[i],
                },
            )

        values = list(set(data_prev.columns) - set(data_curr.columns))

        if len(values):
            output.append(
                format_output(
                    value=values,
                    cohort=cohort,
                    site=site,
                    output_format=output_format,
                    column_name=values,
                    synid=synid_view_all["id"].iloc[i],
                    patient_id=None,
                    instrument=None,
                    instance=None,
                    check_no=38,
                    infer_site=False,
                )
            )

    return output


def col_added(config, cohort, site, report, output_format="log"):
    output = []

    synid_view_current = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["current"]
    )
    synid_view_previous = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["previous"]
    )
    synid_view_all = synid_view_current[
        synid_view_current["form"].isin(synid_view_previous["form"])
    ]

    for i in range(len(synid_view_all)):
        if report == "table" or report == "comparison":
            data_curr = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "synid_table": str(synid_view_all["id"].iloc[i]),
                    "previous": False,
                    "select": None,
                },
            )
            data_prev = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "synid_table": str(synid_view_all["id"].iloc[i]),
                    "previous": True,
                    "select": None,
                },
            )
        elif report == "release":
            data_curr = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "version": config["release"][cohort]["current"],
                    "file_name": synid_view_all["form"].iloc[i],
                },
            )
            data_prev = get_bpc_data(
                config=config,
                cohort=cohort,
                site=site,
                report=report,
                obj={
                    "version": config["release"][cohort]["previous"],
                    "file_name": synid_view_all["form"].iloc[i],
                },
            )

        values = list(set(data_curr.columns) - set(data_prev.columns))

        if len(values):
            output.append(
                format_output(
                    value=values,
                    cohort=cohort,
                    site=site,
                    output_format=output_format,
                    column_name=values,
                    synid=synid_view_all["id"].iloc[i],
                    patient_id=None,
                    instrument=None,
                    instance=None,
                    check_no=39,
                    infer_site=False,
                )
            )

    return output


def file_added(config, cohort, site, report, output_format="log"):
    synid_view_current = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["current"]
    )
    synid_view_previous = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["previous"]
    )

    values = list(set(synid_view_current["form"]) - set(synid_view_previous["form"]))
    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=None,
        synid=None,
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=40,
        infer_site=False,
    )

    return output


def file_removed(config, cohort, site, report, output_format="log"):
    synid_view_current = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["current"]
    )
    synid_view_previous = get_bpc_set_view(
        config=config, cohort=cohort, report=report, version=config["release"][cohort]["previous"]
    )

    values = list(set(synid_view_previous["form"]) - set(synid_view_current["form"]))
    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=None,
        synid=None,
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=41,
        infer_site=False,
    )

    return output


def required_not_uploaded(
    config, cohort, site, report, output_format="log", exclude=["qa_full_reviewer_dual"]
):
    synid_file_dd = get_bpc_synid_prissmm(
        synid_table_prissmm=config["synapse"]["prissmm"]["id"],
        cohort=cohort,
        file_name="Data Dictionary non-PHI",
    )
    dd = get_data(synid_file_dd)

    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    col_req = dd[dd["Required Field?"] == "y"][
        ~dd["Variable / Field Name"].isin(exclude)
    ]["Variable / Field Name"].tolist()
    col_data = [col.split("___")[0] for col in data.columns]

    values = list(set(col_req) - set(col_data))
    output = format_output(
        value=values,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=values,
        synid=obj_upload["data1"],
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=42,
        infer_site=False,
    )

    return output


def ct_drug_not_investigational(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data_upload = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)
    code_masked = config["maps"]["drug"]["investigational_drug"]
    code_yes = config["maps"]["drug"]["ct_yes"]

    results = pd.DataFrame()
    for i in range(1, 6):
        # query = f"""
        # SELECT record_id, redcap_repeat_instrument, redcap_repeat_instance, 'drugs_drug_{i}' as column_name
        # FROM data_upload
        # WHERE redcap_repeat_instrument = 'ca_directed_drugs' AND drugs_drug_{i} != {code_masked} AND drugs_ct_yn = {code_yes}
        # """
        filtered_df = data_upload[
            (data_upload['redcap_repeat_instrument'] == 'ca_directed_drugs') & 
            (data_upload[f'drugs_drug_{i}'] != code_masked) & 
            (data_upload['drugs_ct_yn'] == code_yes)
        ]
        filtered_df.rename(columns={f'drugs_drug_{i}': 'column_name'}, inplace=True)
        result = filtered_df[['record_id', 'redcap_repeat_instrument', 'redcap_repeat_instance', 'column_name']]
        results = pd.concat([results, result])

    output = format_output(
        value=[None] * len(results),
        cohort=cohort,
        site=site,
        synid=obj_upload["data1"],
        patient_id=results["record_id"].tolist(),
        instrument=results["redcap_repeat_instrument"].tolist(),
        instance=results["redcap_repeat_instance"].tolist(),
        column_name=results["column_name"].tolist(),
        check_no=43,
        infer_site=False,
    )

    return output


def file_not_csv(config, cohort, site, report, output_format="log"):
    output = []
    res = {}
    obj_upload = config["uploads"][cohort][site]

    if obj_upload["data1"] is not None:
        res[obj_upload["data1"]] = is_synapse_entity_csv(obj_upload["data1"])

    if obj_upload["data2"] is not None:
        res[obj_upload["data2"]] = is_synapse_entity_csv(obj_upload["data2"])

    if res and not all(res.values()):
        output = format_output(
            value=None,
            cohort=cohort,
            site=site,
            synid=[k for k, v in res.items() if not v],
            patient_id=None,
            instrument=None,
            instance=None,
            column_name=None,
            check_no=44,
            infer_site=False,
        )

    return output


def data_header_col_mismatch(config, cohort, site, report, output_format="log"):
    output = []
    res = {}
    obj_upload = config["uploads"][cohort][site]

    if obj_upload["data1"] is not None and obj_upload["header1"] is not None:
        data1 = get_data(obj_upload["data1"])
        header1 = get_data(obj_upload["header1"])
        res[obj_upload["data1"]] = data1.shape[1] == header1.shape[1]

    if obj_upload["data2"] is not None and obj_upload["header2"] is not None:
        data2 = get_data(obj_upload["data2"])
        header2 = get_data(obj_upload["header2"])
        res[obj_upload["data2"]] = data2.shape[1] == header2.shape[1]

    if res and not all(res.values()):
        output = format_output(
            value=None,
            cohort=cohort,
            site=site,
            synid=[k for k, v in res.items() if not v],
            patient_id=None,
            instrument=None,
            instance=None,
            column_name=None,
            check_no=45,
            infer_site=False,
        )

    return output


def current_count_not_target(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    n_current = get_bpc_case_count(data)

    phase_cohort_list = parse_phase_from_cohort(cohort)
    cohort_without_phase = phase_cohort_list[0]
    phase = phase_cohort_list[1]
    query = f"SELECT target_cases FROM {config['synapse']['target_count']['id']} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}"
    n_target = int(
        syn.tableQuery(query, includeRowIdAndRowVersion=False)
        .asDataFrame()
        .values[0][0]
    )

    output = None
    if n_current != n_target:
        output = format_output(
            value=f"current count {n_current} not equal to target {n_target}",
            cohort=cohort,
            site=site,
            output_format=output_format,
            column_name=config["column_name"]["patient_id"],
            synid=obj_upload["data1"],
            patient_id=None,
            instrument=None,
            instance=None,
            check_no=46,
            infer_site=False,
        )

    return output


def patient_count_too_large(config, cohort, site, report, output_format="log"):
    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    n_current = get_bpc_case_count(data)

    phase_cohort_list = parse_phase_from_cohort(cohort)
    cohort_without_phase = phase_cohort_list[0]
    phase = phase_cohort_list[1]
    query = f"SELECT target_cases FROM {config['synapse']['target_count']['id']} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}"
    query_result = (
        syn.tableQuery(query, includeRowIdAndRowVersion=False).asDataFrame().values
    )
    n_target = 0
    if len(query_result):
        n_target = int(query_result[0][0])

    output = None
    if n_current > n_target:
        output = format_output(
            value=f"{n_current}-->{n_target} (remove {n_current - n_target})",
            cohort=cohort,
            site=site,
            output_format=output_format,
            column_name=config["column_name"]["patient_id"],
            synid=obj_upload["data1"],
            patient_id=None,
            instrument=None,
            instance=None,
            check_no=47,
            infer_site=False,
        )

    return output


def cpt_sample_type_numeric(config, cohort, site, report, output_format="log"):
    output = None

    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    res = data[
        (data["redcap_repeat_instrument"] == config["instrument_name"]["panel"])
        & data["cpt_sample_type"].apply(lambda x: isinstance(x, float))
    ]

    output = format_output(
        value=res["cpt_sample_type"].tolist(),
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name="cpt_sample_type",
        synid=obj_upload["data1"],
        patient_id=res["record_id"].tolist(),
        instrument=res["redcap_repeat_instrument"].tolist(),
        instance=res["redcap_repeat_instance"].tolist(),
        check_no=48,
        infer_site=False,
    )

    return output


def quac_required_column_missing(config, cohort, site, report, output_format="log"):
    output = None

    col_req = list(config["column_name"].values())

    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    res = list(set(col_req) - set(data.columns))

    output = format_output(
        value=res,
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=res,
        synid=obj_upload["data1"],
        patient_id=None,
        instrument=None,
        instance=None,
        check_no=49,
        infer_site=False,
    )

    return output


def invalid_choice_code(config, cohort, site, report, output_format="log"):
    output = None

    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    synid_dd = get_bpc_synid_prissmm(
        synid_table_prissmm=config["synapse"]["prissmm"]["id"],
        cohort=cohort,
        file_name="Data Dictionary non-PHI",
    )
    dd = get_data(synid_dd)

    for i in range(len(data.columns)):
        var_name = data.columns[i]
        choices = dd[dd["Variable / Field Name"] == var_name][
            "Choices, Calculations, OR Slider Labels"
        ]
        if len(choices) and not pd.isna(choices).all():
            codes = parse_mapping(choices)["codes"]
            if isinstance(codes, float):
                codes = codes.append(float(codes))
            idx_invalid = data.iloc[:, i].apply(lambda x: x not in [None] + list(codes))

            if not idx_invalid.empty:
                output = pd.concat(
                    [
                        output,
                        format_output(
                            value=data.loc[idx_invalid, data.columns[i]],
                            cohort=cohort,
                            site=site,
                            output_format=output_format,
                            column_name=data.columns[i],
                            synid=obj_upload["data1"],
                            patient_id=data.loc[
                                idx_invalid, config["column_name"]["patient_id"]
                            ],
                            instrument=data.loc[
                                idx_invalid, config["column_name"]["instrument"]
                            ],
                            instance=data.loc[
                                idx_invalid, config["column_name"]["instance"]
                            ],
                            check_no=50,
                            infer_site=False,
                        ),
                    ]
                )

    return output


def less_than_adjusted_target(config, cohort, site, report, output_format="log"):
    output = None

    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    n_current = get_bpc_case_count(data)

    phase_cohort_list = parse_phase_from_cohort(cohort)
    cohort_without_phase = phase_cohort_list[0]
    phase = phase_cohort_list[1]
    query = f"SELECT adjusted_cases FROM {config['synapse']['target_count']['id']} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}"
    n_adj = int(
        syn.tableQuery(query, includeRowIdAndRowVersion=False)
        .asDataFrame()
        .values[0][0]
    )

    output = None
    if n_current < n_adj:
        output = format_output(
            value=f"{n_current}-->{n_adj} (add {n_adj - n_current})",
            cohort=cohort,
            site=site,
            output_format=output_format,
            column_name=config["column_name"]["patient_id"],
            synid=obj_upload["data1"],
            patient_id=None,
            instrument=None,
            instance=None,
            check_no=51,
            infer_site=False,
        )

    return output


def greater_than_adjusted_target(config, cohort, site, report, output_format="log"):
    output = None

    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    n_current = get_bpc_case_count(data)

    phase_cohort_list = parse_phase_from_cohort(cohort)
    cohort_without_phase = phase_cohort_list[0]
    phase = phase_cohort_list[1]
    query = f"SELECT adjusted_cases FROM {config['synapse']['target_count']['id']} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}"
    n_adj = int(
        syn.tableQuery(query, includeRowIdAndRowVersion=False)
        .asDataFrame()
        .values[0][0]
    )

    output = None
    if n_current > n_adj:
        output = format_output(
            value=f"{n_current}-->{n_adj} (remove {n_current - n_adj})",
            cohort=cohort,
            site=site,
            output_format=output_format,
            column_name=config["column_name"]["patient_id"],
            synid=obj_upload["data1"],
            patient_id=None,
            instrument=None,
            instance=None,
            check_no=52,
            infer_site=False,
        )

    return output


def character_double_value(config, cohort, site, report, output_format="log"):
    output = None

    synid_table_all = get_bpc_table_synapse_ids()
    objs = [
        {"synid_table": str(synid), "previous": False, "select": None}
        for synid in synid_table_all
    ]

    for obj in objs:
        data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj)

        for i in range(len(data.columns)):
            idx = data.iloc[:, i].apply(lambda x: not isinstance(x, float))
            data_col = data.iloc[:, i].copy()
            data_col[idx] = None
            comp = pd.DataFrame(
                {
                    "original": data_col.astype(str),
                    "converted": data_col.astype(float).astype(str),
                }
            )

            if not comp["original"].equals(comp["converted"]):
                output = pd.concat(
                    [
                        output,
                        format_output(
                            value=None,
                            cohort=cohort,
                            site=site,
                            output_format=output_format,
                            column_name=data.columns[i],
                            synid=obj["synid_table"],
                            patient_id=None,
                            instrument=None,
                            instance=None,
                            check_no=53,
                            infer_site=False,
                        ),
                    ]
                )

    return output


def patient_removed_not_retracted(config, cohort, site, report, output_format="log"):
    results = get_bpc_patient_sample_added_removed(
        config=config,
        cohort=cohort,
        site=site,
        report=report,
        check_patient=True,
        check_added=False,
        account_for_retracted=True,
    )
    output = format_output(
        results["ids"],
        cohort=cohort,
        site=site,
        output_format=output_format,
        column_name=results["column_name"],
        synid=results["synid_entity_source"],
        check_no=54,
        infer_site=True,
    )

    return output


def sample_removed_not_retracted(config, cohort, site, report, output_format="log"):
    output = None

    results = get_bpc_patient_sample_added_removed(
        config=config,
        cohort=cohort,
        site=site,
        report=report,
        check_patient=False,
        check_added=False,
        account_for_retracted=True,
    )
    retracted_pts = get_retracted_patients(cohort)
    for pt in retracted_pts:
        results["ids"] = [id for id in results["ids"] if pt not in id]

    if len(results["ids"]):
        output = format_output(
            results["ids"],
            cohort=cohort,
            site=site,
            output_format=output_format,
            column_name=results["column_name"],
            synid=results["synid_entity_source"],
            check_no=55,
            infer_site=True,
        )

    return output


def sample_missing_oncotree_code(config, cohort, site, report, output_format="log"):
    output = None

    obj_upload = config["uploads"][cohort][site]
    data = get_bpc_data(config=config, cohort=cohort, site=site, report=report, obj=obj_upload)

    var_code = config["column_name"]["oncotree_code"]
    var_pat = config["column_name"]["patient_id"]
    var_sam = config["column_name"]["sample_id"]
    var_form = config["column_name"]["instrument"]
    var_insta = config["column_name"]["instance"]

    df_res = data[
        (data[var_code].isna()) & (data[var_form] == config["instrument_name"]["panel"])
    ][[var_pat, var_sam, var_form, var_insta]]

    if len(df_res):
        output = format_output(
            value=df_res[var_sam],
            cohort=cohort,
            site=site,
            output_format=output_format,
            column_name=var_code,
            synid=obj_upload["data1"],
            patient_id=df_res[var_pat],
            instrument=df_res[var_form],
            instance=df_res[var_insta],
            check_no=56,
            infer_site=False,
        )

    return output
