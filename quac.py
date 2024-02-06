#!/usr/bin/env python

# Description: Command line interface for conducting quality assurance (QA) checks
#   on BPC cohorts and sites for different stages of processing.
# Author: Haley Hunter-Zinck
# Date: June 10, 2021

# pre-setup ---------------------------------

import argparse
import datetime
import os
import time

import pandas as pd
import synapseclient
import yaml

from utils import save_to_synapse
from checklist import (
    get_check_functions,
    update_config_for_comparison_report,
    update_config_for_release_report
)

tic = time.time()

workdir = "."
if not os.path.exists("config.yaml"):
    workdir = "/usr/local/src/myscripts"
with open(os.path.join(workdir, "config.yaml"), "r") as stream:
    config = yaml.safe_load(stream)

choices_report = sorted(config["report"].keys())
choices_number = [
    int(k)
    for k, v in config["checks"].items()
    if v["implemented"] and not v["deprecated"]
]
choices_level = sorted(
    set([v["level"] for v in config["checks"].values() if v["level"] != "fail"])
)
choices_cohort = sorted(config["uploads"].keys())
choices_site = sorted(
    set([item for sublist in config["uploads"].values() for item in sublist])
)
choice_all = "all"

# cli ------------------------------------------------

# parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    dest="cohort",
    type=str,
    choices=choices_cohort,
    help="Cohort on which to perform checks",
    required=True,
)
parser.add_argument(
    "-s",
    dest="site",
    type=str,
    default=choice_all,
    choices=[choice_all] + choices_site,
    help="Site on which to perform checks",
    required=False,
)
parser.add_argument(
    "-r",
    dest="report",
    type=str,
    choices=choices_report,
    help="Report to generate",
    required=True,
)
parser.add_argument(
    "-n",
    dest="number",
    type=int,
    help="Reference number of check to run individually within report",
    required=False,
)
parser.add_argument(
    "-l",
    dest="level",
    type=str,
    default=choice_all,
    choices=[choice_all] + choices_level,
    help="Level of priority of checks to run",
    required=False,
)
parser.add_argument(
    "-o",
    dest="overview",
    action="store_true",
    default=False,
    help="Display overview of parameters and checks to be performed but do not execute",
)
parser.add_argument(
    "-u",
    dest="save_to_synapse",
    action="store_true",
    default=False,
    help="Save report log file to pre-specified Synapse folder",
)
parser.add_argument(
    "-v",
    dest="verbose",
    action="store_true",
    default=False,
    help="Display messages on script progress to the user",
)
# extract command line arguments
args = parser.parse_args()
report = args.report
number = args.number
level = args.level
cohort = args.cohort
sites = args.site
verbose = args.verbose
save_synapse = args.save_to_synapse
overview = args.overview

# setup ------------------------------------------------------------------------


# synapse login
syn = synapseclient.login()

# update config
config = update_config_for_comparison_report(syn, config)
config = update_config_for_release_report(syn, config)

# check user input -------------------------------------------------------------

if number is not None:
    # number should be relevant for report
    check_nos = config["report"][report]
    if number not in check_nos:
        msg1 = f"check number '{number}' is not applicable for report type '{report}'. "
        msg2 = f"To view applicable check numbers, run the following: "
        msg3 = f"'python genie-bpc-quac.py -c {cohort} -r upload -o'"
        raise ValueError(msg1 + msg2 + msg3)

    # number should be relevant to number
    check_no_level = config["checks"][number]["level"]
    if level not in [choice_all, check_no_level]:
        msg1 = f"check number '{number}' is not applicable for report type '{report}' and level '{level}'. "
        msg2 = f"To view applicable check numbers, run the following: "
        msg3 = f"'python genie-bpc-quac.py -c {cohort} -r upload -l {level} -o'"
        raise ValueError(msg1 + msg2 + msg3)

# for release report, ensure previous release is available for comparison
if report == "release" and config["release"][cohort]["previous"] is None:
    msg1 = f"cohort {cohort} does not have a previous release. "
    msg2 = f"To view other available cohorts, run the following: "
    msg3 = f"'python genie-bpc-quac.py -h'"
    raise ValueError(msg1 + msg2 + msg3)

# for comparison report, ensure previous table version is available
if report == "comparison" and config["comparison"][cohort]["previous"] is None:
    msg1 = f"cohort {cohort} does not have a previous table version for comparison. "
    msg2 = f"To view other available cohorts, run the following: "
    msg3 = f"'python genie-bpc-quac.py -h'"
    raise ValueError(msg1 + msg2 + msg3)

# for any report, site must be relevant for cohort
if sites != choice_all and sites not in config["uploads"][cohort]:
    msg1 = f"Site '{sites}' does not contribute data for the '{cohort}' cohort. "
    msg2 = f"Valid sites for the '{cohort}' cohort: "
    msg3 = f"{', '.join(config['uploads'][cohort])}"
    raise ValueError(msg1 + msg2 + msg3)

# parameter messaging ----------------------------------------

if verbose:
    print("Parameters: ")
    print(f"- cohort:\t\t{cohort}")
    print(f"- site(s):\t\t{sites}")
    print(f"- report:\t\t{report}")
    if report == "comparison":
        print(f"  - previous:\t\t{config['comparison'][cohort]['previous']}")
        print(f"  - current:\t\t{config['comparison'][cohort]['current']}")
    if report == "release":
        print(f"  - previous:\t\t{config['release'][cohort]['previous']}")
        print(f"  - current:\t\t{config['release'][cohort]['current']}")
    print(f"- level:\t\t{level}")
    if number is not None:
        print(f"- number:\t\t{number}")
    print(f"- overview:\t\t{overview}")
    print(f"- verbose:\t\t{verbose}")
    print(f"- save on synapse:\t{save_synapse}")

# level checks ----------------------------------------

# storage
valid_levels = [level]
check_nos = []
res = []
outfile = ""
n_issue = 0

# format input
if level == choice_all:
    valid_levels = choices_level
valid_levels = ["fail"] + valid_levels
if number is None:
    check_nos = config["report"][report]
else:
    check_nos = [number]

if report in ["upload", "masking"] and sites == choice_all:
    sites = list(
        set(config["uploads"][cohort].keys()) - set(config["constants"]["sage"])
    )
else:
    site = sites

# collect relevant check functions
check_nos_valid = [
    check_no
    for check_no in check_nos
    if config["checks"][check_no]["deprecated"] == 0
    and config["checks"][check_no]["implemented"] == 1
    and config["checks"][check_no]["level"] in valid_levels
]
check_labels = [config["checks"][check_no]["label"] for check_no in check_nos_valid]
check_level = [config["checks"][check_no]["level"] for check_no in check_nos_valid]
check_fxns = get_check_functions(check_labels)

# TODO fix this
# if sites == choice_all:
#     sites = list(
#         set(config["uploads"][cohort].keys()) - set(config["constants"]["sage"])
#     )
if isinstance(sites, str):
    sites = [sites]

if overview:
    print(f"Checks ({len(check_fxns)}):")
    for check_no in check_nos_valid:
        print(
            f"- {config['checks'][check_no]['level']} {str(check_no).zfill(2)} ({config['checks'][check_no]['label']}): {config['checks'][check_no]['description']} {config['checks'][check_no]['action']}"
        )
else:
    if check_fxns:
        for site in sites:
            # run each applicable QA check
            res = pd.DataFrame()
            for index, key in enumerate(check_fxns.keys()):
                fxn = check_fxns[key]
                if verbose:
                    print(
                        f"{datetime.datetime.now()}: Checking '{key}' for cohort '{cohort}' and site '{site}'..."
                    )

                res_check = fxn(
                    config=config,
                    cohort=cohort,
                    site=None if site == choice_all else site,
                    report=report,
                )
                # TODO: make sure all return types are the same from checklist functions
                if isinstance(res_check, list) and len(res_check) > 0:
                    res_check = pd.concat(res_check)
                if res_check is None or len(res_check) == 0:
                    res_check = pd.DataFrame()
                if verbose:
                    # print(res_check)
                    # HACK res_check.isna().all().all() doesn't look great
                    # TODO Make this if statement more refined
                    print(
                        f" --> {0 if res_check is None or len(res_check) == 0 or res_check.empty or res_check.isna().all().all() else len(res_check)} {check_level[index]}(s) identified"
                    )
                res = pd.concat([res, res_check])
                res.to_csv('test.csv')
                # check for flagged fail check
                if (
                    check_level[index] == "fail"
                    and res_check is not None
                    and len(res_check) > 0
                ):
                    raise Exception("Fail check flagged")

            # write all issues to file
            outfile = f"{cohort}_{site}_{report}_{level}.csv".lower()
            if not res.empty:
                # TODO: fix this
                # issue_no = list(range(1, len(res) + 1))
                # pd.concat(
                #     [pd.DataFrame(issue_no, columns=["issue_no"]), pd.DataFrame(res)],
                #     axis=1,
                # ).to_csv(outfile, index=False)
                res.to_csv(outfile, index=False)
            else:
                with open(outfile, "w") as f:
                    f.write(f"No {level}s triggered.  Congrats! All done!")

            # print number of detected issues
            n_issue = 0 if len(res) == 0 else len(res)
            if verbose and os.path.exists(outfile):
                if level == choice_all:
                    print(
                        f"{datetime.datetime.now()}: Issues ({n_issue}) written to {outfile}"
                    )
                else:
                    print(
                        f"{datetime.datetime.now()}: {level.capitalize()}s ({n_issue}) written to {outfile}"
                    )

            if save_synapse and os.path.exists(outfile):
                synid_folder_output = config["output"][cohort]

                synid_file_output = save_to_synapse(
                    path=outfile,
                    parent_id=synid_folder_output,
                    prov_name="GENIE BPC QA log",
                    prov_desc=f"GENIE BPC QA {report} {level} log for cohort '{cohort}' and site '{site}'",
                    prov_used=None,
                    prov_exec="https://github.com/hhunterzinck/genie-bpc-quac/blob/develop/genie-bpc-quac.R",
                )
                syn.setAnnotations(
                    synid_file_output,
                    annotations={
                        "cohort": cohort,
                        "site": site,
                        "level": level,
                        "report": report,
                        "issueCount": int(n_issue),
                    },
                )
                os.remove(outfile)

                if verbose:
                    print(
                        f"{datetime.datetime.now()}: Saved log to Synapse at '{outfile}' ({synid_file_output})"
                    )
    else:
        if verbose:
            print(
                f"{datetime.datetime.now()}: No applicable {level}-level checks to perform."
            )

# wrap-up ----------------------------------------------------------------------

# finish
toc = time.time()
if verbose:
    print(f"{datetime.datetime.now()}: Runtime: {round(toc - tic)} s")
