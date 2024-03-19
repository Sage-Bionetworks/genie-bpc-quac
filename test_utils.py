# Description: tests for the BPC QA function checks, wrappers, and helpers.
# Author: Haley Hunter-Zinck
# Date: June 22, 2021

# setup ------------------------------------------------------------------------

import pytest
import yaml
import synapseclient
import pandas as pd

syn = synapseclient.Synapse()
syn.login()

# Import functions from your Python files
# from checklist import *
# from fxns import *

# global variables
with open("config.yaml", "r") as stream:
    config = yaml.safe_load(stream)

# local files
file_csv = "test.csv"

# fixtures -----------------------------------------------------------------------

# version 1
cols = [
    synapseclient.table.Column(name="Name", columnType="STRING", maximumSize=20),
    synapseclient.table.Column(name="Chromosome", columnType="STRING", maximumSize=20),
    synapseclient.table.Column(name="Start", columnType="INTEGER"),
    synapseclient.table.Column(name="End", columnType="INTEGER"),
    synapseclient.table.Column(
        name="Strand", columnType="STRING", enumValues=["+", "-"], maximumSize=1
    ),
    synapseclient.table.Column(name="TranscriptionFactor", columnType="BOOLEAN"),
]

genes_v1 = pd.DataFrame(
    {
        "Name": ["foo", "arg", "zap", "bah", "bnk", "xyz"],
        "Chromosome": [1, 2, 2, 1, 1, 1],
        "Start": [12345, 20001, 30033, 40444, 51234, 61234],
        "End": [126000, 20200, 30999, 41444, 54567, 68686],
        "Strand": ["+", "+", "-", "-", "+", "+"],
        "TranscriptionFactor": [False, False, False, False, True, False],
    }
)

genes_v2 = genes_v1.copy()
genes_v2["Chromosome"] = genes_v2["Chromosome"] + 1


# tests
def test_get_main_genie_ids():
    # replace with your actual function and expected output
    result = get_main_genie_ids(
        synid_table_sample=config["synapse"]["genie_sample"]["id"],
        patient=True,
        sample=True,
    )
    assert result.shape[1] == 2

    result = get_main_genie_ids(
        synid_table_sample=config["synapse"]["genie_sample"]["id"],
        patient=True,
        sample=False,
    )
    assert result.shape[1] == 1

    result = get_main_genie_ids(
        synid_table_sample=config["synapse"]["genie_sample"]["id"],
        patient=False,
        sample=True,
    )
    assert result.shape[0] > 100000

    result = get_main_genie_ids(
        synid_table_sample=config["synapse"]["genie_sample"]["id"],
        patient=True,
        sample=True,
    )
    assert result.shape[0] >= 100000

    result = get_main_genie_ids(
        synid_table_sample=config["synapse"]["genie_sample"]["id"],
        patient=False,
        sample=False,
    )
    assert result is None
