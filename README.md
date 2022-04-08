# GENIE BPC Quality Assurance Checklist

## Overview

This repository provides a command line tool for checking raw data, intermediate files, and final data releases for the GENIE Biopharma Collaborative (BPC) project.  

## Installation

Clone this repository and navigate to the directory:

```
git clone git@github.com:Sage-Bionetworks/genie-bpc-quac.git
cd genie-bpc-quac/
```

For installation with Docker: 

```
docker build -t genie-bpc-quac .
```

For install without Docker, install Synapser and other required packages:
```
R -e 'install.packages("synapser", repos = c("http://ran.synapse.org", "http://cran.fhcrc.org"))'
R -e 'renv::restore()'
```

## Usage 

Make sure to cache your Synapse personal access token (PAT) as an environmental variable:

```
export SYNAPSE_AUTH_TOKEN={your_personal_access_token_here}
```

To run with Docker:

```
docker run --rm genie-bpc-quac -h
```

To run without Docker:

```
Rscript genie-bpc-quac.R -h
```

The command line interface will display as follows: 

```
usage: genie-bpc-quac.R [-h] -c {BLADDER,BrCa,CRC,NSCLC,PANC,Prostate}
                        [-s {all,DFCI,MSK,UHN,VICC}] -r
                        {comparison,masking,release,table,upload} [-n NUMBER]
                        [-l {all,error,warning}] [-o] [-u] [-v]
                        [-a SYNAPSE_AUTH]

optional arguments:
  -h, --help            show this help message and exit
  -c {BLADDER,BrCa,CRC,NSCLC,PANC,Prostate}
                        Cohort on which to perform checks
  -s {all,DFCI,MSK,UHN,VICC}
                        Site on which to perform checks
  -r {comparison,masking,release,table,upload}
                        Report to generate
  -n NUMBER             Reference number of check to run individually within
                        report
  -l {all,error,warning}
                        Level of priority of checks to run
  -o                    Display overview of parameters and checks to be
                        performed but do not execute
  -u                    Save report log file to pre-specified Synapse folder
  -v                    Display messages on script progress to the user
  -a SYNAPSE_AUTH       Path to .synapseConfig file or Synapse PAT (default:
                        '~/.synapseConfig')
```

Example command line with Docker:

```
docker run --rm genie-bpc-quac -c {cohort} -s {site} -r upload -l error -v -a $SYNAPSE_AUTH_TOKEN
```

Example command line without Docker:

```
Rscript genie-bpc-quac.R -c {cohort} -s {site} -r upload -l error -v -a $SYNAPSE_AUTH_TOKEN
```

## Reports

### upload

Upload reports run quality checks on the data uploaded by individual sites.  

To see an overview of error level checks implemented in the upload report:
```
Rscript genie-bpc-quac.R -r upload -c {cohort} -s {site} -l error -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs:

```
Checks (23):
- fail 44 (file_not_csv): The file does not appear to be a CSV file. Please check the file format and ensure the correct data was uploaded.
- fail 45 (data_header_col_mismatch): The number of columns in the data and header files do not match. Please check the data and header files have the same corresponding columns.
- fail 49 (quac_required_column_missing): File must contain column but column not found. Please ensure this column is included.
- error 03 (patient_added): Patient ID added. Please confirm that patient was meant to be added
- error 07 (empty_row): Row entries are all missing. Please remove row from upload file.
- error 08 (missing_sample_id): Sample ID is missing. Please fill in missing sample ID.
- error 12 (col_data_type_sor_mismatch): Column data type does not match data type specified in scope of release. Please check values for column and ensure that they match the appropriate data type.
- error 13 (col_entry_data_type_sor_mismatch): Entry data type does not match data type specified in scope of release. Please check value for column and ensure that it matches the appropriate data type.
- error 14 (no_mapped_diag): Mapped diagnosis is missing. Please fill in missing diagnosis.
- error 17 (sample_not_in_main_genie): Sample ID does not match any ID on the main GENIE sample list. Please correct or remove sample ID.
- error 18 (patient_not_in_main_genie): Patient ID does not match any ID on the main GENIE patient list. Please correct or remove patient ID.
- error 20 (col_entry_datetime_format_mismatch): Timestamp value is not formatted correctly. Please correct the format of the timestamp to 'YYYY-mm-dd HH:MM:SS'.
- error 22 (col_entry_date_format_mismatch): Date value is not formatted correctly. Please correct the format of the date to 'YYYY-mm-dd'.
- error 23 (col_empty_but_required): Column values are all missing for required column. Please fill in required data, if applicable.
- error 26 (patient_marked_removed_from_bpc): Patient has been marked as removed from BPC. Please remove the patient and all associated data from the dataset.
- error 27 (sample_marked_removed_from_bpc): Sample has been marked as removed from BPC. Please remove the sample and all associated data from the dataset.
- error 42 (required_not_uploaded): Required variable was not uploaded. Please add the required variable to the dataset.
- error 48 (cpt_sample_type_numeric): cpt_sample_type is numeric instead of text. Please replace numeric value with appropriate text label.
- error 50 (invalid_choice_code): The selected value does not match value choices in the data dictionary. Please confirm selected value matches a valid choice.
- error 51 (less_than_adjusted_target): Case count is less than adjusted target count. Please confirm case count and submit any retracted samples.
- error 52 (greater_than_adjusted_target): Case count is greater than adjusted target count. Please confirm case count.
- error 54 (patient_removed_not_retracted): Patient ID removed but not retracted. Please confirm that patient was meant to be removed and submit via retraction form.
- error 55 (sample_removed_not_retracted): Sample ID removed but not retracted. Please confirm that sample was meant to be removed and submit via retraction form.
```

To see an overview of warning level checks implemented in the upload report:
```
Rscript genie-bpc-quac.R -r upload -c {cohort} -s {site} -l warning -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs:

```
Checks (15):
- fail 44 (file_not_csv): The file does not appear to be a CSV file. Please check the file format and ensure the correct data was uploaded.
- fail 45 (data_header_col_mismatch): The number of columns in the data and header files do not match. Please check the data and header files have the same corresponding columns.
- fail 49 (quac_required_column_missing): File must contain column but column not found. Please ensure this column is included.
- warning 04 (patient_removed): Patient ID removed. Please confirm that patient was meant to be removed
- warning 05 (sample_added): Sample ID added. Please confirm that sample was meant to be added
- warning 06 (sample_removed): Sample ID removed. Please confirm that sample was meant to be removed
- warning 01 (col_import_template_added): Column found in file upload but not import template. Please remove column from upload file.
- warning 02 (col_import_template_removed): Column found in import template but not file upload. Please confirm that column has no relevant data; otherwise, include column in upload.
- warning 09 (col_empty): Column values are all missing. Please confirm that site has no data for the column.
- warning 19 (col_data_datetime_format_mismatch): Column timestamps are not formatted correctly. Please correct the format of the timestamps to 'YYYY-mm-dd HH:MM:SS'.
- warning 21 (col_data_date_format_mismatch): Column dates are not formatted correctly. Please correct the format of the dates to 'YYYY-mm-dd'.
- warning 28 (patient_count_too_small): Current case count is less than target count. Please add cases to meet target.
- warning 33 (irr_sample): Sample ID looks like an IRR sample. Please remove sample if IRR analyses are no longer relevant.
- warning 34 (irr_patient): Patient ID looks like an IRR patient. Please remove patient if IRR analyses are no longer relevant.
- warning 47 (patient_count_too_large): Current case count is greater than target count. Please remove cases to meet target.
```

### masking

Masking reports run drug masking quality checks on the data uploaded by individual sites.  

To see an overview of error level checks implemented in the masking report:
```
Rscript genie-bpc-quac.R -r masking -c {cohort} -s {site} -l error -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs:
```
Checks (5):
- fail 44 (file_not_csv): The file does not appear to be a CSV file. Please check the file format and ensure the correct data was uploaded.
- fail 45 (data_header_col_mismatch): The number of columns in the data and header files do not match. Please check the data and header files have the same corresponding columns.
- error 29 (investigational_drug_duration): Investigational drug duration is greater than 1 day. Please adjust start and end dates of drug duration to be the same day.
- error 30 (investigational_drug_other_name): Investigational drug has the other drug column filled in. Please remove the name in the other drug column.
- error 31 (investigational_drug_not_ct): Investigational drug is not marked as being in a clinical trial. Please indicate that the investigational drug is in a clinical trial.
```

To see an overview of warning level checks implemented in the masking report:
```
Rscript genie-bpc-quac.R -r masking -c {cohort} -s {site} -l warning -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs:
```
Checks (4):
- fail 44 (file_not_csv): The file does not appear to be a CSV file. Please check the file format and ensure the correct data was uploaded.
- fail 45 (data_header_col_mismatch): The number of columns in the data and header files do not match. Please check the data and header files have the same corresponding columns.
- warning 32 (drug_not_fda_approved): Drug does not have a valid associated FDA approval year in HemOnc. Please review the drug instance and determine if the drug name needs to be masked.
- warning 43 (ct_drug_not_investigational): Drug on a clinical trial regimen is not masked as an Investigational Drug. Please mask the drug name with the Investigational Drug label.
```

### table

Table reports run checks on the data once it has been merged and uploaded from individual data files into Synapse tables.  

To see an overview of error level checks implemented in the table report:
```
Rscript genie-bpc-quac.R -r table -c {cohort} -s {site} -l error -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs:
```
Checks (8):
- error 07 (empty_row): Row entries are all missing. Please remove row from upload file.
- error 12 (col_data_type_sor_mismatch): Column data type does not match data type specified in scope of release. Please check values for column and ensure that they match the appropriate data type.
- error 13 (col_entry_data_type_sor_mismatch): Entry data type does not match data type specified in scope of release. Please check value for column and ensure that it matches the appropriate data type.
- error 14 (no_mapped_diag): Mapped diagnosis is missing. Please fill in missing diagnosis.
- error 25 (col_sor_not_table): Column found in the scope of release but not the table. Please confirm that column has no relevant data; otherwise, include column in table.
- error 26 (patient_marked_removed_from_bpc): Patient has been marked as removed from BPC. Please remove the patient and all associated data from the dataset.
- error 27 (sample_marked_removed_from_bpc): Sample has been marked as removed from BPC. Please remove the sample and all associated data from the dataset.
- error 53 (character_double_value): Column contains characters converted to doubles. Please check to ensure values match uploaded data.
```

To see an overview of warning level checks implemented in the table report:
```
Rscript genie-bpc-quac.R -r table -c {cohort} -s {site} -l warning -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs:
```
Checks (3):
- warning 09 (col_empty): Column values are all missing. Please confirm that site has no data for the column.
- warning 24 (col_table_not_sor): Column found in table but not the scope of release. Please remove column from table.
- warning 35 (col_empty_site_not_others): Column is empty for site but not for other sites. Please confirm that site has no data for the column.
```

### comparison

Comparison reports compare the current version of the data in the Synapse tables to the version of the same tables representing the previous data upload for the cohort, if applicable.   

To see an overview of error level checks implemented in the comparison report:
```
Rscript genie-bpc-quac.R -r comparison -c {cohort} -s {site} -l error -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs: 
```
Checks (3):
- error 03 (patient_added): Patient ID added. Please confirm that patient was meant to be added
- error 36 (col_five_perc_inc_missing): Column had a greater than 5% increase in missingness since last upload. Please ensure that 5% increase in missing values is expected.
- error 38 (col_removed): Column removed since last upload. Please ensure that column was intentionally removed.
```

To see an overview of warning level checks implemented in the comparison report:
```
Rscript genie-bpc-quac.R -r comparison -c {cohort} -s {site} -l warning -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs: 
```
Checks (8):
- warning 04 (patient_removed): Patient ID removed. Please confirm that patient was meant to be removed
- warning 05 (sample_added): Sample ID added. Please confirm that sample was meant to be added
- warning 06 (sample_removed): Sample ID removed. Please confirm that sample was meant to be removed
- warning 15 (rows_added): Row added. Please confirm that row was meant to be added.
- warning 16 (rows_removed): Row removed. Please confirm that row was meant to be removed.
- warning 37 (col_five_perc_dec_missing): Column had a greater than 5% decrease in missingness since last upload. Please ensure that 5% decrease in missing values is expected.
- warning 39 (col_added): Column added since last upload. Please ensure that column was intentionally added.
- warning 40 (file_added): File added since last release. Please ensure that file was intentionally added.
```

### release

Release reports compare the current and previous final data release files, if applicable.  

To see an overview of error level checks implemented in the release report:
```
Rscript genie-bpc-quac.R -r release -c {cohort} -s {site} -l error -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs:
```
Checks (4):
- error 03 (patient_added): Patient ID added. Please confirm that patient was meant to be added
- error 36 (col_five_perc_inc_missing): Column had a greater than 5% increase in missingness since last upload. Please ensure that 5% increase in missing values is expected.
- error 38 (col_removed): Column removed since last upload. Please ensure that column was intentionally removed.
- error 41 (file_removed): File removed since last release. Please ensure that file was intentionally removed.
```

To see an overview of warning level checks implemented in the release report:
```
Rscript genie-bpc-quac.R -r release -c {cohort} -s {site} -l warning -v -a $SYNAPSE_AUTH_TOKEN -o
```

which outputs:

```
Checks (8):
- warning 04 (patient_removed): Patient ID removed. Please confirm that patient was meant to be removed
- warning 05 (sample_added): Sample ID added. Please confirm that sample was meant to be added
- warning 06 (sample_removed): Sample ID removed. Please confirm that sample was meant to be removed
- warning 15 (rows_added): Row added. Please confirm that row was meant to be added.
- warning 16 (rows_removed): Row removed. Please confirm that row was meant to be removed.
- warning 37 (col_five_perc_dec_missing): Column had a greater than 5% decrease in missingness since last upload. Please ensure that 5% decrease in missing values is expected.
- warning 39 (col_added): Column added since last upload. Please ensure that column was intentionally added.
- warning 40 (file_added): File added since last release. Please ensure that file was intentionally added.
```

## Adding new QA checks

The QuAC framework is built in modular units for extensibility.  If a new QA check needs to be added, implement the following steps:

1. In the `config.yaml` under the `checks` key, add another element using the next available index number.  The check element metadata should consist of the following information: 
```
checks:
  {check_no}:
      implemented: {0 or 1 to indicate whether the check is currently implemented}
      deprecated: {0 or 1 to indicate whether the check is currently deprecated}
      level: {error, warning, or fail to indicate check level}
      label: {label for the check that should match the function name implementing the check}
      description: {human-readable description of what the check is examining}
      action: {human-readable description of the request to fix the issue detected by the check}
```

2. In `checklist.R`, implement the check as a function with the following format:
```
{label} <- function(cohort, site, report, output_format = "log") {
  output <- NULL
  {implementation}
  output <- format_output({values}, 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = {column_name}, 
                            synid = {file synapse id},
                            check_no = {check_no},
                            infer_site = F)
  return(output)
}
```

3. Add the `check_no` to appopriate report check list `config.yaml` under the `report` key.  

The above steps will incorporate the novel check into the program when the user requests the respective `report` and `level`.  
