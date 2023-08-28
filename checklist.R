# Description: checklist of QA functions for the GENIE BPC QA process.
# Author: Haley Hunter-Zinck
# Date: July 1, 2021

# global variables ------------------------------------

workdir = "."
if (!file.exists("config.yaml")) {
  workdir <- "/usr/local/src/myscripts"
}
config <- read_yaml(glue("{workdir}/config.yaml"))

# list of functions ------------------------------------

#' Gather all functions corresponding to numbered checks.  
#'
#' @labels Labels corresponding to checks
#' @return List containing implemented check functions in addition
#' to associated metadata.
#' @example
#' get_check_functions(c(1,2,3,4,5))
get_check_functions <- function(labels) {

  fxns <- list()
  for (label in labels) {
    fxns[[label]] <- get(label)
  }
  return(fxns)
}

# config updates ------------------

#' Update the configuration global variables with information 
#' from external references for the comparison report.
#' 
#' @param config Raw configuration object loaded from config.yaml
#' @return Configuration object augmented by information for external references
update_config_for_comparison_report <- function(config) {
  # update for comparison reports
  query <- glue("SELECT * FROM {config$synapse$version_date$id}")
  comp <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  
  config$comparison <- list()
  for (i in 1:nrow(comp)) {
    config$comparison[[comp[i,"cohort"]]] <- list(previous = comp[i,"previous_date"], current = comp[i,"current_date"])
  }
  
  return(config)
}

#' Update the configuration global variables with information 
#' from external references for the release report.
#' 
#' @param config Raw configuration object loaded from config.yaml
#' @return Configuration object augmented by information for external references
update_config_for_release_report <- function(config) {
  # update for release comparisons
  query <- glue("SELECT * FROM {config$synapse$version_release$id} WHERE current = 'true'")
  rel <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  
  config$release <- list()
  for (i in 1:nrow(rel)) {
    previous <- NULL
    if (!is.na(rel[i,"previous_version"])) {
      previous <- glue("{rel[i,'previous_version']}-{rel[i,'previous_type']}")
    }
    current <- glue("{rel[i,'release_version']}-{rel[i,'release_type']}")
    config$release[[rel[i,"cohort"]]] <- list(previous = previous, current = current)
  }
  
  return(config)
}

# summary functions ------------------------------------

#' Format output of check functions according to requested format.
#'
#' @param value vector of problematic values
#' @param output_format format in which to return problematic values
#' @param cohort cohort over which the check is performed
#' @param site site over which the check is performed
#' @param synid synapse ID of the file over which the check is perfomed
#' @param column_name name of the column from which the problematic values
#' were drawn
#' @param check_no check reference number
#' @return formatted output of problematic values:
#'           log: one row per value formatted as cohort,site, synid,
#'           synid_name, column_name, value,
#'                 check_no, description, action
format_output <- function(value, output_format = "log",
                          cohort = NA, site = NA,  synid = NA,
                          patient_id = NA, instrument = NA, instance = NA, 
                          column_name = NA, check_no = NA, infer_site = F) {

  if (length(value) == 0) {
    return(NULL)
  }

  res <- c()

  if (output_format == "log") {

    # get main input file data
    if (is.na(synid)) {
      cat("Warning: no file Synapse ID specified.  Setting file name to NA. \n")
      synid_name <- NA
    } else {
      synid_name <- paste0(get_synid_file_name(synid), collapse = ";")
    }

    # get check metadata info
    if (is.na(check_no)) {

      cat("Warning: no check number specified for logging.  Returning NAs \
          for description and action. \n")
      ref <- setNames(c(NA, NA), c("description", "action"))

    } else {

      ref <- setNames(c(config$checks[[check_no]]$description, config$checks[[check_no]]$action),
                      c("description", "action"))
    }

    # setup output row for error logging
    labels <- c("cohort", "site", "synapse_id", "synapse_name",
                "patient_id", "instrument", "instance",
                "column_name", "value", "check_no",
                "description", "request")
    
    # extract site from value, when appropriate
    if(is.na(site) && infer_site) {
      site <- unlist(lapply(lapply(strsplit(value, split = "-"), head, n = 2), tail, n = 1))
    }

    if (length(value) > 1) {
      res <- matrix(cbind(cohort, site, synid, synid_name,
                          patient_id, instrument, instance,
                          column_name, value, check_no,
                          ref["description"], ref["action"]),
                    nrow = length(value), dimnames = list(c(), labels))
    } else {
      res <- matrix(c(cohort, site, synid, synid_name,
                      patient_id, instrument, instance,
                      column_name, value, check_no,
                          ref["description"], ref["action"]),
                    nrow = length(value), dimnames = list(c(), labels))
    }

  } else {

    # if output format not recognized, just return the
    res <- value
  }

  return(res)
}

get_row_key <- function(idx, data, column_names = c(config$column_name$patient_id,
                                                    config$column_name$instrument,
                                                    config$column_name$instance)) {
  
  if (length(idx)) {
    value <- apply(as.matrix(data[idx,column_names, drop = F]), 
                   1, paste0, collapse = "|")
    column_name <- paste0(column_names, collapse = "|")
  } else {
    value <- c()
    column_name <- c()
  }
  
  return(list(value = value, column_name = column_name))
}

# main genie functions ------------------------------

get_main_genie_ids <- function(synid_table_sample, patient = T, sample = T) {
  
  if (patient & sample) {
    query <- glue("SELECT PATIENT_ID, SAMPLE_ID FROM {synid_table_sample}")
  } else if(patient) {
    query <- glue("SELECT DISTINCT PATIENT_ID FROM {synid_table_sample}")
  } else if (sample) {
    query <- glue("SELECT SAMPLE_ID FROM {synid_table_sample}")
  } else {
    return(NULL)
  }
  
  res <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  
  return(res)
}

# bpc functions -------------------------------------

#' Get Synapse IDs of the most recent PRISSMM documentation files
#' for a cohort.
#'
#' @param synid_table_prissmm Synapse ID of the table containing Synapse IDs
#' of all PRISSM documentation
#' @param cohort cohort of interest
#' @file_name name of the type of documentation file of interest
#' @return Synapse ID of the most recent PRISSMM documentation corresponding
#' to the cohort and type of document.
#' @example
#' get_bpc_synid_prissmm("syn12345", "my_cohort", "Import Template")
get_bpc_synid_prissmm <- function(synid_table_prissmm, cohort,
        file_name = c("Import Template", "Data Dictionary non-PHI")) {

  query <- glue("SELECT id FROM {synid_table_prissmm} \
                WHERE cohort = '{cohort}' \
                ORDER BY name DESC LIMIT 1")
  synid_folder_prissmm <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  synid_prissmm_children <- as.list(synGetChildren(synid_folder_prissmm))
  synid_prissmm_children_ids <- setNames(unlist(lapply(synid_prissmm_children,
                                          function(x) {return(x$id)})),
                                        unlist(lapply(synid_prissmm_children,
                                          function(x) {return(x$name)})))
  
  return(as.character(synid_prissmm_children_ids[file_name]))
}

#' Return a file upload data corresponding to a cohort-site pair as an R object.
#'
#' @param cohort cohort of interest
#' @param site site of interest
#' @param obj upload file object from config
#' @return Contents of a cohort-side data upload file as an R data frame.
#' @example
#' get_bpc_data_upload(cohort, site, list(data1 = "syn12345", data2 = "syn6554332", 
#'                           header1 = "syn39857289375, header2 = "syn11111"),
#' get_bpc_data_upload(cohort, site, list(data1 = "syn12345"))
get_bpc_data_upload <- function(cohort, site, report, 
                                synid_file_data1, 
                                synid_file_data2 = NULL, 
                                synid_file_header1 = NULL, 
                                synid_file_header2 = NULL) {

  data <- c()
  data1 <- c()
  data2 <- c()

  # get data 1 (default, should always be specified)
  ent <- synGet(synid_file_data1)
  data1 <- read.csv(ent$path, check.names = F,
                    na.strings = c(""), 
                    stringsAsFactors = F,
                    colClasses = "character")

  # check for header1
  if (length(synid_file_header1)) {
    ent <- synGet(synid_file_header1)
    colnames(data1) <- as.character(read.csv(ent$path, check.names = F,
                                             na.strings = c(""),
                                             stringsAsFactors = F,
                                             colClasses = "character"))
  }

  # check for data 2
  if (length(synid_file_data2)) {
    ent <- synGet(synid_file_data2)
    data2 <- read.csv(ent$path, check.names = F,
                      na.strings = c(""), 
                      stringsAsFactors = F,
                      colClasses = "character")
  }

  # check for header2
  if (length(synid_file_header2)) {
    ent <- synGet(synid_file_header2)
    colnames(data2) <- as.character(read.csv(ent$path, check.names = F,
                                             na.strings = c(""),
                                             stringsAsFactors = F,
                                             colClasses = "character"))
  }

  if (length(synid_file_data2)) {
    data <- data1 %>% inner_join(data2, by = c("record_id", 
                                               "redcap_repeat_instrument", 
                                               "redcap_repeat_instance"))
  } else {
    data <- data1
  }

  return(data)
}

#' Get version of the previous table corresponding to the given date
#' according to the tracking table.
#' 
#' @param synid_table Synapse ID of the table of interest.
#' @param date_version Date of the previous version of interest.
#' @return Version number of the table corresponding to the date or NA 
#' if no version corresponding to the requested date is given.  
#' @example 
#' get_version_at_date("syn12345", date_version = "2100-01-01")
get_bpc_version_at_date <- function(synid_table, date_version,
                                ts_format = "%Y-%m-%dT%H:%M:%OS", 
                                tz_current = "UTC",
                                tz_target = "US/Pacific") {
  
  rest_cmd <- glue("/entity/{synid_table}/version?limit=50")
  version_history_list = synRestGET(rest_cmd)
  
  version_history = as.data.frame(do.call(rbind, version_history_list$results))
  version_history$date_cut <- sapply(version_history$modifiedOn, get_date_as_string,
                                     ts_format = ts_format, tz_current = tz_current, 
                                     tz_target = tz_target)
  
  idx <- which(version_history$date_cut == date_version)
  if(length(idx) == 0) {
    return(NA)
  }
  version_no <- version_history$versionNumber[idx][[1]]
  
  return(version_no)
}

#' Get data contained within a table for a given cohort at the current or 
#' previous upload. 
#' 
#' @param synid_table Synapse ID of a table
#' @param cohort Label specifying the cohort
#' @param site Label specifying site
#' @param select SQL select clause for querying a table
#' @param previous TRUE to get previous version; FALSE for current
#' @return DataFrame
get_bpc_data_table <- function(synid_table, cohort, site = NA, select = NA,
                               previous = F) {
  
  version_no <- NA
  
  if(previous) {
    
    version_date <- config$comparison[[cohort]]$previous
    version_no <- get_bpc_version_at_date(synid_table = synid_table, 
                                      date_version = version_date)
  } 
  
  if(is.na(site)) {
    data <- get_data_filtered(synid_table, 
                              column_name = 'cohort', 
                              col_value = cohort,
                              select = select,
                              version = version_no, exact = T)
  } else {
    data <- get_data_filtered(synid_table, 
                              column_name = c('cohort', 'record_id'), 
                              col_value = c(cohort, glue("-{site}-")),
                              select = select,
                              version = version_no, exact = c(T, F))
  }
  
  
  return(data)
}

get_bpc_data_release <- function(cohort, 
                                site, 
                                version,
                                file_name) {
  
  path <- glue("{cohort}/{version}/{cohort}_{version}_clinical_data/{file_name}")
  synid_file <- get_file_synid_from_path(synid_folder_root = config$synapse$release$id, 
                                         path = path)
  data <- get_data(synid_file)
  
  if (!is.na(site)) {
    data <- data %>% filter(grepl(pattern = site, x = record_id))
  } 
  
  return(data)
}

get_bpc_data <- function(cohort, site, report, obj = NULL) {
  
  if (is.element(report, c("upload", "masking"))) {
    return(get_bpc_data_upload(cohort, site, 
                               synid_file_data1 = obj$data1,
                               synid_file_data2 = obj$data2,
                               synid_file_header1 = obj$header1,
                               synid_file_header2 = obj$header2))
  }
  
  if (is.element(report, c("table", "comparison"))) {
    return(get_bpc_data_table(cohort = cohort, site = site, 
                              synid_table = obj$synid_table, 
                              select = obj$select,
                              previous = obj$previous))
  }
  
  if (is.element(report, c("release"))) {
    return(get_bpc_data_release(cohort = cohort, 
                                site = site, 
                                version = obj$version, 
                                file_name = obj$file_name))
  }
  
  return(NULL)
}

#' Get patients or sample IDs added or removed between different stages or versions.
#' 
#' @param cohort Name of the cohort
#' @param site Name of the site
#' @param check_patient If TRUE, check patient IDs; otherwise, check sample IDs.
#' @param check_added If TRUE, check IDs added since upload; otherwise, check
#'   IDs removed.
#' @param account_for_retracted if TRUE, account for retracted ids when looking
#'   at IDs removed; otherwise, report all IDs removed regardless of retraction status
#' @return vector of IDs that changed between uploads
get_bpc_patient_sample_added_removed <- function(cohort, site, report, 
                                             check_patient, check_added,
                                             account_for_retracted = F) {
  
  column_name <- ""
  table_name <- ""
  synid_entity_source <- NA
  results <- list()
  retracted <- c()
  
  if (check_patient) {
    column_name <- config$column_name$patient_id
    table_name <- config$table_name$patient_id
    file_name <- config$file_name$patient_file
    
    if (account_for_retracted) {
      retracted <- get_retracted_patients(cohort) 
    }
  } else {
    column_name <- config$column_name$sample_id
    table_name <- config$table_name$sample_id
    file_name <- config$file_name$sample_file
    
    if (account_for_retracted) {
      retracted <- get_retracted_samples(cohort)
    }
  }
  
  if (report == "upload") {
    synid_tables <- get_synid_from_table(config$synapse$tables_view$id,
                                         condition = "double_curated = false",
                                         with_names = T)
    synid_table <- as.character(synid_tables[table_name])
    data_previous <- get_bpc_data(cohort = cohort, site = site, report = "table", 
                                  obj = list(synid_table = synid_table, previous = F, select = NA))
    
    # get upload data excluding IRR cases
    obj_upload <- config$uploads[[cohort]][[site]]
    synid_entity_source <- obj_upload$data1
    data_current_irr <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = obj_upload)
    data_current <- data_current_irr %>%
      filter(!grepl(pattern = "[_-]2$", x = (!!as.symbol(config$column_name$patient_id))))
  } else if (report == "table" || report == "comparison") {
    synid_tables <- get_synid_from_table(config$synapse$tables_view$id,
                                         condition = "double_curated = false",
                                         with_names = T)
    synid_entity_source <- as.character(synid_tables[table_name])
    
    data_current <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                 obj = list(synid_table = synid_entity_source, previous = F, select = NA))
    data_previous <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                  obj = list(synid_table = synid_entity_source, previous = T, select = NA))
  } else if (report == "release") {
    
    version_current <- config$release[[cohort]]$current
    version_previous <- config$release[[cohort]]$previous
    
    path <- glue("{cohort}/{version_current}/{cohort}_{version_current}_clinical_data/{file_name}")
    synid_entity_source <- get_file_synid_from_path(synid_folder_root = config$synapse$release$id, 
                                           path = path)
    
    if (is.na(version_previous)) {
      return(NULL)
    }
    
    data_current <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                 obj = list(version = version_current, file_name = file_name))
    data_previous <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                  obj = list(version = version_previous, file_name = file_name))
  } else {
    return(NULL)
  }
  
  if (check_added) {
    results$ids <- setdiff(setdiff(data_current[[column_name]], data_previous[[column_name]]), NA)
  } else {
    results$ids <- setdiff(setdiff(data_previous[[column_name]], c(retracted, data_current[[column_name]])), NA)
  }
  
  results$column_name = column_name
  results$synid_entity_source = synid_entity_source
  return(results)
}

get_bpc_index_missing_sample <- function(data,
                                          instrument = config$instrument$panel, 
                                          na.strings = c("NA", "")) {
  idx <- which(data[,config$column_name$instrument] == instrument &
                 (is.na(data[,config$column_name$sample_id]) | 
                    is.element(data[,config$column_name$sample_id], na.strings)))
  
  return(idx)
}

get_bpc_instrument_of_variable <- function(variable_name, cohort) {
  
  synid_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                    cohort = cohort,
                                    file_name = "Data Dictionary non-PHI")
  dd <- get_data(synid_dd)
  instrument <- dd$`Form Name`[match(variable_name, dd$`Variable / Field Name`)]
  
  return(instrument)
}

get_bpc_sor_data_type_single <- function(var_name, sor = NULL) {
  
  if (is.null(sor)) {
    sor <- get_data(config$synapse$sor$id, sheet = 2)
  }
  
  if (sor %>% filter(VARNAME == var_name) %>% count() == 0) {
    return(NA)
  }
  
  data_type <- tolower(unique(sor %>%
    filter(VARNAME == var_name) %>%
    select(DATA.TYPE)))
  
  map_dt <- config$maps$data_type
  if (length(which(names(map_dt) == data_type))) {
    return(as.character(map_dt[data_type]))
  }
  
  return(data_type)
}

get_bpc_sor_data_type <- function(var_name, sor = NULL) {
  
  data_types <- c()
  
  for (i in 1:length(var_name)) {
    data_types[i] <- get_bpc_sor_data_type_single(var_name = var_name[i], sor = sor)
  }
  
  return(data_types)
}

get_bpc_table_synapse_ids <- function() {
  query <- glue("SELECT id, name FROM {config$synapse$tables_view$id} WHERE double_curated = 'false' AND table_type = 'data'")
  table_info <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  
  return(setNames(table_info$id, table_info$name))
}

get_bpc_table_instrument <- function(synapse_id) {
  query <- glue("SELECT form FROM {config$synapse$tables_view$id} WHERE double_curated = 'false' AND table_type = 'data' AND id = '{synapse_id}'")
  form <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  
  return(as.character(form))
}

get_bpc_set_view <- function(cohort, report, version = NA) {
  
  view <- NULL
  
  if (report == "table" || report == "comparison") {
    query <- glue("SELECT id, primary_key, form FROM {config$synapse$tables_view$id} WHERE double_curated = 'false' AND table_type = 'data'")
    view <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  } else if (report == "release") {
    
    if (is.na(version)) {
      version <- config$release[[cohort]]$current
    }
    path <- glue("{cohort}/{version}/{cohort}_{version}_clinical_data")
    synid_folder <- get_folder_synid_from_path(synid_folder_root = config$synapse$release$id, 
                                               path = path)
    raw <- get_synapse_folder_children(synapse_id = synid_folder, 
                                       include_types=list("file"))
    view <- data.frame(matrix(cbind(as.character(raw), rep("cohort, record_id, redcap_repeat_instance", length(raw)), names(raw)), 
                              ncol = 3, dimnames = list(c(), c("id", "primary_key", "form"))), stringsAsFactors = F)
  }
  
  return(view)
}

get_bpc_pair <- function(cohort, site, report, synid_entity_source) {
  
  data <- list()
  
  if (report == "table" || report == "comparison") {
    
    data$current <- get_bpc_data(cohort = cohort, site = site, report = report,
                              obj = list(synid_table = synid_entity_source, select = NA, previous = F))
    data$previous <- get_bpc_data(cohort = cohort, site = site, report = report,
                              obj = list(synid_table = synid_entity_source, select = NA, previous = T))
  } else if (report == "release") {
    
    version_current <- config$release[[cohort]]$current
    version_previous <- config$release[[cohort]]$previous
    file_name <- synGet(synid_entity_source, downloadFile = F)$properties$name
    
    data$current <- get_bpc_data(cohort = cohort, site = site, report = report, 
                              obj = list(version = version_current, file_name = file_name))
    data$previous <- get_bpc_data(cohort = cohort, site = site, report = report, 
                              obj = list(version = version_previous, file_name = file_name))
  }
  
  return(data)
}

#' Get year of latest curation date for a cohort.
#' 
#' @param cohort BPC cohort label
#' @return Integer representing the year of lastest curation date.
get_bpc_curation_year <- function(cohort, site) {
  
  synid_table_curation <- config$synapse$curation$id
  query <- glue("SELECT MAX(curation_dt) FROM {synid_table_curation} WHERE cohort = '{cohort}' AND redcap_data_access_group = '{site}'")
  dt <- as.character(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))[1])
  return(substr(dt, 1, 4))
}

#' Get current unique case count without IRR samples in a BPC file represented as a data frame.
#' @param data data frame 
#' @return integer
get_bpc_case_count <- function(data) {
  patient_id <- data[[config$column_name$patient_id]]
  n_total <- length(unique(patient_id))
  n_irr <- length(unique(patient_id[grepl(pattern = "[-_]2$", x = patient_id)]))
  n_current = n_total - n_irr
  
  return(n_current)
}

#' Get list of patients retracted for a cohort
#' @param cohort cohort label of interest
#' @return vector of patient IDs or NULL if no retracted patient IDs
#' @example get_retracted_patients(cohort = "BLADDER")
get_retracted_patients <- function(cohort) {
  query <- glue("SELECT record_id FROM {config$synapse$rm_pat$id} WHERE {cohort} = 'true'")
  retracted <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  if(length(retracted)) {
    return(retracted)
  }
  return(NULL)
}

#' Get list of samples retracted for a cohort
#' @param cohort cohort label of interest
#' @return vector of sample IDs or NULL if no retracted sample IDs
#' @example get_retracted_samples(cohort = "BLADDER")
get_retracted_samples <- function(cohort) {
  query <- glue("SELECT SAMPLE_ID FROM {config$synapse$rm_sam$id} WHERE {cohort} = 'true'")
  retracted <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  if(length(retracted)) {
    return(retracted)
  }
  return(NULL)
}

# hemonc functions ------------------------------------

#' Get HemOnc code from NCIT code from map stored on Synapse.
#' 
#' @param code_ncit Numeric code associated with a concept in the NCIT standard vocabulary.
#' @return Integer corresponding to HemOnc code. 
get_hemonc_from_ncit <- function(code_ncit) {
  synid_table_map <- config$synapse$map$id
  query <- glue("SELECT HemOnc_code FROM {synid_table_map} WHERE NCIT = 'C{code_ncit}'")
  code_hemonc <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  if (!length(code_hemonc)) {
    code_hemonc = NA
  }
  
  return(unique(as.integer(code_hemonc)))
}

#' Get BPC name from NCIT code from map stored on Synapse.
#' 
#' @param code_ncit Numeric code associated with a concept in the NCIT standard vocabulary.
#' @return Integer corresponding to HemOnc code. 
get_bpc_from_ncit <- function(codes_ncit) {
  
  names_bpc <- c()
  synid_table_map <- config$synapse$map$id
  query <- "SELECT BPC FROM {synid_table_map} WHERE NCIT = 'C{code_ncit}'"
  
  for (code_ncit in codes_ncit) {
    name_bpc <- as.character(unlist(as.data.frame(synTableQuery(glue(query), includeRowIdAndRowVersion = F))))
    
    if (!length(name_bpc)) {
      names_bpc = append(names_bpc, NA)
    } else {
      names_bpc = append(names_bpc, name_bpc)
    }
  }
  
  return(names_bpc)
}


#' Get FDA approval year from HemOnc ontology relationships.
#' 
#' @param code_hemonc HemOnc ontology concept
#' @return integer representing year of FDA approval
get_hemonc_fda_approval_year <- function(code_hemonc) {
  
  if (is.na(code_hemonc)) {
    return(NA)
  }
  
  year <- 0
  synid_table_rel <- config$synapse$relationship$id
  synid_table_con <- config$synapse$concept$id
  
  query <- glue("SELECT concept_code_2 FROM {synid_table_rel} WHERE vocabulary_id_1 = 'HemOnc' AND relationship_id = 'Was FDA approved yr' AND concept_code_1 = {code_hemonc}")
  concept_code_2 <- as.integer(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  if (!length(concept_code_2)) {
    return(NA)
  }
  
  for (concept_code in concept_code_2) {
    query <- glue("SELECT concept_name FROM {synid_table_con} WHERE concept_code = {concept_code}")
    year_code <- as.integer(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
    year <- max(year, year_code)
  }
  
  if (!length(year)) {
    return(NA)
  }
  
  return(year)
}

# check functions -------------------------------------

#' Get columns found in file upload but not import template
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format output format for the check
#' @return columns in the uploaded file but not the import template, formatted
#' according to the specified output_format
#' @example
#' col_import_template_added(cohort = "BrCa", site = "MSK",
#' output_format = "log")
col_import_template_added <- function(cohort, site, report,
                                            output_format = "log") {

  obj_upload <- config$uploads[[cohort]][[site]]
  synid_template <- get_bpc_synid_prissmm(config$synapse$prissmm$id, cohort,
                                      file_name = "Import Template")

  data_template <- get_data(synid_template)
  data_upload <- get_bpc_data(cohort = cohort, site = site, 
                              report = report, obj = obj_upload)

  results <- get_columns_added(data_current = data_upload,
                               data_previous = data_template)
  output <- format_output(value = results, cohort = cohort, site = site,
                          output_format = output_format,
                          column_name = results, synid = obj_upload$data1,
                          check_no = 1)

  return(output)
}

#' Get columns found in import template but not file upload
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return columns in the import template but not the file upload, formatted
#' according to the specified output_format
#' @example
#' col_import_template_removed(cohort = "BrCa", site = "MSK",
#' output_format = "log")
col_import_template_removed <- function(cohort, site, report,
                                              output_format = "log") {

  obj_upload <- config$uploads[[cohort]][[site]]
  synid_template <- get_bpc_synid_prissmm(config$synapse$prissmm$id, cohort,
                                      file_name = "Import Template")

  data_template <- get_data(synid_template)
  data_upload <- get_bpc_data(cohort = cohort, site = site, 
                              report = report, obj = obj_upload)

  results <- get_columns_removed(data_current = data_upload,
                                 data_previous = data_template)
  output <- format_output(results, cohort = cohort, site = site,
                          output_format = output_format,
                          column_name = results, synid = obj_upload$data1,
                          check_no = 2)

  return(output)
}

#' Get patient IDs added in the most recent upload
#' relative to the last upload.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format output format for the check
#' @return patient IDs added in the most recent upload, formatted
#' according to the specified output_format
#' @example
#' patient_added(cohort = "BrCa", output_format = "log")
patient_added <- function(cohort, site, report, output_format = "log") {
  
  results <- get_bpc_patient_sample_added_removed(cohort = cohort, site = site, report = report,
                                              check_patient = T, check_added = T)
  output <- format_output(results$ids, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = results$column_name, 
                          synid = results$synid_entity_source,
                          check_no = 3,
                          infer_site = T)
  
  return(output)
}

#' Get patient IDs removed in the most recent upload
#' relative to the last upload.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format output format for the check
#' @return patient IDs removed in the most recent upload, formatted
#' according to the specified output_format
#' @example
#' patient_added(cohort = "BrCa", output_format = "log")
patient_removed <- function(cohort, site, report, output_format = "log") {
  
  results <- get_bpc_patient_sample_added_removed(cohort = cohort, site = site,  report = report,
                                              check_patient = T, check_added = F)
  output <- format_output(results$ids, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = results$column_name, 
                          synid = results$synid_entity_source,
                          check_no = 4,
                          infer_site = T)
  
  return(output)
}

#' Get sample IDs added in the most recent upload
#' relative to the last upload.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return sample IDs added in the most recent upload, formatted
#' according to the specified output_format
#' @example
#' sample_added(cohort = "BrCa", output_format = "log")
sample_added <- function(cohort, site, report, output_format = "log") {
  
  results <- get_bpc_patient_sample_added_removed(cohort = cohort, site = site,  report = report,
                                              check_patient = F, check_added = T)
  output <- format_output(results$ids, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = results$column_name, 
                          synid = results$synid_entity_source,
                          check_no = 5,
                          infer_site = T)
  
  return(output)
}

#' Get sample IDs removed in the most recent upload
#' relative to the last upload.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return sample IDs removed in the most recent upload, formatted
#' according to the specified output_format
#' @example
#' sample_removed(cohort = "BrCa", output_format = "log")
sample_removed <- function(cohort, site, report, output_format = "log") {
  
  results <- get_bpc_patient_sample_added_removed(cohort = cohort, site = site,  report = report,
                                              check_patient = F, check_added = F)
  output <- format_output(results$ids, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = results$column_name, 
                          synid = results$synid_entity_source,
                          check_no = 6,
                          infer_site = T)
  
  return(output)
}

#' Identify any rows that are empty of meaningful data.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return row IDs in requested output format that are empty of useful info
#' @example
#' empty_row(cohort = "BrCa", site = "MSK", output_format = "log")
empty_row <- function(cohort, site, report, output_format = "log") {
  
  objs <- list()
  output <- c()
  
  # gather data objects
  if (report == "upload") {
    objs[[1]] <- config$uploads[[cohort]][[site]]
  } else if (report == "table") {
    synid_table_all <- get_bpc_table_synapse_ids()
    for (i in 1:length(synid_table_all)) {
      objs[[i]] <- list(synid_table = as.character(synid_table_all[i]), previous = F, select = NA)
    }
  } else {
    return(NULL)
  }
  
  for (obj in objs) {
    data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj)
    
    complete_cols <- grep("complete", names(data), value = T)
    redcap_cols <- c(config$column_name$patient_id,
                     config$column_name$instrument,
                     config$column_name$instance)
    exclude <- c(complete_cols, redcap_cols)
    
    res <- apply(as.matrix(data), 1, is_empty, na.strings = c("NA",""), exclude = exclude)
    idx <- which(res)
    
    output <- rbind(output, format_output(value = rep(NA, length(idx)), 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = NA, 
                            synid = if (is.null(obj$data1)) obj$synid_table else obj$data1,
                            patient_id = data[idx, config$column_name$patient_id], 
                            instrument = if (is.null(obj$data1)) get_bpc_table_instrument(obj$synid_table) else data$redcap_repeat_instrument[idx], 
                            instance = data[idx, config$column_name$instance],
                            check_no = 7,
                            infer_site = F))
  }

  return(output)
}

#' Identify any rows where the sample ID should 
#' be filled in but is empty.  Note: for current BPC instruments, only applies to the 
#' "cancer_panel_test" instrument rows.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return row IDs in requested output format with missing but required sample IDs
#' @example
#' missing_sample_id(cohort = "BrCa", site = "MSK", output_format = "log")
missing_sample_id <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, 
                       report = report, obj = obj_upload)
  idx <- get_bpc_index_missing_sample(data = data)
  
  output <- format_output(value = data[idx, config$column_name$sample_id], 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = config$column_name$sample_id, 
                          synid = obj_upload$data1,
                          patient_id = data[idx, config$column_name$patient_id], 
                          instrument = data[idx, config$column_name$instrument], 
                          instance = data[idx, config$column_name$instance],
                          check_no = 8,
                          infer_site = F)
  
  return(output)
}

#' Identify any columns where all values are missing.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return column names in requested output format
#' @example
#' col_empty(cohort = "BrCa", site = "MSK", output_format = "log")
col_empty <- function(cohort, site, report, output_format = "log") {
  
  objs <- list()
  output <- c()
  
  # gather data objects
  if (report == "upload") {
    objs[[1]] <- config$uploads[[cohort]][[site]]
  } else if (report == "table") {
    synid_table_all <- get_bpc_table_synapse_ids()
    for (i in 1:length(synid_table_all)) {
      objs[[i]] <- list(synid_table = as.character(synid_table_all[i]), previous = F, select = NA)
    }
  } else {
    return(NULL)
  }
  
  for (obj in objs) {
    
    data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj)
    
    # empty columns (accounting for checkbox variables)
    is_col_empty <- unlist(lapply(data, is_empty))
    col_root_empty <- unlist(lapply(strsplit(names(is_col_empty)[which(is_col_empty)], split = "___"), head, n = 1))
    col_root_not_empty <- unlist(lapply(strsplit(names(is_col_empty)[which(!is_col_empty)], split = "___"), head, n = 1))
    col_empty <- setdiff(col_root_empty, col_root_not_empty)
    
    # get relevant instrument
    instrument <- NA
    if (is.null(obj$data1)) {
      instrument <- get_bpc_table_instrument(obj$synid_table)
    } 
    
    output <- rbind(output, format_output(value = col_empty, 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = col_empty, 
                            synid = if (is.null(obj$data1)) obj$synid_table else obj$data1,
                            patient_id = NA, 
                            instrument = instrument, 
                            instance = NA,
                            check_no = 9,
                            infer_site = F))
  }
  
  return(output)
}

col_data_type_dd_mismatch <- function(cohort, site, report, output_format = "log") {
  return(NULL)
}

col_entry_data_type_dd_mismatch <- function(cohort, site, report, output_format = "log") {
  return(NULL)
}

#' Identify any columns where inferred  data type is of type 'character' but the 
#' Scope of Release reference states the data type is of type 'numeric'.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return column names in requested output format
col_data_type_sor_mismatch <- function(cohort, site, report, output_format = "log") {
  
  objs <- list()
  output <- c()
  
  # read sor
  sor <- get_data(config$synapse$sor$id, sheet = 2)
  
  # gather data objects
  if (report == "upload") {
    objs[[1]] <- config$uploads[[cohort]][[site]]
  } else if (report == "table") {
    synid_table_all <- get_bpc_table_synapse_ids()
    for (i in 1:length(synid_table_all)) {
      objs[[i]] <- list(synid_table = as.character(synid_table_all[i]), previous = F, select = NA)
    }
  } else {
    return(NULL)
  }
  
  for (obj in objs) {
    data <- get_bpc_data(cohort = cohort, site = site, 
                         report = report, obj = obj)
    
    # remove allowed non-integer values
    for (var in names(config$noninteger_values)) {
      if (is.element(var, colnames(data))) {
        data[[var]][which(is.element(data[[var]], unlist(config$noninteger_values[var])))] = NA
      }
    }
    
    # variable data types
    type_inf <- unlist(config$maps$data_type[unlist(lapply(data, infer_data_type))])
    type_sor <- get_bpc_sor_data_type(var_name = colnames(data), sor = sor)
    idx <- which(type_sor == "numeric" & type_inf == "character")
    
    # format output
    output <- rbind(output, format_output(value = colnames(data)[idx], 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = NA, 
                            synid = if (is.null(obj$data1)) obj$synid_table else obj$data1,
                            patient_id = NA, 
                            instrument = if (is.null(obj$data1)) get_bpc_table_instrument(obj$synid_table) else NA, 
                            instance = NA,
                            check_no = 12,
                            infer_site = F))
  }
  
  return(output)
}

#' Identify any columns where inferred data type is of type 'character' but the 
#' Scope of Release reference states the data type is of type 'numeric'.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return value and row information in requested format
col_entry_data_type_sor_mismatch <- function(cohort, site, report, output_format = "log") {
  
  objs <- list()
  output <- c()
  
  # read sor and upload file
  sor <- get_data(config$synapse$sor$id, sheet = 2)
  
  # gather data objects
  if (report == "upload") {
    objs[[1]] <- config$uploads[[cohort]][[site]]
  } else if (report == "table") {
    synid_table_all <- get_bpc_table_synapse_ids()
    for (i in 1:length(synid_table_all)) {
      objs[[i]] <- list(synid_table = as.character(synid_table_all[i]), previous = F, select = NA)
    }
  } else {
    return(NULL)
  }
  
  for (obj in objs) {
    data <- get_bpc_data(cohort = cohort, site = site, 
                         report = report, obj = obj)
    
    # remove allowed non-integer values
    for (var in names(config$noninteger_values)) {
      if (is.element(var, colnames(data))) {
        data[[var]][which(is.element(data[[var]], unlist(config$noninteger_values[var])))] = NA
      }
    }
    
    # variable data types from scope of release
    type_sor <- get_bpc_sor_data_type(var_name = colnames(data), sor = sor)
    names(type_sor) <- colnames(data)
    
    for (i in 1:ncol(data)) {
      
      type_sor_col <- type_sor[colnames(data)[i]]
      
      if (!is.na(type_sor_col) && type_sor_col == "numeric") {
        
        type_inf <- unlist(config$maps$data_type[apply(data[,i, drop = F], 1, infer_data_type)])
        idx <- which(type_inf == "character")
        
        # format output
        output <- rbind(output, format_output(value = data[idx, i], 
                                              cohort = cohort, 
                                              site = site,
                                              output_format = output_format,
                                              column_name = colnames(data)[i], 
                                              synid = if (is.null(obj$data1)) obj$synid_table else obj$data1,
                                              patient_id = data$record_id[idx], 
                                              instrument = if (is.null(obj$data1)) get_bpc_table_instrument(obj$synid_table) else data$redcap_repeat_instrument[idx], 
                                              instance = data$redcap_repeat_instance[idx],
                                              check_no = 13,
                                              infer_site = F))
      }
    }
  }
  
  return(output)
}

no_mapped_diag <- function(cohort, site, report, output_format = "log") {
  
  # read data from appropriate source
  if (report == "upload") {
    obj <- config$uploads[[cohort]][[site]]
  } else if (report == "table") {
    synid_table_all <- get_bpc_table_synapse_ids()
    obj <- list(synid_table = as.character(synid_table_all[config$table_name$sample_id]), previous = F, select = NA)
  } else {
    return(NULL)
  }
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj)
  
  # report any samples in the upload and table
  if (report == "upload") {
    res <- data %>% filter(is.na(config$column_name$oncotree_code) & redcap_repeat_instrument == config$instrument_name$panel) 
  } else if (report == "table") {
    res <- data %>% filter(is.na(config$column_name$oncotree_code)) 
  }
  
  output <- format_output(value = res[[config$column_name$sample_id]], 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = config$column_name$oncotree_code, 
                          synid = if (is.null(obj$data1)) obj$synid_table else obj$data1,
                          patient_id = res[[config$column_name$patient_id]], 
                          instrument = config$instrument_name$panel, 
                          instance = res[[config$column_name$instance]],
                          check_no = 14,
                          infer_site = F)
  
  return(output)
}

#' Identify any row of data that has been added for a 
#' given cohort-site pair between the current and previous upload.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return key values uniquely identifying added rows
#' @example
#' rows_added(cohort = "BrCa", site = "MSK", output_format = "log")
rows_added <- function(cohort, site, report, output_format = "log") {
  
  view <- get_bpc_set_view(cohort, report)

  # get added for for each table
  output <- c()
  synid_entity_source <- ""
  for (i in 1:nrow(view)) {
    
    if (report == "table" || report == "comparison") {
      synid_entity_source <- view$id[i]
      data <- get_bpc_pair(cohort, site, report, synid_entity_source) 
      primary_keys <- strsplit(view$primary_key[i], split = ', ')[[1]]
    } else if (report == "release") {
      
      synid_entity_source <- view$id[i]
      data <- get_bpc_pair(cohort = cohort, site = site, report = report, synid_entity_source)
      primary_keys <- intersect(strsplit(view$primary_key[i], split = ', ')[[1]], colnames(data$current))
    }

    data_added <- data$current %>% 
      anti_join(data$previous, by = primary_keys) %>%
      select(all_of(primary_keys))
    
    if (nrow(data_added)) {
      output <- rbind(output, format_output(value = rep(NA, nrow(data_added)),
                                            cohort = cohort,
                                            site = site,
                                            output_format = output_format,
                                            column_name = NA,
                                            synid = synid_entity_source,
                                            patient_id = data_added$record_id,
                                            instrument = view$form[i],
                                            instance = if (!is.null(data_added$redcap_repeat_instance)) data_added$redcap_repeat_instance else NA,
                                            check_no = 15,
                                            infer_site = F))
    }
  }

  return(output)
}

#' Identify any row of data that has been removed for a 
#' given cohort-site pair between the current and previous upload.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return key values uniquely identifying added rows
#' @example
#' rows_removed(cohort = "BrCa", site = "MSK", output_format = "log")
rows_removed <- function(cohort, site, report, output_format = "log", debug = F) {
  view <- get_bpc_set_view(cohort, report)
  
  # get added for for each table
  output <- c()
  synid_entity_source <- ""
  for (i in 1:nrow(view)) {
    
    if (report == "table" || report == "comparison") {
      synid_entity_source <- view$id[i]
      data <- get_bpc_pair(cohort, site, report, synid_entity_source) 
      primary_keys <- strsplit(view$primary_key[i], split = ', ')[[1]]
    } else if (report == "release") {
      
      synid_entity_source <- view$id[i]
      data <- get_bpc_pair(cohort = cohort, site = site, report = report, synid_entity_source)
      primary_keys <- intersect(strsplit(view$primary_key[i], split = ', ')[[1]], colnames(data$current))
    }
    
    data_removed <- data$previous %>% 
      anti_join(data$current, by = primary_keys) %>%
      select(all_of(primary_keys))
    
    if (nrow(data_removed)) {
      output <- rbind(output, format_output(value = rep(NA, nrow(data_removed)),
                                            cohort = cohort,
                                            site = site,
                                            output_format = output_format,
                                            column_name = NA,
                                            synid = synid_entity_source,
                                            patient_id = data_removed$record_id,
                                            instrument = view$form[i],
                                            instance = if (!is.null(data_removed$redcap_repeat_instance)) data_removed$redcap_repeat_instance else NA,
                                            check_no = 16,
                                            infer_site = F))
    }
  }
  
  return(output)
  
}

#' Identify any sample ID in a file that
#' is not listed in the main GENIE reference.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return vector of sample IDs 
#' @example
#' sample_not_in_main_genie(cohort = "BrCa", site = "MSK", output_format = "log")
sample_not_in_main_genie <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  
  mg_ids <- get_main_genie_ids(config$synapse$genie_sample$id, patient = T, sample = T)
  bpc_data <- get_bpc_data(cohort = cohort, site = site, 
                           report = report, obj = obj_upload)
  bpc_sids <- bpc_data$cpt_genie_sample_id[which(!is.na(bpc_data$cpt_genie_sample_id) & !grepl(pattern = "[-_]2$", x = bpc_data$cpt_genie_sample_id))]
  
  bpc_not_mg_sid <- get_added(bpc_sids, mg_ids$SAMPLE_ID)
  bpc_not_mg_pid <- c()
  if (length(bpc_not_mg_sid)) {
    bpc_not_mg_pid <- as.character(unlist(bpc_data %>% 
      filter((!!as.symbol(config$column_name$sample_id)) %in% bpc_not_mg_sid) %>%
      select((!!as.symbol(config$column_name$patient_id)))))
  }
  
  output <- format_output(value = bpc_not_mg_sid, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = "cpt_genie_sample_id", 
                          synid = obj_upload$data1,
                          patient_id = bpc_not_mg_pid, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 17,
                          infer_site = F)
  
  return(output)
}

#' Identify any patient ID in a file that
#' is not listed in the main GENIE reference.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return vector of patient IDs 
#' @example
#' patient_not_in_main_genie(cohort = "BrCa", site = "MSK", output_format = "log")
patient_not_in_main_genie <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  
  mg_ids <- get_main_genie_ids(config$synapse$genie_sample$id, patient = T, sample = F)
  bpc_data <- get_bpc_data(cohort = cohort, site = site, 
                           report = report, obj = obj_upload)
  bpc_pids <- bpc_data$record_id[!grepl(pattern = "[-_]2$", x = bpc_data$record_id)]
  
  bpc_not_mg_pid <- get_added(bpc_pids, mg_ids$PATIENT_ID)
  
  output <- format_output(value = bpc_not_mg_pid, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = "record_id", 
                          synid = obj_upload$data1,
                          patient_id = bpc_not_mg_pid, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 18,
                          infer_site = F)
  
  return(output)
}

#' Column contains timestamps in the incorrect format.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return vector of column names
#' @example
#' col_data_datetime_format_mismatch(cohort = "BrCa", site = "MSK", output_format = "log")
col_data_datetime_format_mismatch <- function(cohort, site, report, output_format = "log") {
  
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, 
                       report = report, obj = obj_upload)
  
  synid_file_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                         cohort = cohort,
                                         file_name = "Data Dictionary non-PHI")
  dd <- get_data(synid_file_dd)
  column_names <- intersect(colnames(data), unlist(dd %>% 
                                             filter(grepl(x = `Text Validation Type OR Show Slider Number`, pattern = "^datetime_")) %>%
                                             select("Variable / Field Name")))
                    
  res <- c()
  for (column_name in column_names) {
    res[column_name] <- all(is_timestamp_format_correct(data[[column_name]], 
                                                        formats = c("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")))
  }
  
  values <- names(res)[!res]
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = values, 
                          synid = obj_upload$data1,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 19,
                          infer_site = F)
  
  return(output)
}

#' Individual timestamps values in a column are not the correct format.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return values with incorrect timestamp formatting
#' @example
#' c0020_col_entry_datetime_format_mismatch(cohort = "BrCa", site = "MSK", output_format = "log")
col_entry_datetime_format_mismatch <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  synid_file_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                         cohort = cohort,
                                         file_name = "Data Dictionary non-PHI")
  dd <- get_data(synid_file_dd)
  column_names <- intersect(colnames(data), unlist(dd %>% 
                                               filter(grepl(x = `Text Validation Type OR Show Slider Number`, pattern = "^datetime_")) %>%
                                               select("Variable / Field Name")))
  
  res <- matrix(NA, nrow = nrow(data), ncol = length(column_names), dimnames = list(c(), column_names))
  for (column_name in column_names) {
    res[,column_name] <- is_timestamp_format_correct(data[[column_name]], 
                                                        formats = c("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"))
  }
  
  idx_values <- which(!res, arr.ind = T)
  values <- rep(NA, nrow(idx_values))
  for (idx_column in unique(idx_values[,2])) {
    values[which(idx_values[,2] == idx_column)] <- data[[column_names[idx_column]]][idx_values[which(idx_values[,2] == idx_column),1]]
  }

  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = column_names[idx_values[,2]], 
                          synid = obj_upload$data1,
                          patient_id = data$record_id[idx_values[,1]], 
                          instrument = data$redcap_repeat_instrument[idx_values[,1]], 
                          instance = data$redcap_repeat_instance[idx_values[,1]],
                          check_no = 20,
                          infer_site = F)
  
  return(output)
}

#' Column contains dates in the incorrect format.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return vector of column names
#' @example
#' col_data_date_format_mismatch(cohort = "BrCa", site = "MSK", output_format = "log")
col_data_date_format_mismatch <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site,                                report = report, obj = obj_upload)
  
  synid_file_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                         cohort = cohort,
                                         file_name = "Data Dictionary non-PHI")
  dd <- get_data(synid_file_dd)
  column_names <- intersect(colnames(data), unlist(dd %>% 
                           filter(grepl(x = `Text Validation Type OR Show Slider Number`, pattern = "^date_")) %>%
                           select("Variable / Field Name")))
  
  res <- c()
  for (column_name in column_names) {
    res[column_name] <- all(is_date_format_correct(data[[column_name]], 
                                                        formats = "%Y-%m-%d"))
  }
  
  values <- names(res)[!res]
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = values, 
                          synid = obj_upload$data1,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 21,
                          infer_site = F)
  
  return(output)
}

#' Individual date values in a column are not the correct format.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return values with incorrect date formatting
#' @example
#' col_entry_date_format_mismatch(cohort = "BrCa", site = "MSK", output_format = "log")
col_entry_date_format_mismatch <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site,                                report = report, obj = obj_upload)
  
  synid_file_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                         cohort = cohort,
                                         file_name = "Data Dictionary non-PHI")
  dd <- get_data(synid_file_dd)
  column_names <- intersect(colnames(data), unlist(dd %>% 
                           filter(grepl(x = `Text Validation Type OR Show Slider Number`, pattern = "^date_")) %>%
                           select("Variable / Field Name")))
  
  res <- matrix(NA, nrow = nrow(data), ncol = length(column_names), dimnames = list(c(), column_names))
  for (column_name in column_names) {
    res[,column_name] <- is_date_format_correct(data[[column_name]], 
                                                     formats = "%Y-%m-%d")
  }
  
  idx_values <- which(!res, arr.ind = T)
  values <- rep(NA, nrow(idx_values))
  for (idx_column in unique(idx_values[,2])) {
    values[which(idx_values[,2] == idx_column)] <- data[[column_names[idx_column]]][idx_values[which(idx_values[,2] == idx_column),1]]
  }
  
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = column_names[idx_values[,2]], 
                          synid = obj_upload$data1,
                          patient_id = data$record_id[idx_values[,1]], 
                          instrument = data$redcap_repeat_instrument[idx_values[,1]], 
                          instance = data$redcap_repeat_instance[idx_values[,1]],
                          check_no = 22,
                          infer_site = F)
  
  return(output)
}

#' Check for columns that are required but completely empty.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return vector of column names
#' @example
#' col_empty_but_required(cohort = "BrCa", site = "MSK", output_format = "log")
col_empty_but_required <- function(cohort, site, report, output_format = "log", exclude = c("qa_full_reviewer_dual")) {
  
  synid_file_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                         cohort = cohort,
                                         file_name = "Data Dictionary non-PHI")
  dd <- get_data(synid_file_dd)
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  col_req <- unlist(dd %>% 
                      filter(`Required Field?` == 'y' & !is.element(`Variable / Field Name`, exclude)) %>%
                      select(`Variable / Field Name`))
  
  # get empty columns, accounting for unraveled checkbox variables
  is_col_empty <- unlist(lapply(data, is_empty))
  col_root_empty <- unlist(lapply(strsplit(names(is_col_empty)[which(is_col_empty)], split = "___"), head, n = 1))
  col_root_not_empty <- unlist(lapply(strsplit(names(is_col_empty)[which(!is_col_empty)], split = "___"), head, n = 1))
  col_empty <- setdiff(col_root_empty, col_root_not_empty)
  
  output <- format_output(value = intersect(col_req, col_empty), 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = NA, 
                          synid = obj_upload$data1,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 23,
                          infer_site = F)
  
  return(output)
}

#' Check for columns that are in the tables but
#' not the scope of release.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return vector of column names
#' @example
#' col_table_not_sor(cohort = "BrCa", output_format = "log")
col_table_not_sor <- function(cohort, site, report, output_format = "log") {
  
  sor <- get_data(config$synapse$sor$id, sheet = 2)
  sor_variables <- as.character(unlist(sor %>%
                                         filter(TYPE %in% c("Curated","Project GENIE Tier 1 data","Tumor Registry")) %>%
                                         select("VARNAME") %>%
                                         distinct()))
  
  query <- glue("SELECT id FROM {config$synapse$tables_view$id} WHERE double_curated = 'false' AND table_type = 'data'")
  synid_tables <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  table_variables <- c()
  for(synid_table in synid_tables) {
    schema <- synGet(synid_table)
    cols <- unlist(lapply(as.list(synGetTableColumns(schema)), function(x) {return(x$name)}))
    table_variables <- append(table_variables, cols)
  }
  
  values <- setdiff(table_variables, sor_variables)
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = values, 
                          synid = config$synapse$tables_view$id,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 24,
                          infer_site = F)
  
  return(output)
}

#' Check for columns that are in the scope
#' of release but not the tables.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return vector of column names
#' @example
#' col_table_not_sor(cohort = "BrCa", output_format = "log")
col_sor_not_table <- function(cohort, site, report, output_format = "log") {
  
  sor <- get_data(config$synapse$sor$id, sheet = 2)
  sor_variables <- as.character(unlist(sor %>%
    filter(TYPE %in% c("Curated","Project GENIE Tier 1 data","Tumor Registry")) %>%
    select("VARNAME") %>%
    distinct()))
  
  query <- glue("SELECT id FROM {config$synapse$tables_view$id} WHERE double_curated = 'false' AND table_type = 'data'")
  synid_tables <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  table_variables <- c()
  for(synid_table in synid_tables) {
    schema <- synGet(synid_table)
    cols <- unlist(lapply(as.list(synGetTableColumns(schema)), function(x) {return(x$name)}))
    table_variables <- append(table_variables, cols)
  }
  
  values <- setdiff(sor_variables, table_variables)
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = values, 
                          synid = config$synapse$tables_view$id,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 25,
                          infer_site = F)
  
  return(output)
}

#' Check for patients marked as removed from BPC in a BPC cohort.   
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param report Type of report in which the check is being conducted (e.g. upload, table).
#' @param output_format (optional) output format for the check
#' @return vector of patient IDs
patient_marked_removed_from_bpc <- function(cohort, site, report, output_format = "log") {
  
  # read data from appropriate source
  if (report == "upload") {
    obj <- config$uploads[[cohort]][[site]]
  } else if (report == "table") {
    synid_table_all <- get_bpc_table_synapse_ids()
    obj <- list(synid_table = as.character(synid_table_all[config$table_name$patient_id]), previous = F, select = NA)
  } else {
    return(NULL)
  }
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj)
  
  # read samples for cohort from patient table
  query <- glue("SELECT record_id FROM {config$synapse$rm_pat$id} WHERE {cohort} = 'true'")
  pat_rm <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  # report any samples in the upload and table
  values <- intersect(data[[config$column_name$patient_id]], pat_rm)
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = config$column_name$patient_id, 
                          synid = if (is.null(obj$data1)) obj$synid_table else obj$data1,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 26,
                          infer_site = F)
  
  return(output)
}

#' Check for samples marked as removed from BPC a BPC cohort.   
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return vector of sample IDs
#' @example
#' sample_marked_removed_from_bpc(cohort = "BrCa", site = "DFCI, output_format = "log")
sample_marked_removed_from_bpc <- function(cohort, site, report, output_format = "log") {
  
  # read data from appropriate source
  if (report == "upload") {
    obj <- config$uploads[[cohort]][[site]]
  } else if (report == "table") {
    synid_table_all <- get_bpc_table_synapse_ids()
    obj <- list(synid_table = as.character(synid_table_all[config$table_name$sample_id]), previous = F, select = NA)
  } else {
    return(NULL)
  }
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj)
  
  # read samples for cohort from patient table
  query <- glue("SELECT SAMPLE_ID FROM {config$synapse$rm_sam$id} WHERE {cohort} = 'true'")
  sam_rm <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  # report any samples in the upload and table
  values <- intersect(data[[config$column_name$sample_id]], sam_rm)
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = config$column_name$sample_id, 
                          synid = if (is.null(obj$data1)) obj$synid_table else obj$data1,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 27,
                          infer_site = F)
  
  return(output)
}

#' Check if patient count less than target count.   
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return number of current patients 
#' @example
#' patient_count_too_small(cohort = "Prostate", site = "VICC", output_format = "log")
patient_count_too_small <- function(cohort, site, report, output_format = "log") {
  
  # read upload
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  n_current <- get_bpc_case_count(data)
  
  # read samples for cohort from patient table
  phase_cohort_list <- unlist(parse_phase_from_cohort(cohort))
  cohort_without_phase <- phase_cohort_list[1]
  phase <- phase_cohort_list[2]
  query <- glue("SELECT target_cases FROM {config$synapse$target_count$id} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}")
  query_result <- as.integer(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  n_target <- 0
  if (length(query_result)){
    n_target <- query_result
  }
  
  output <- NULL
  if (n_current < n_target) {
    output <- format_output(value = glue("{n_current}-->{n_target} (need {n_target - n_current} more)"), 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = config$column_name$patient_id, 
                            synid = obj_upload$data1,
                            patient_id = NA, 
                            instrument = NA, 
                            instance = NA,
                            check_no = 28,
                            infer_site = F)
  }
  
  return(output)
}

#' Check if investigational (masked) drug duration is greater than 1 day.    
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return number of current patients 
#' @example
#' investigational_drug_duration(cohort = "Prostate", site = "VICC", output_format = "log")
investigational_drug_duration <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data_upload <- get_bpc_data(cohort = cohort, site = site, 
                              report = report, obj = obj_upload)
  code_masked <- config$maps$drug$investigational_drug
  
  results <- c()
  for (i in 1:5) {
    
    results <- rbind(results, data_upload %>%
      filter(redcap_repeat_instrument == "ca_directed_drugs" & 
               !!as.symbol(glue("drugs_drug_{i}")) == code_masked &
               !!as.symbol(glue("drugs_startdt_int_{i}")) != !!as.symbol(glue("drugs_enddt_int_{i}"))) %>%
      select(record_id, redcap_repeat_instrument, redcap_repeat_instance) %>%
      mutate(column_name = glue("drugs_enddt_int_{i}")))
  }
  
  output <- format_output(value = rep(NA, nrow(results)),
                          cohort = cohort, 
                          site = site,  
                          synid = obj_upload$data1,
                          patient_id = results$record_id, 
                          instrument = results$redcap_repeat_instrument, 
                          instance = results$redcap_repeat_instance, 
                          column_name = results$column_name, 
                          check_no = 29, 
                          infer_site = F)
  
  return(output)
}

#' Check if investigational (masked) drug has another drug specified in the other drug column.    
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return number of current patients 
#' @example
#' investigational_drug_other_name(cohort = "Prostate", site = "VICC", output_format = "log")
investigational_drug_other_name <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data_upload <- get_bpc_data(cohort = cohort, site = site, 
                              report = report, obj = obj_upload)
  code_masked <- config$maps$drug$investigational_drug
  
  results <- c()
  for (i in 1:5) {
    
    results <- rbind(results, data_upload %>%
                       filter(redcap_repeat_instrument == "ca_directed_drugs" & 
                                !!as.symbol(glue("drugs_drug_{i}")) == code_masked &
                                !is.na(!!as.symbol(glue("drugs_drug_oth_{i}")))) %>%
                       select(record_id, redcap_repeat_instrument, redcap_repeat_instance) %>%
                       mutate(column_name = glue("drugs_drug_oth_{i}")))
  }
  
  output <- format_output(value = rep(NA, nrow(results)),
                          cohort = cohort, 
                          site = site,  
                          synid = obj_upload$data1,
                          patient_id = results$record_id, 
                          instrument = results$redcap_repeat_instrument, 
                          instance = results$redcap_repeat_instance, 
                          column_name = results$column_name, 
                          check_no = 30, 
                          infer_site = F)
  
  return(output)
}

#' Check that investigational (masked) drug is indicated as part of a clinical trial.    
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return number of current patients 
#' @example
#' investigational_drug_not_ct(cohort = "Prostate", site = "VICC", output_format = "log")
investigational_drug_not_ct <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data_upload <- get_bpc_data(cohort = cohort, site = site, 
                              report = report, obj = obj_upload)
  code_masked <- config$maps$drug$investigational_drug
  code_yes <- config$maps$drug$ct_yes
  
  results <- c()
  for (i in 1:5) {
    
    results <- rbind(results, data_upload %>%
                       filter(redcap_repeat_instrument == "ca_directed_drugs" & 
                                !!as.symbol(glue("drugs_drug_{i}")) == code_masked &
                                drugs_ct_yn != code_yes) %>%
                       select(record_id, redcap_repeat_instrument, redcap_repeat_instance) %>%
                       mutate(column_name = glue("drugs_drug_{i}")))
  }
  
  output <- format_output(value = rep(NA, nrow(results)),
                          cohort = cohort, 
                          site = site,  
                          synid = obj_upload$data1,
                          patient_id = results$record_id, 
                          instrument = results$redcap_repeat_instrument, 
                          instance = results$redcap_repeat_instance, 
                          column_name = results$column_name, 
                          check_no = 31, 
                          infer_site = F)
  
  return(output)
}

#' Check if non-investigational (unmasked) drugs are not FDA approved.    
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return number of current patients 
#' @example
#' drug_not_fda_approved(cohort = "Prostate", site = "VICC", report = "masking", output_format = "log")
drug_not_fda_approved <- function(cohort, site, report, output_format = "log") {
  
  code_drug <- c()
  fda_status <- c()
  results <- c()
  
  year_curation <- get_bpc_curation_year(cohort, site)
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data_upload <- get_bpc_data(cohort = cohort, site = site,
                              report = report, obj = obj_upload)
  code_masked <- config$maps$drug$investigational_drug

  for (i in 1:5) {

    code_drug_i <- unlist(data_upload %>%
      filter(redcap_repeat_instrument == "ca_directed_drugs" &
               !!as.symbol(glue("drugs_drug_{i}")) != code_masked) %>%
      select(!!as.symbol(glue("drugs_drug_{i}"))))
    code_drug <- unique(append(code_drug, code_drug_i))
  }

  # fda status according to hemonc
  for (code_ncit in as.character(code_drug)) {

    # map to hemonc code
    code_hemonc <- get_hemonc_from_ncit(code_ncit)
    year_fda <- get_hemonc_fda_approval_year(code_hemonc)

    if (is.na(year_fda)) {
      fda_status[code_ncit] <- "unknown"
    } else if (year_curation <= year_fda) {
      fda_status[code_ncit] <- "unapproved"
    } else {
      fda_status[code_ncit] <- "approved"
    }
  }
  
  # extract any instances not explicitly approved
  code_unapproved <- names(fda_status[which(fda_status != "approved")])
  for (i in 1:5) {
    results <- rbind(results, data_upload %>%
      filter(redcap_repeat_instrument == "ca_directed_drugs" & is.element(!!as.symbol(glue("drugs_drug_{i}")), code_unapproved)) %>%
      mutate(column_name = glue("drugs_drug_{i}")) %>%
      select(record_id, redcap_repeat_instrument, redcap_repeat_instance, !!as.symbol(glue("drugs_drug_{i}")), column_name) %>%
      rename(drug_name = glue("drugs_drug_{i}")))
  }
  
  output <- format_output(value = results$drug_name,
                          cohort = cohort,
                          site = site,
                          synid = obj_upload$data1,
                          patient_id = results$record_id,
                          instrument = results$redcap_repeat_instrument,
                          instance = results$redcap_repeat_instance,
                          column_name = results$column_name,
                          check_no = 32,
                          infer_site = F)

  return(output)
}

#' Sample ID is an IRR sample
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return vector of sample IDs 
#' @example
#' irr_sample(cohort = "BrCa", site = "MSK", output_format = "log")
irr_sample <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  
  mg_ids <- get_main_genie_ids(config$synapse$genie_sample$id, patient = T, sample = T)
  bpc_data <- get_bpc_data(cohort = cohort, site = site,                                report = report, obj = obj_upload)
  bpc_sids <- bpc_data$cpt_genie_sample_id[grepl(pattern = "[-_]2$", x = bpc_data$cpt_genie_sample_id)]
  
  bpc_not_mg_sid <- get_added(bpc_sids, mg_ids$SAMPLE_ID)
  bpc_not_mg_pid <- c()
  if (length(bpc_not_mg_sid)) {
    
    bpc_not_mg_pid <- mg_ids %>% 
      filter(SAMPLE_ID %in% bpc_not_mg_sid) %>%
      select(PATIENT_ID)
  }
  
  output <- format_output(value = bpc_not_mg_sid, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = "cpt_genie_sample_id", 
                          synid = obj_upload$data1,
                          patient_id = bpc_not_mg_pid, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 17,
                          infer_site = F)
  
  return(output)
}

#' Patient ID in a IRR sample.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return vector of patient IDs 
#' @example
#' irr_patient(cohort = "BrCa", site = "MSK", output_format = "log")
irr_patient <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  
  mg_ids <- get_main_genie_ids(config$synapse$genie_sample$id, patient = T, sample = F)
  bpc_data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  bpc_pids <- bpc_data$record_id[grepl(pattern = "[-_]2$", x = bpc_data$record_id)]
  
  bpc_not_mg_pid <- get_added(bpc_pids, mg_ids$PATIENT_ID)
  
  output <- format_output(value = bpc_not_mg_pid, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = "record_id", 
                          synid = obj_upload$data1,
                          patient_id = bpc_not_mg_pid, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 18,
                          infer_site = F)
  
  return(output)
}

#' Identify any columns where all values are missing for one site but not others.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return column names in requested output format
col_empty_site_not_others <- function(cohort, site, report, output_format = "log") {
  
  objs <- list()
  output <- c()
  sites <- site
  
  if (is.na(site)) {
    sites <- setdiff(names(config$uploads[[cohort]]), config$constants$sage)
  }
  
  # gather data objects
  synid_table_all <- get_bpc_table_synapse_ids()
  for (i in 1:length(synid_table_all)) {
    objs[[i]] <- list(synid_table = as.character(synid_table_all[i]), previous = F, select = NA)
  }
  
  for (obj in objs) {
    
    data <- get_bpc_data(cohort = cohort, site = NA, report = report, obj = obj)
    
    for (index_site in sites) {
      
      data_index_site <- data %>% filter(grepl(pattern = index_site, x = record_id))
      data_other_site <- data %>% filter(!grepl(pattern = index_site, x = record_id))
      idx_index_site <- which(unlist(lapply(data_index_site, is_empty)))
      idx_other_site <- which(unlist(lapply(data_other_site, is_empty)))
      idx_highlight <- setdiff(idx_index_site, idx_other_site)
      
      output <- rbind(output, format_output(value = colnames(data)[idx_highlight], 
                                            cohort = cohort, 
                                            site = index_site,
                                            output_format = output_format,
                                            column_name = colnames(data)[idx_highlight], 
                                            synid = if (is.null(obj$data1)) obj$synid_table else obj$data1,
                                            patient_id = NA, 
                                            instrument = if (is.null(obj$data1)) get_bpc_table_instrument(obj$synid_table) else data$redcap_repeat_instrument[idx_highlight], 
                                            instance = NA,
                                            check_no = 35,
                                            infer_site = F))
    }
  }
  
  return(output)
}

#' Identify any columns where the fraction of missing values has increased
#' by more than 5% between uploads.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param report Type of report
#' @param output_format (optional) output format for the check
#' @return column names in requested output format
col_five_perc_inc_missing <- function(cohort, site, report, output_format = "log") {
  
  output <- c()
  
  synid_view_current <- get_bpc_set_view(cohort = cohort, report = report, version = config$release[[cohort]]$current)
  synid_view_previous <- get_bpc_set_view(cohort = cohort, report = report, version = config$release[[cohort]]$previous)
  synid_view_all <- synid_view_current %>% filter(is.element(form, synid_view_previous$form))
  
  for (i in 1:length(synid_view_all)) {
    
    if (report == "table" || report == "comparison") {
      data_curr <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(synid_table = as.character(synid_view_all$id[i]), previous = F, select = NA))
      data_prev <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(synid_table = as.character(synid_view_all$id[i]), previous = T, select = NA))
    } else if (report == "release") {
      data_curr <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(version = config$release[[cohort]]$current, file_name = synid_view_all$form[i]))
      data_prev <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(version = config$release[[cohort]]$previous, file_name = synid_view_all$form[i]))
    }
    
    f_miss_curr <- unlist(lapply(data_curr, fraction_empty))
    f_miss_prev <- unlist(lapply(data_prev, fraction_empty))
    
    vars <- intersect(names(f_miss_curr), names(f_miss_prev))
    idx_diff <- which(f_miss_curr[match(vars, names(f_miss_curr))] - f_miss_prev[match(vars, names(f_miss_prev))] > config$thresholds$fraction_missing)
      
    if (length(idx_diff)) {
      output <- rbind(output, format_output(value = colnames(data_curr)[idx_diff], 
                                            cohort = cohort, 
                                            site = site,
                                            output_format = output_format,
                                            column_name = colnames(data_curr)[idx_diff], 
                                            synid = synid_view_all$id[i],
                                            patient_id = NA, 
                                            instrument = NA, 
                                            instance = NA,
                                            check_no = 36,
                                            infer_site = F))
    }
  }
  
  return(output)
}

#' Identify any columns where the fraction of missing values has decreased
#' by more than 5% between uploads.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param report Type of report
#' @param output_format (optional) output format for the check
#' @return column names in requested output format
col_five_perc_dec_missing <- function(cohort, site, report, output_format = "log") {
  
  output <- c()
  
  synid_view_current <- get_bpc_set_view(cohort = cohort, report = report, version = config$release[[cohort]]$current)
  synid_view_previous <- get_bpc_set_view(cohort = cohort, report = report, version = config$release[[cohort]]$previous)
  synid_view_all <- synid_view_current %>% filter(is.element(form, synid_view_previous$form))
  
  for (i in 1:length(synid_view_all)) {
    
    if (report == "table" || report == "comparison") {
      data_curr <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(synid_table = as.character(synid_view_all$id[i]), previous = F, select = NA))
      data_prev <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(synid_table = as.character(synid_view_all$id[i]), previous = T, select = NA))
    } else if (report == "release") {
      data_curr <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(version = config$release[[cohort]]$current, file_name = synid_view_all$form[i]))
      data_prev <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(version = config$release[[cohort]]$previous, file_name = synid_view_all$form[i]))
    }
    
    f_miss_curr <- unlist(lapply(data_curr, fraction_empty))
    f_miss_prev <- unlist(lapply(data_prev, fraction_empty))
    
    vars <- intersect(names(f_miss_curr), names(f_miss_prev))
    idx_diff <- which(f_miss_prev[match(vars, names(f_miss_prev))] - f_miss_curr[match(vars, names(f_miss_curr))] > config$thresholds$fraction_missing)
    
    if (length(idx_diff)) {
      output <- rbind(output, format_output(value = colnames(data_curr)[idx_diff], 
                                            cohort = cohort, 
                                            site = site,
                                            output_format = output_format,
                                            column_name = colnames(data_curr)[idx_diff], 
                                            synid = synid_view_all$id[i],
                                            patient_id = NA, 
                                            instrument = NA, 
                                            instance = NA,
                                            check_no = 37,
                                            infer_site = F))
    }
  }
  
  return(output)
}

#' Identify any columns removed between previous and current upload.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param report Type of report
#' @param output_format (optional) output format for the check
#' @return column names in requested output format
col_removed <- function(cohort, site, report, output_format = "log") {
  
  output <- c()
  
  synid_view_current <- get_bpc_set_view(cohort = cohort, report = report, version = config$release[[cohort]]$current)
  synid_view_previous <- get_bpc_set_view(cohort = cohort, report = report, version = config$release[[cohort]]$previous)
  synid_view_all <- synid_view_current %>% filter(is.element(form, synid_view_previous$form))
  
  for (i in 1:length(synid_view_all)) {
    
    if (report == "table" || report == "comparison") {
      data_curr <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(synid_table = as.character(synid_view_all$id[i]), previous = F, select = NA))
      data_prev <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(synid_table = as.character(synid_view_all$id[i]), previous = T, select = NA))
    } else if (report == "release") {
      data_curr <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(version = config$release[[cohort]]$current, file_name = synid_view_all$form[i]))
      data_prev <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(version = config$release[[cohort]]$previous, file_name = synid_view_all$form[i]))
    }
    
    values <- setdiff(names(data_prev), names(data_curr))
    
    if (length(values)) {
      output <- rbind(output, format_output(value = values, 
                                            cohort = cohort, 
                                            site = site,
                                            output_format = output_format,
                                            column_name = values, 
                                            synid = synid_view_all$id[i],
                                            patient_id = NA, 
                                            instrument = NA, 
                                            instance = NA,
                                            check_no = 38,
                                            infer_site = F))
    }
  }
  
  return(output)
}

#' Identify any columns removed between previous and current upload.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param report Type of report
#' @param output_format (optional) output format for the check
#' @return column names in requested output format
col_added <- function(cohort, site, report, output_format = "log") {
  
  output <- c()
  
  synid_view_current <- get_bpc_set_view(cohort = cohort, report = report, version = config$release[[cohort]]$current)
  synid_view_previous <- get_bpc_set_view(cohort = cohort, report = report, version = config$release[[cohort]]$previous)
  synid_view_all <- synid_view_current %>% filter(is.element(form, synid_view_previous$form))
  
  for (i in 1:length(synid_view_all)) {
    
    if (report == "table" || report == "comparison") {
      data_curr <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(synid_table = as.character(synid_view_all$id[i]), previous = F, select = NA))
      data_prev <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(synid_table = as.character(synid_view_all$id[i]), previous = T, select = NA))
    } else if (report == "release") {
      data_curr <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(version = config$release[[cohort]]$current, file_name = synid_view_all$form[i]))
      data_prev <- get_bpc_data(cohort = cohort, site = site, report = report, 
                                obj = list(version = config$release[[cohort]]$previous, file_name = synid_view_all$form[i]))
    }
    
    values <- setdiff(names(data_curr), names(data_prev))
    
    if (length(values)) {
      output <- rbind(output, format_output(value = values, 
                                            cohort = cohort, 
                                            site = site,
                                            output_format = output_format,
                                            column_name = values, 
                                            synid = synid_view_all$id[i],
                                            patient_id = NA, 
                                            instrument = NA, 
                                            instance = NA,
                                            check_no = 39,
                                            infer_site = F))
    }
  }
  
  return(output)
}

#' Identify any files that have been added between releases.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param report Type of report
#' @param output_format (optional) output format for the check
#' @return files names
file_added <- function(cohort, site, report, output_format = "log") {
  synid_view_current <- get_bpc_set_view(cohort, report, version = config$release[[cohort]]$current)
  synid_view_previous <- get_bpc_set_view(cohort, report, version = config$release[[cohort]]$previous)
  
  values <- setdiff(synid_view_current$form, synid_view_previous$form)
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = NA, 
                          synid = NA,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 40,
                          infer_site = F)
  
  return(output)
}

#' Identify any files that have been removed between releases.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param report Type of report
#' @param output_format (optional) output format for the check
#' @return files names
file_removed <- function(cohort, site, report, output_format = "log") {
  synid_view_current <- get_bpc_set_view(cohort, report, version = config$release[[cohort]]$current)
  synid_view_previous <- get_bpc_set_view(cohort, report, version = config$release[[cohort]]$previous)
  
  values <- setdiff(synid_view_previous$form, synid_view_current$form)
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = NA, 
                          synid = NA,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 41,
                          infer_site = F)
    
  return(output)
}

#' Identify any columns marked required but not present in the dataset.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param report Type of report
#' @param output_format (optional) output format for the check
#' @return column names in requested output format
required_not_uploaded <- function(cohort, site, report, output_format = "log", exclude = c("qa_full_reviewer_dual")) {
  
  synid_file_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                         cohort = cohort,
                                         file_name = "Data Dictionary non-PHI")
  dd <- get_data(synid_file_dd)
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  col_req <- unlist(dd %>% 
                      filter(`Required Field?` == 'y' & !is.element(`Variable / Field Name`, exclude)) %>%
                      select("Variable / Field Name"))
  
  col_data <- unique(unlist(lapply(strsplit(colnames(data), split = "___"), head, n = 1)))
  
  values <- setdiff(col_req, col_data)
  output <- format_output(value = values, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = values, 
                          synid = obj_upload$data1,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 42,
                          infer_site = F)
  
  return(output)
}

#' Check that all drugs on a clinical trial regimen are marked investigational (masked).    
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return number of current patients 
#' @example
#' ct_drug_not_investigational(cohort = "Prostate", site = "VICC", report = "masking", output_format = "log")
ct_drug_not_investigational <- function(cohort, site, report, output_format = "log") {
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data_upload <- get_bpc_data(cohort = cohort, site = site, 
                              report = report, obj = obj_upload)
  code_masked <- config$maps$drug$investigational_drug
  code_yes <- config$maps$drug$ct_yes
  
  results <- c()
  for (i in 1:5) {
    
    results <- rbind(results, data_upload %>%
                       filter(redcap_repeat_instrument == "ca_directed_drugs" & 
                                !!as.symbol(glue("drugs_drug_{i}")) != code_masked &
                                drugs_ct_yn == code_yes) %>%
                       select(record_id, redcap_repeat_instrument, redcap_repeat_instance) %>%
                       mutate(column_name = glue("drugs_drug_{i}")))
  }
  
  output <- format_output(value = rep(NA, nrow(results)),
                          cohort = cohort, 
                          site = site,  
                          synid = obj_upload$data1,
                          patient_id = results$record_id, 
                          instrument = results$redcap_repeat_instrument, 
                          instance = results$redcap_repeat_instance, 
                          column_name = results$column_name, 
                          check_no = 43, 
                          infer_site = F)
  
  return(output)
}

#' Check that a file loaded on Synapse is in CSV format.    
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return number of current patients 
#' @example
#' file_not_csv(cohort = "Prostate", site = "VICC", report = "masking", output_format = "log")

file_not_csv <- function(cohort, site, report, output_format = "log") {
  
  output <- c()
  res <- c()
  obj_upload <- config$uploads[[cohort]][[site]]
  
  if (!is.null(obj_upload$data1)) {
    res[obj_upload$data1] = is_synapse_entity_csv(obj_upload$data1)
  }
  
  if (!is.null(obj_upload$data2)) {
    res[obj_upload$data2] = is_synapse_entity_csv(obj_upload$data2)
  }

  if (!is.null(res) && length(which(!res))) {
    output <- format_output(value = NA,
                            cohort = cohort, 
                            site = site,  
                            synid = names(res)[which(!res)],
                            patient_id = NA, 
                            instrument = NA, 
                            instance = NA, 
                            column_name = NA, 
                            check_no = 44, 
                            infer_site = F)
  }
  
  return(output)
}

data_header_col_mismatch <- function(cohort, site, report, output_format = "log") {
  output <- c()
  res <- c()
  obj_upload <- config$uploads[[cohort]][[site]]
  
  if (!is.null(obj_upload$data1) && !is.null(obj_upload$header1)) {
    data1 = get_data(obj_upload$data1)
    header1 = get_data(obj_upload$header1)
    res[obj_upload$data1] = ncol(data1) == ncol(header1)
  }
  
  if (!is.null(obj_upload$data2) && !is.null(obj_upload$header2)) {
    data2 = get_data(obj_upload$data2)
    header2 = get_data(obj_upload$header2)
    res[obj_upload$data2] = ncol(data2) == ncol(header2)
  }
  
  if (!is.null(res) && length(which(!res))) {
    output <- format_output(value = NA,
                            cohort = cohort, 
                            site = site,  
                            synid = names(res)[idx],
                            patient_id = NA, 
                            instrument = NA, 
                            instance = NA, 
                            column_name = NA, 
                            check_no = 45, 
                            infer_site = F)
  }
  
  return(output)
}

#' Check if patient count does not equal target count.   
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return current and target count message
#' @example
#' current_count_not_target(cohort = "Prostate", site = "VICC", output_format = "log")
current_count_not_target <- function(cohort, site, report, output_format = "log") {
  # read upload
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  n_current <- get_bpc_case_count(data)
  
  # read samples for cohort from patient table
  phase_cohort_list <- unlist(parse_phase_from_cohort(cohort))
  cohort_without_phase <- phase_cohort_list[1]
  phase <- phase_cohort_list[2]
  query <- glue("SELECT target_cases FROM {config$synapse$target_count$id} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}")
  n_target <- as.integer(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  output <- NULL
  if (n_current != n_target) {
    output <- format_output(value = glue("current count {n_current} not equal to target {n_target}"), 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = config$column_name$patient_id, 
                            synid = obj_upload$data1,
                            patient_id = NA, 
                            instrument = NA, 
                            instance = NA,
                            check_no = 46,
                            infer_site = F)
  }
  
  return(output)
  
}

#' Check if patient count less than target count.   
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return number of current patients 
#' @example
#' patient_count_too_large(cohort = "Prostate", site = "VICC", output_format = "log")
patient_count_too_large <- function(cohort, site, report, output_format = "log") {
  
  # read upload
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  n_current <- get_bpc_case_count(data)
  
  # read samples for cohort from patient table
  phase_cohort_list <- unlist(parse_phase_from_cohort(cohort))
  cohort_without_phase <- phase_cohort_list[1]
  phase <- phase_cohort_list[2]
  query <- glue("SELECT target_cases FROM {config$synapse$target_count$id} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}")
  query_result <- as.integer(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  n_target <- 0
  if (length(query_result)){
    n_target <- query_result
  }
  
  output <- NULL
  if (n_current > n_target) {
    output <- format_output(value = glue("{n_current}-->{n_target} (remove {n_current - n_target})"), 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = config$column_name$patient_id, 
                            synid = obj_upload$data1,
                            patient_id = NA, 
                            instrument = NA, 
                            instance = NA,
                            check_no = 47,
                            infer_site = F)
  }
  
  return(output)
}

#' Check if cpt_sample_type variable for applicable instruments is numeric.  It 
#' should be text. 
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return rows with numeric values for cpt_sample_type 
#' @example
#' cpt_sample_type_numeric(cohort = "Prostate", site = "VICC", output_format = "log")
cpt_sample_type_numeric <- function(cohort, site, report, output_format = "log") {
  
  output <- NULL

  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  res <- data %>% 
    filter(redcap_repeat_instrument == config$instrument_name$panel & is_double(cpt_sample_type)) %>%
    select(cpt_sample_type, record_id, redcap_repeat_instrument, redcap_repeat_instance)
    
  output <- format_output(value = res$cpt_sample_type, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = "cpt_sample_type", 
                          synid = obj_upload$data1,
                          patient_id = res$record_id, 
                          instrument = res$redcap_repeat_instrument, 
                          instance = res$redcap_repeat_instance,
                          check_no = 48,
                          infer_site = F)
    return(output)
}

#' Check for columns that are required for running quality assurance checklist.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return quac required columns that are missing
#' @example
#' cpt_sample_type_numeric(cohort = "Prostate", site = "VICC", output_format = "log")
quac_required_column_missing <- function(cohort, site, report, output_format = "log") {
  
  output <- NULL
  
  col_req <- as.character(unlist(config$column_name))
 
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  res <- setdiff(col_req, colnames(data))
  
  output <- format_output(value = res, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = res, 
                          synid = obj_upload$data1,
                          patient_id = NA, 
                          instrument = NA, 
                          instance = NA,
                          check_no = 49,
                          infer_site = F)
  return(output)
}

#' Check for missing drug name when drug start and/or end date is specified.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param output_format (optional) output format for the check
#' @return missing drug records
#' @example
#' invalid_choice_code(cohort = "Prostate", site = "VICC", output_format = "log")
invalid_choice_code <- function(cohort, site, report, output_format = "log") {
 
  output <- NULL
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  synid_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                          cohort = cohort,
                                          file_name = "Data Dictionary non-PHI")
  dd <- get_data(synid_dd)
  
  idx_invalid <- c()
  for (i in 1:length(data)) {
    var_name = colnames(data)[i]
    choices <- dd %>% filter(`Variable / Field Name` == var_name) %>% select(`Choices, Calculations, OR Slider Labels`)
    if (nrow(choices) && !is.na(choices)) {
      codes <- parse_mapping(choices)[,"codes"]
      if (is_double(codes)) {
        codes <- append(codes, as.double(codes))
      }
      idx_invalid <- which(!is.element(data[,i], c(NA, codes)))
      
      if (length(idx_invalid)) {
        output <- rbind(output, format_output(value = data[idx_invalid,i], 
                                              cohort = cohort, 
                                              site = site,
                                              output_format = output_format,
                                              column_name = colnames(data)[i], 
                                              synid = obj_upload$data1,
                                              patient_id = data[idx_invalid, config$column_name$patient_id], 
                                              instrument = data[idx_invalid, config$column_name$instrument], 
                                              instance = data[idx_invalid, config$column_name$instance],
                                              check_no = 50,
                                              infer_site = F))
      } 
    }
  }
  
  return(output)
}

#' Check for case count less than adjusted target.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return missing drug records
#' @example
#' less_than_adjusted_target(cohort = "Prostate", site = "VICC", output_format = "log")
less_than_adjusted_target <- function(cohort, site, report, output_format = "log") {
  output <- NULL
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  n_current <- get_bpc_case_count(data)
  
  # read samples for cohort from patient table
  phase_cohort_list <- unlist(parse_phase_from_cohort(cohort))
  cohort_without_phase <- phase_cohort_list[1]
  phase <- phase_cohort_list[2]
  query <- glue("SELECT adjusted_cases FROM {config$synapse$target_count$id} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}")
  n_adj <- as.integer(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  output <- NULL
  if (length(n_adj) && n_current < n_adj) {
    output <- format_output(value = glue("{n_current}-->{n_adj} (add {n_adj - n_current})"), 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = config$column_name$patient_id, 
                            synid = obj_upload$data1,
                            patient_id = NA, 
                            instrument = NA, 
                            instance = NA,
                            check_no = 51,
                            infer_site = F)
  }
  
  return(output)
}

#' Check for case count greater than adjusted target.  
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return missing drug records
#' @example
#' greater_than_adjusted_target(cohort = "Prostate", site = "VICC", output_format = "log")
greater_than_adjusted_target <- function(cohort, site, report, output_format = "log") {
  output <- NULL
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  n_current <- get_bpc_case_count(data)
  
  # read samples for cohort from patient table
  phase_cohort_list <- unlist(parse_phase_from_cohort(cohort))
  cohort_without_phase <- phase_cohort_list[1]
  phase <- phase_cohort_list[2]
  query <- glue("SELECT adjusted_cases FROM {config$synapse$target_count$id} WHERE cohort = '{cohort_without_phase}' AND site = '{site}' AND phase = {phase}")
  n_adj <- as.integer(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  output <- NULL
  if (length(n_adj) && n_current > n_adj) {
    output <- format_output(value = glue("{n_current}-->{n_adj} (remove {n_current - n_adj})"), 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = config$column_name$patient_id, 
                            synid = obj_upload$data1,
                            patient_id = NA, 
                            instrument = NA, 
                            instance = NA,
                            check_no = 52,
                            infer_site = F)
  }
  
  return(output)
}

#' Check for character entries that have been converted to doubles.   
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return columns with character doubles
#' @example
#' character_double_value(cohort = "Prostate", site = "VICC", output_format = "log")
character_double_value <- function(cohort, site, report, output_format = "log") {
  
  objs <- list()
  output <- NULL
  
  synid_table_all <- get_bpc_table_synapse_ids()
  for (i in 1:length(synid_table_all)) {
    objs[[i]] <- list(synid_table = as.character(synid_table_all[i]), previous = F, select = NA)
  }
  
  for (obj in objs) {
    data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj)
    
    for (i in 1:ncol(data)) {
      idx = which(!sapply(data[,i], is_double))
      data_col <- data[,i]
      data_col[idx] = NA
      comp <- cbind(as.character(data_col), as.character(as.double(data_col)))
                             
       if (!identical(comp[,1], comp[,2])) {
         output <- rbind(output, format_output(value = NA, 
                       cohort = cohort, 
                       site = site,
                       output_format = output_format,
                       column_name = colnames(data)[i], 
                       synid = obj$synid_table,
                       patient_id = NA, 
                       instrument = NA, 
                       instance = NA,
                       check_no = 53,
                       infer_site = F))
       }
    }
  }

  return(output)
}

#' Check for removed patients that are not in the retraction table.   
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return columns with character doubles
#' @example
#' patient_removed_not_retracted(cohort = "Prostate", site = "VICC", output_format = "log")
patient_removed_not_retracted <- function(cohort, site, report, output_format = "log") {
  results <- get_bpc_patient_sample_added_removed(cohort = cohort, 
                                                  site = site,  
                                                  report = report,
                                                  check_patient = T, 
                                                  check_added = F,
                                                  account_for_retracted = T)
  output <- format_output(results$ids, 
                          cohort = cohort, 
                          site = site,
                          output_format = output_format,
                          column_name = results$column_name, 
                          synid = results$synid_entity_source,
                          check_no = 54,
                          infer_site = T)
  
  return(output)
}

#' Get sample IDs removed in the most recent upload
#' relative to the last upload and not retracted
#' or associated with retracted patient.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center
#' @param output_format (optional) output format for the check
#' @return sample IDs removed in the most recent upload, formatted
#' according to the specified output_format
#' @example
#' sample_removed_not_retracted(cohort = "BrCa", output_format = "log")
sample_removed_not_retracted <- function(cohort, site, report, output_format = "log") {
  
  output <- NULL
  
  results <- get_bpc_patient_sample_added_removed(cohort = cohort, 
                                                  site = site,  
                                                  report = report,
                                                  check_patient = F, 
                                                  check_added = F,
                                                  account_for_retracted = T)
  retracted_pts <- get_retracted_patients(cohort)
  for (i in 1:length(retracted_pts)) {
    idx <- grep(pattern = retracted_pts[i], x = results$ids)
    results$ids <- results$ids[-idx]
  }
  
  if (length(results$ids)) {
    output <- format_output(results$ids, 
                            cohort = cohort, 
                            site = site,
                            output_format = output_format,
                            column_name = results$column_name, 
                            synid = results$synid_entity_source,
                            check_no = 55,
                            infer_site = T)
  }
  
  return(output)
}

#' Check for missing drug name when drug start and/or end date is specified.
#'
#' @param cohort Name of the cohort
#' @param site Name of the site or center 
#' @param report Type of report in which the check is being conducted (e.g. upload, table)
#' @param output_format (optional) output format for the check
#' @return sample IDs with missing oncotree codes (cpt_oncotree_code)
#' @example
#' sample_missing_oncotree_code(cohort = "BLADDER", site = "MSK", report = "upload", output_format = "log")
sample_missing_oncotree_code <- function(cohort, site, report, output_format = "log") {
  
  output <- NULL
  
  obj_upload <- config$uploads[[cohort]][[site]]
  data <- get_bpc_data(cohort = cohort, site = site, report = report, obj = obj_upload)
  
  var_code <- config$column_name$oncotree_code
  var_pat <- config$column_name$patient_id
  var_sam <- config$column_name$sample_id
  var_form <- config$column_name$instrument
  var_insta <- config$column_name$instance
  
  df_res <- data %>% 
    filter(is.na(!!as.symbol(var_code)) & !!as.symbol(var_form) == config$instrument_name$panel) %>%
    select(!!as.symbol(var_pat), !!as.symbol(var_sam), !!as.symbol(var_form), !!as.symbol(var_insta))
  
  if (nrow(df_res)) {
    output <- rbind(output, format_output(value = df_res[[var_sam]], 
                                          cohort = cohort, 
                                          site = site,
                                          output_format = output_format,
                                          column_name = var_code, 
                                          synid = obj_upload$data1,
                                          patient_id = df_res[[var_pat]], 
                                          instrument = df_res[[var_form]], 
                                          instance = df_res[[var_insta]],
                                          check_no = 56,
                                          infer_site = F))
  } 
  
  return(output)
}
