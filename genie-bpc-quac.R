#!/usr/bin/env Rscript

# Description: Command line interface for conducting quality assurance (QA) checks
#   on BPC cohorts and sites for different stages of processing.
# Author: Haley Hunter-Zinck
# Date: June 10, 2021

# pre-setup ---------------------------------

library(glue)
library(argparse)
library(yaml)

workdir = "."
if (!file.exists("config.yaml")) {
  workdir <- "/usr/local/src/myscripts"
}
config <- read_yaml(glue("{workdir}/config.yaml"))

choices_report <- sort(names(config$report))
choices_number <- as.integer(names(config$checks)[which(unlist(lapply(config$checks, function(x) {if (x$implemented && !x$deprecated) T else F})))])
choices_level <- setdiff(sort(unique(unlist(lapply(config$checks, function(x) {return(x$level)})))), "fail")
choices_cohort <- sort(names(config$uploads))
choices_site <- sort(unique(unlist(lapply(config$uploads, names))))
choice_all = "all"

# cli ------------------------------------------------

# parse command line arguments
parser <- ArgumentParser()
parser$add_argument("-c", dest = "cohort", type = "character", choices = choices_cohort, 
                    help = "Cohort on which to perform checks", required = T)
parser$add_argument("-s", dest = "site", type = "character", default = choice_all, choices = c(choice_all, choices_site),
                    help = "Site on which to perform checks", required = F)
parser$add_argument("-r", dest = "report", type = "character", choices = choices_report, 
                    help = "Report to generate", required = T)
parser$add_argument("-n", dest = "number", type = "integer", 
                   help = "Reference number of check to run individually within report", required = F)
parser$add_argument("-l", dest = "level", type = "character", default = choice_all, choices = c(choice_all, choices_level), 
                    help = "Level of priority of checks to run", required = F)
parser$add_argument("-o", dest = "overview", action = "store_true", default = F,
                    help = "Display overview of parameters and checks to be performed but do not execute")
parser$add_argument("-u", dest = "save_to_synapse", action = "store_true", default = F,
                    help = "Save report log file to pre-specified Synapse folder")
parser$add_argument("-v", dest = "verbose", action = "store_true", default = F,
                    help = "Display messages on script progress to the user")
parser$add_argument("-a", dest = "synapse_auth", type = "character", default = "~/.synapseConfig",
                    help = "Path to .synapseConfig file or Synapse PAT (default: '~/.synapseConfig')")

# extract command line arguments
args <- parser$parse_args()
report <- args$report
number <- args$number
level <- args$level
cohort <- args$cohort
sites <- args$site
verbose <- args$verbose
save_synapse <- args$save_to_synapse
overview <- args$overview
synapse_auth <- args$synapse_auth

# setup ------------------------------------------------------------------------

# start timer
tic <- as.double(Sys.time())

# libraries
library(dplyr)
library(openxlsx)
library(rjson)
library(synapser)

# functions
source(glue("{workdir}/fxns.R"))
source(glue("{workdir}/checklist.R"))

# synapse login
syn <- synLogin(auth = synapse_auth)

# update config
config <- update_config_for_comparison_report(config)
config <- update_config_for_release_report(config)

# check user input -------------------------------------------------------------

if (!is.null(number)) {
  
  # number should be relevant for report
  check_nos <- config$report[[report]]
  if (!is.element(number, check_nos)) {
    msg1 <- glue("check number '{number}' is not applicable for report type '{report}'. ")
    msg2 <- glue("To view applicable check numbers, run the following: ")
    msg3 <- glue("'Rscript genie-bpc-quac.R -c {cohort} -r upload -o'")
    stop(paste0(msg1, msg2, msg3))
  }
  
  # number should be relevant to number
  check_no_level <- config$checks[[number]]$level
  if (!is.element(level, c(choice_all, check_no_level))) {
    msg1 <- glue("check number '{number}' is not applicable for report type '{report}' and level '{level}'. ")
    msg2 <- glue("To view applicable check numbers, run the following: ")
    msg3 <- glue("'Rscript genie-bpc-quac.R -c {cohort} -r upload -l {level} -o'")
    stop(paste0(msg1, msg2, msg3))
  }
}

# for release report, ensure previous release is available for comparison
if (report == "release" && is.null(config$release[[cohort]]$previous)) {
  msg1 <- glue("cohort {cohort} does not have a previous release. ")
  msg2 <- glue("To view other available cohorts, run the following: ")
  msg3 <- glue("'Rscript genie-bpc-quac.R -h'")
  stop(paste0(msg1, msg2, msg3))
}

# for comparison report, ensure previous table version is available
if (report == "comparison" && is.null(config$comparison[[cohort]]$previous)) {
  msg1 <- glue("cohort {cohort} does not have a previous table version for comparison. ")
  msg2 <- glue("To view other available cohorts, run the following: ")
  msg3 <- glue("'Rscript genie-bpc-quac.R -h'")
  stop(paste0(msg1, msg2, msg3))
}

# for upload report, site cannot be 'all'
if (report == "upload" && sites == choice_all) {
  msg1 <- glue("'{report}' report must be run on an individual site. ")
  msg2 <- glue("To view available sites, run the following: ")
  msg3 <- glue("'Rscript genie-bpc-quac.R -h'")
  stop(paste0(msg1, msg2, msg3))
}

# parameter messaging ----------------------------------------

if (verbose) {
  
  print(glue("Parameters: "))
  print(glue("- cohort:\t\t{cohort}"))
  print(glue("- site(s):\t\t{sites}"))
  print(glue("- report:\t\t{report}"))
  if (report == "comparison") {
    print(glue("  - previous:\t\t{config$comparison[[cohort]]$previous}"))
    print(glue("  - current:\t\t{config$comparison[[cohort]]$current}"))
  }
  if (report == "release") {
    print(glue("  - previous:\t\t{config$release[[cohort]]$previous}"))
    print(glue("  - current:\t\t{config$release[[cohort]]$current}"))
  }
  print(glue("- level:\t\t{level}"))
  if (!is.null(number)) {
    print(glue("- number:\t\t{number}"))
  }
  print(glue("- overview:\t\t{overview}"))
  print(glue("- verbose:\t\t{verbose}"))
  print(glue("- save on synapse:\t{save_synapse}"))
}

# level checks ----------------------------------------

# storage
valid_levels <- level
check_nos <- c()
res <- c()
outfile <- ""

# format input
if (level == choice_all) {
  valid_levels <- choices_level
} 
valid_levels <- c("fail", valid_levels)
if (is.null(number)) {
  check_nos <- config$report[[report]]
} else {
  check_nos <- number
}

# collect relevant check functions
check_nos_valid <- check_nos[unlist(lapply(config$checks[check_nos], function(x) {return(if (x$deprecated == 0 && x$implemented == 1 && is.element(x$level, valid_levels)) T else F)}))]
check_labels <- unlist(lapply(config$checks[check_nos_valid], function(x) {return(x$label)}))
check_level <- unlist(lapply(config$checks[check_nos_valid], function(x) {return(x$level)}))
check_fxns <- get_check_functions(check_labels)

if (overview) {
  cat(glue("Checks ({length(check_fxns)}):"), "\n")
  for (check_no in check_nos_valid) {
    cat(glue("- {config$checks[[check_no]]$level} {sprintf('%02d', check_no)} ({config$checks[[check_no]]$label}): {config$checks[[check_no]]$description} {config$checks[[check_no]]$action}"), "\n")
  }
} else {
  if (length(check_fxns)) {
    
    if (report == "upload" && sites == choice_all) {
      sites <- setdiff(names(config$uploads[[cohort]]), config$constants$sage)
    } else {
      site <- sites
    }
    
    # run each applicable QA check
    res <- c()
    for (i in 1:length(check_fxns)) {
      
      fxn <- check_fxns[[i]]
      if (verbose) {
        cat(glue("{now()}: Checking '{names(check_fxns)[i]}' for cohort '{cohort}' and site '{site}'..."))
      }
      
      invisible(capture.output(res_check <- check_fxns[[i]](cohort = cohort, site = if (site == choice_all) NA else site, report = report)))
      if (verbose) {
        cat(glue(" --> {if(is.null(res_check) || is.na(res_check)) 0 else nrow(res_check)} {check_level[i]}(s) identified"), "\n")
      }
      
      res <- rbind(res, res_check)
      
      # check for flagged fail check
      if (check_level[i] == "fail" && !is.null(res_check) && nrow(res_check) > 0) {
        stop()
      }
    }
    
    # write all issues to file
    outfile <- tolower(glue("{cohort}_{site}_{report}_{level}.csv"))
    if (length(res)) {
      issue_no <- seq_len(nrow(res))
      write.csv(cbind(issue_no, res), file = outfile, row.names = F)
    } else {
      to_write = glue("No {level}s triggered.  Congrats! All done!")
      write(to_write, ncolumns = 1, file = outfile)
    }
  } else {
    if (verbose) {
      print(glue("{now()}: No applicable {level}-level checks to perform."))
    }
  }
}


# wrap-up ----------------------------------------------------------------------

# output all output files
n_issue <- if (length(res)) nrow(res) else 0
if (verbose && file.exists(outfile)) {
  
  if (level == choice_all) {
    print(glue("{now()}: Issues ({n_issue}) written to {outfile}"))
  } else {
    print(glue("{now()}: {capitalize(level)}s ({n_issue}) written to {outfile}"))
  }
}

if (save_synapse && file.exists(outfile)) {
  
  synid_folder_output <- config$output[[cohort]]
  
  synid_file_output <- save_to_synapse(path = outfile, 
                  parent_id = synid_folder_output, 
                  prov_name = "GENIE BPC QA log", 
                  prov_desc = glue("GENIE BPC QA {report} {level} log for cohort '{cohort}' and site(s) '{sites}'"), 
                  prov_used = NA, 
                  prov_exec = "https://github.com/hhunterzinck/genie-bpc-quac/blob/develop/genie-bpc-quac.R")
  synSetAnnotations(synid_file_output, annotations=list(cohort = cohort, 
                                                        site = sites, 
                                                        level = level, 
                                                        report = report,
                                                        issueCount = as.integer(n_issue)))
  file.remove(outfile)
  
  if (verbose) {
    print(glue("{now()}: Saved log to Synapse at '{outfile}' ({synid_file_output})"))
  }
}

# finish
toc <- as.double(Sys.time())
if (verbose) {
  print(glue("{now()}: Runtime: {round(toc - tic)} s"))
}

quit(status = n_issue)
