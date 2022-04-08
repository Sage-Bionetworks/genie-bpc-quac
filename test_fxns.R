# Description: tests for the BPC QA function checks, wrappers, and helpers.
# Author: Haley Hunter-Zinck
# Date: June 22, 2021

# setup ------------------------------------------------------------------------

library(testthat)
library(yaml)
library(synapser)
synLogin()
source("checklist.R")
source("fxns.R")

# global variables
config <- read_yaml("config.yaml")

# local files
file_csv <- "test.csv"

# fixtures -----------------------------------------------------------------------

# version 1
cols <- list(
  Column(name = 'Name', columnType = 'STRING', maximumSize = 20),
  Column(name = 'Chromosome', columnType = 'STRING', maximumSize = 20),
  Column(name = 'Start', columnType = 'INTEGER'),
  Column(name = 'End', columnType = 'INTEGER'),
  Column(name = 'Strand', columnType = 'STRING', enumValues = list('+', '-'), maximumSize = 1),
  Column(name = 'TranscriptionFactor', columnType = 'BOOLEAN'))
genes_v1 <- data.frame(
  Name = c("foo", "arg", "zap", "bah", "bnk", "xyz"), 
  Chromosome = c(1,2,2,1,1,1), 
  Start = c(12345,20001,30033,40444,51234,61234),
  End = c(126000,20200,30999,41444,54567,68686),
  Strand = c('+', '+', '-', '-', '+', '+'),
  TranscriptionFactor = c(F,F,F,F,T,F))
genes_v2 <- data.frame(
  Name = c("foo", "arg", "zap", "bah", "bnk", "xyz"), 
  Chromosome = c(1,2,2,1,1,1) + 1, 
  Start = c(12345,20001,30033,40444,51234,61234),
  End = c(126000,20200,30999,41444,54567,68686),
  Strand = c('+', '+', '-', '-', '+', '+'),
  TranscriptionFactor = c(F,F,F,F,T,F))

# test table
schema <- Schema(name = 'test', columns = cols, parent = config$synapse$test_project$id)
test_table_v1 <- Table(schema, genes_v1)
test_table_v1 <- synStore(test_table_v1)
synid_test_table <- test_table_v1$tableId
#synCreateSnapshotVersion(synid_test_table, comment = "first version")
# note: function referenced in API but not implemented, had to do manually through interface
results <- as.data.frame(synTableQuery(glue("select * from {synid_test_table}")))
results[,-c(1:2)] <- genes_v2
test_table_v2 <- Table(synid_test_table, results)
test_table_v2 <- synStore(test_table_v2)
synCreateSnapshotVersion(synid_test_table, comment = "second version")
# note: function referenced in API but not implemented, had to do manually through interface

# test file
write.csv(genes_v1, file = file_csv)
file <- File(path = file_csv, parent = config$synapse$test_project$id)
file <- synStore(file)
synid_test_file <- file$get("id")
write.csv(genes_v2, file = file_csv)
file <- File(path = file_csv, parent = config$synapse$test_project$id)
file <- synStore(file)

#test_that("functon_name", {
# expect_true()
# expect_false()
#})

# main genie helpers --------------------------------------------------------------

test_that("get_main_genie_ids", {
  expect_true(ncol(get_main_genie_ids(synid_table_sample = config$synapse$genie_sample$id, 
                                      patient = T, 
                                      sample = T)) == 2)
  expect_true(ncol(get_main_genie_ids(synid_table_sample = config$synapse$genie_sample$id, 
                                      patient = T, 
                                      sample = F)) == 1)
  expect_true(nrow(get_main_genie_ids(synid_table_sample = config$synapse$genie_sample$id, 
                                      patient = F, 
                                      sample = T)) > 100000)
  expect_false(nrow(get_main_genie_ids(synid_table_sample = config$synapse$genie_sample$id, 
                                      patient = T, 
                                      sample = T)) < 100000)
  expect_true(is.null(get_main_genie_ids(synid_table_sample = config$synapse$genie_sample$id, 
                                       patient = F, 
                                       sample = F)))
  
})

# bpc helpers ---------------------------------------------------------------------

test_that("get_bpc_synid_prissmm", {
  expect_true(get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                    cohort = "BrCa",
                                    file_name = "Import Template") == "syn24409231")
  expect_true(get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                    cohort = "NSCLC",
                                    file_name = "Data Dictionary non-PHI") == "syn25610053")
  expect_false(get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                    cohort = "NSCLC",
                                    file_name = "Data Dictionary non-PHI") == "syn00000000")
})


test_that("get_bpc_data_upload", {
  expect_true(nrow(get_bpc_data_upload(obj = config$uploads$panc_vicc)) > 0)
  expect_true(length(colnames(get_bpc_data_upload(obj = config$uploads$prostate_msk))) > 0)
  expect_false(nrow(get_bpc_data_upload(obj = config$uploads$crc_dfci)) == 0)
})

test_that("get_bpc_version_at_date", {
  
  query <- glue("SELECT id FROM {config$synapse$tables_view$id} WHERE double_curated = 'false' LIMIT 1")
  synid_table <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  expect_true(get_bpc_version_at_date(synid_table = synid_table, 
                                              date_version = "2020-08-26") == 18)
  expect_true(get_bpc_version_at_date(synid_table = synid_table, 
                                      date_version = "2021-05-20") == 32)
  expect_false(get_bpc_version_at_date(synid_table = synid_table, 
                                       date_version = "2021-04-27") == 0)
})

test_that("get_bpc_data_table", {
  
  query <- glue("SELECT id FROM {config$synapse$tables_view$id} WHERE double_curated = 'false' LIMIT 1")
  synid_table <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  
  expect_true(nrow(get_bpc_data_table(synid_table = synid_table, 
                                 cohort = "NSCLC", 
                                 previous = F)) > 0)
  expect_false(nrow(get_bpc_data_table(synid_table = synid_table, 
                                       cohort = "BrCa", 
                                       site = "DFCI", 
                                       previous = F)) == 0)
  expect_false(identical(get_bpc_data_table(synid_table = synid_table, 
                                       cohort = "NSCLC", 
                                       site = "UHN", 
                                       previous = F),
                         get_bpc_data_table(synid_table = synid_table, 
                                            cohort = "NSCLC", 
                                            site = "UHN", 
                                            previous = T)))
  expect_false(identical(get_bpc_data_table(synid_table = synid_table, 
                                            cohort = "NSCLC", 
                                            site = "UHN", 
                                            previous = F),
                         get_bpc_data_table(synid_table = synid_table, 
                                            cohort = "BrCa", 
                                            site = "UHN", 
                                            previous = F)))
  expect_true(identical(get_bpc_data_table(synid_table = synid_table, 
                                            cohort = "BrCa", 
                                            site = "MSK", 
                                            previous = T),
                         get_bpc_data_table(synid_table = synid_table, 
                                            cohort = "BrCa", 
                                            site = "MSK", 
                                            previous = T)))
})

test_that("get_bpc_patient_sample_added_removed", {
  expect_true(length(get_bpc_patient_sample_added_removed(cohort = "BrCa", site = "VICC", 
                                                  check_patient = T, check_added = T)$ids) == 0)
  expect_true(length(get_bpc_patient_sample_added_removed(cohort = "NSCLC", site = "MSK", 
                                                          check_patient = T, check_added = F)$ids) == 2)
  expect_true(length(get_bpc_patient_sample_added_removed(cohort = "CRC", site = "MSK", 
                                                          check_patient = F, check_added = T)$ids) == 1)
  expect_true(length(get_bpc_patient_sample_added_removed(cohort = "PANC", site = "UHN", 
                                                          check_patient = F, check_added = F)$ids) == 2)
  expect_false(length(get_bpc_patient_sample_added_removed(cohort = "PANC", site = "UHN", 
                                                          check_patient = F, check_added = F)$ids) == -1)
})

test_that("get_bpc_index_missing_sample", {
  
  sample_id <- c(NA, "NA", "", "sample_01", NA, "NA", "", "sample_02")
  instrument <- c(rep("instrument_A", 4), rep("instrument_B", 4))
  data <- data.frame(sample_id, instrument)
  colnames(data) <- c(config$column_name$sample_id, config$column_name$instrument)
  
  expect_equal(length(get_bpc_index_missing_sample(data = data,
                                                  instrument = "instrument_A", 
                                                  na.strings = c())), 1)
  expect_equal(length(get_bpc_index_missing_sample(data = data,
                                                   instrument = "instrument_A", 
                                                   na.strings = c("NA"))), 2)
  expect_equal(length(get_bpc_index_missing_sample(data = data,
                                                   instrument = "instrument_A")), 3)
  expect_false(length(get_bpc_index_missing_sample(data = data,
                                                   instrument = "instrument_B", 
                                                   na.strings = c())) == 3)
  expect_false(length(get_bpc_index_missing_sample(data = data,
                                                   instrument = "instrument_B", 
                                                   na.strings = c("NA"))) == 3)
  expect_equal(length(get_bpc_index_missing_sample(data = data,
                                                   instrument = "instrument_A")), 3)
  
})

test_that("get_bpc_instrument_of_variable", {
  expect_equal(get_bpc_instrument_of_variable(variable_name = "birth_year", 
                                              cohort = "NSCLC"), "patient_characteristics")
  expect_equal(get_bpc_instrument_of_variable(variable_name = "ca_stage", 
                                              cohort = "NSCLC"), "cancer_diagnosis")
  expect_false(get_bpc_instrument_of_variable(variable_name = "ca_stage", 
                                              cohort = "NSCLC") == "patient_characteristics")
})

test_that("get_bpc_sor_data_type_single", {
  sor <- get_data(config$synapse$sor$id, sheet = 2)
  expect_equal(get_bpc_sor_data_type_single(var_name = "cohort", sor = sor), "character")
  expect_false(get_bpc_sor_data_type_single(var_name = "drugs_start_time", sor = sor) == "datetime")
  expect_true(get_bpc_sor_data_type_single(var_name = "drugs_start_time", sor = sor) == "character")
  expect_true(is.na(get_bpc_sor_data_type_single(var_name = "this_is_not_a_variable_name", sor = sor)))
})

test_that("get_hemonc_from_ncit", {
  expect_true(is.na(get_hemonc_from_ncit("29796")))
  expect_equal(get_hemonc_from_ncit("362"), 100)
})

test_that("get_bpc_from_ncit", {
  expect_true(is.na(get_hemonc_from_ncit("0")))
  expect_equal(get_bpc_from_ncit("52182"), "AG024322")
  expect_equal(get_bpc_from_ncit(rep("52182", 2)), rep("AG024322", 2))
})

# bpc checks -----------------------------------------------------------------------

# generic helpers ----------------------------------------------------------------------

test_that("is_synapse_table", {
  expect_true(is_synapse_table(synid_test_table))
  expect_false(is_synapse_table(synid_test_file))
  expect_false(is_synapse_table(NULL))
  expect_false(is_synapse_table(NA))
})

test_that("get_data", {
  data_current_table <- get_data(synid_test_table)
  data_current_file <- get_data(synid_test_file)
  data_previous_table <- get_data(synid_test_table, version = 1)
  data_previous_file <- get_data(synid_test_file, version = 1)
  
  expect_equal(nrow(data_current_table), nrow(genes_v2))
  expect_equal(nrow(data_current_file), nrow(genes_v2))
  expect_equal(as.integer(data_current_table$Chromosome), as.integer(data_previous_table$Chromosome) + 1)
  expect_equal(as.integer(data_current_file$Chromosome), as.integer(data_previous_file$Chromosome) + 1)
})

test_that("get_data_filtered", {
  
  col_name <- "Chromosome"
  col_value <- 2
  data_current_table <- get_data_filtered(synid_test_table, col_name = col_name, col_value = col_value, exact = T)
  data_current_file <- get_data_filtered(synid_test_file, col_name = col_name, col_value = col_value, exact = T)
  data_previous_table <- get_data_filtered(synid_test_table, version = 1, col_name = col_name, col_value = col_value, exact = T)
  data_previous_file <- get_data_filtered(synid_test_file, version = 1, col_name = col_name, col_value = col_value, exact = T)
  
  expect_equal(nrow(data_current_table), nrow(genes_v2[genes_v2$Chromosome == col_value,]))
  expect_equal(nrow(data_current_file), nrow(genes_v2[genes_v2$Chromosome == col_value,]))
  expect_equal(nrow(data_previous_table), nrow(genes_v2[genes_v1$Chromosome == col_value,]))
  expect_equal(nrow(data_previous_file), nrow(genes_v2[genes_v1$Chromosome == col_value,]))
  
})

test_that("get_added", {
  expect_identical(get_added(current = c('A','B','C','D'), previous = c('A','B','C')), 'D')
})


test_that("get_removed",{
  expect_identical(get_removed(current = c('B','C','D'), previous = c('A','B','C')), 'A')
})

test_that("get_columns_added_removed", {
  data_previous <- matrix(rnorm(6), ncol = 3, dimnames = list(c(), c("A","B","C")))
  data_current <- matrix(rnorm(6), ncol = 3, dimnames = list(c(), c("A","B","D")))
  
  expect_equal(get_columns_added(data_current, data_previous), "D")
  expect_equal(get_columns_removed(data_current, data_previous), "C")
})

test_that("get_column_elements_added_removed", {
  data_previous <- matrix(c("A1", "A2", "B1", "B2", "C1", "C2"), 
                          ncol = 3, dimnames = list(c(), c("A","B","C")))
  data_current <- matrix(c("A1", "A2", "B1", "B3", "C1", "C2"), 
                         ncol = 3, dimnames = list(c(), c("A","B","C")))
  
  expect_equal(get_column_elements_added(data_current, data_previous), "B3")
  expect_equal(get_column_elements_removed(data_current, data_previous), "B2")
})

test_that("get_date_as_string", {
  expect_equal(get_date_as_string(ts = "2021-07-16 01:02:03 UTC", 
                                  ts_format = "%Y-%m-%d %H:%M:%S", 
                                  tz_current = Sys.timezone(),
                                  tz_target = "US/Pacific"),
               "2021-07-15")
})

test_that("count_not_empty", {
  
  x <- setNames(c("hello", "world", NA, "not empty", ""), c("A", "B", "C", "D", "E"))

  expect_equal(count_not_empty(vector = x), 4)
  expect_equal(count_not_empty(vector = x, exclude = "B"), 3)
  expect_equal(count_not_empty(vector = x, na.strings = c("NA","")), 3)
  expect_equal(count_not_empty(vector = x, exclude = "B", na.strings = c("NA","")), 2)
})

test_that("is_empty", {
  
  x <- setNames(c("hello", NA, ""), c("A", "B", "C"))
  
  expect_false(is_empty(vector = x))
  expect_true(is_empty(vector = x, exclude = c("A", "C")))
  expect_false(is_empty(vector = x, na.strings = c("NA","")))
  expect_true(is_empty(vector = x, na.strings = c("NA",""), exclude = "A"))
})

test_that("is_not_empty", {
  
  x <- setNames(c("hello", NA, ""), c("A", "B", "C"))
  
  expect_true(is_not_empty(vector = x))
  expect_false(is_not_empty(vector = x, exclude = c("A", "C")))
  expect_true(is_not_empty(vector = x, na.strings = c("NA","")))
  expect_false(is_not_empty(vector = x, na.strings = c("NA",""), exclude = "A"))
})

test_that("fraction_empty", {
  expect_equal(fraction_empty(vector = c("",NA,NA,"3")), 0.5)
  expect_equal(fraction_empty(vector = c("",NA,NA,"3"), na.strings = ""), 0.25)
  expect_equal(fraction_empty(vector = c("",NA,NA,"3"), na.strings = "empty"), 0)
  expect_false(fraction_empty(vector = rep("", 100)) == 1)
  expect_true(fraction_empty(vector = rep("", 100)) == 0)
})

test_that("is_double", {
  expect_true(is_double(5))
  expect_true(is_double("10"))
  expect_false(is_double("hello"))
  expect_false(is_double("X123"))
})

test_that("infer_data_type", {
  expect_true(infer_data_type(5) == "numeric")
  expect_true(infer_data_type("10") == "numeric")
  expect_false(infer_data_type("hello") == "numeric")
  expect_false(infer_data_type("X123") == "numeric")
})

test_that("infer_data_type_by_element", {
  expect_true(identical(apply(matrix(c(5,6,7), ncol = 1), 1, infer_data_type), rep("numeric",3)))
  expect_true(identical(apply(matrix(c("over","my","head"), ncol = 1), 1, infer_data_type), rep("character",3)))
  expect_false(identical(apply(matrix(c("world","way",7), ncol = 1), 1, infer_data_type), rep("character",3)))
  expect_false(identical(apply(matrix(c(5,6,"hello"), ncol = 1), 1, infer_data_type), rep("numeric",3)))
})

test_that("get_entities_from_table", {
  expect_equal(length(get_entities_from_table(config$synapse$test_table_view$id)), 5)
})

test_that("is_timestamp_format_correct", {
  expect_true(is_timestamp_format_correct("2021-10-04 08:01:00"))
  expect_false(is_timestamp_format_correct("2021-10-04 08"))
  expect_false(is_timestamp_format_correct("2021-10-04"))
  expect_false(all(is_timestamp_format_correct(c("2021-10-04 08:01:00","2021-10-04 08"))))
  expect_true(all(is_timestamp_format_correct(c("2021-10-04 08:01:00","2021-10-04 08:02"),
                                                formats = c("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"))))
  expect_true(all(is_timestamp_format_correct(c("2021-10-04 08:01:00",NA),
                                              formats = c("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"))))
})

test_that("is_date_format_correct", {
  expect_true(is_date_format_correct("2021-10-04 08:01:00"))
  expect_true(is_date_format_correct("2021-10-04"))
  expect_false(is_date_format_correct("10/04/2021"))
  expect_true(is_date_format_correct("10/04/2021", formats = "%m/%d/%Y"))
  expect_true(is_date_format_correct("10/04/21", formats = "%m/%d/%y"))
  expect_false(all(is_date_format_correct(c("10/04/21","2021-10-04"))))
})

test_that("get_vector_index", {
  expect_equal(get_vector_index(c(1,2), nrow = 3), 4)
  expect_false(get_vector_index(c(1,1), nrow = 10) != 1)
  expect_true(is.na(get_vector_index(c(4,2), nrow = 3)))
  expect_true(is.na(get_vector_index(1, nrow = 3)))
})

# misc -------------------------------------------------------------------------

test_that("capitalize", {
  expect_equal(capitalize("hello"), "Hello")
  expect_equal(capitalize("hello world"), "Hello world")
  expect_false(capitalize("hello world") == "Hello World")
})

# clean up ----------------------------------------------------------------------

# remove example objects
#synDelete(synGet(synid_test_table))
#synDelete(synGet(synid_test_file))
#file.remove(file_csv)


