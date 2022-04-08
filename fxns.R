# Description: main file of functions for performing GENIE BPC file QA checks.
# Authors: Alyssa Acebedo, Xindi Guo, Haley Hunter-Zinck
# Date: June 7, 2021

# reading functions ------------------------------------

#' Determine if Synapse ID refers to a table
#'
#' @param synid Synapse ID
#' @return T if synid refers to a table; F otherwise
#' @examples
#' is_synapse_table("syn12345")
is_synapse_table <- function(synid) {

  if (is.null(synid) || is.na(synid)) {
    return(F)
  }

  entity <- synGet(as.character(synid), downloadFile = F)
  file_type <- entity$properties$concreteType

  if (length(file_type) == 0) {
    return(F)
  }

  if (length(grep(pattern = "table", x = file_type)) > 0) {
    return(T)
  }

  return(F)
}

#' Get file name of Synapse file from the Synapse ID.
#' 
#' @param synid Synapse ID
#' @return String representing the name of the file.
#' @example 
#' get_synid_file_name("syn12345")
#' get_synid_file_name(c("syn12345", "syn6789"))
get_synid_file_name <- function(synids) {
  
  file_names <- c()
  
  for(synid in synids) {
    ent <- synGet(synid)
    file_names <- append(file_names, ent$get("name"))
  }
  
  return(file_names)
}

#' Get synapse IDs of files or tables listed in a table according
#' to the condition on the file attributes, if specified.
#' 
#' @param synid Synapse ID of tables containing Synapse entity list.
#' @param condition boolean condition in SQL syntax
#' @return Vector of Synapse IDs 
#' @example 
#' get_synids_from_table("synapse12345", condition = "my_column = 'my_value'")
get_synid_from_table <- function(synid, condition = NA, with_names = F) {
  
  query <- glue("SELECT id, name FROM {synid}")
  if (!is.na(condition)) {
    
    query <- paste0(query, glue(" WHERE {condition}"))
  }
  
  res <- as.data.frame(synTableQuery(query))
  ids <- res$id
  
  if (with_names) {
    
    names <- get_synid_file_name(ids)
    return(setNames(ids, names))
  }
  
  return(ids)
}

#' Get all data stored at a Synapse ID
#'
#' @param synid Synapse ID
#' @param version Version of the Synapse ID entity to retrieve
#' @param sheet Sheet of excel file to read, if synid points to an .xslx file
#' @return Data frame repereneting stored at the Synapse ID entity.
#' @example
#' get_data(synid = "syn12345", version = 1)
get_data <- function(synid, version = NA, sheet = 1) {

  # determine synapse id data type and query table or read csv
  if (is_synapse_table(synid)) {

    if (is.na(version)) {
      query <- glue("SELECT * FROM {synid}")
    } else {
      query <- glue("SELECT * FROM {synid}.{version}")
    }

    results <- synTableQuery(query)
    data <- as.data.frame(results)
  } else {

    ent <- synGet(as.character(synid), version = version)
    
    if(grepl(pattern = "\\.xlsx$", x = ent$properties$name, fixed = F)) {
      data <- read.xlsx(ent$path, check.names = FALSE, sheet = sheet)
    } else {
      data <- suppressWarnings(read.csv(ent$path, check.names = F,
                                        na.strings = c(""), 
                                        stringsAsFactors = F,
                                        colClasses = "character"))
    }
  }

  return(data)
}

get_data_filtered_table <- function(synid, column_name, col_value, select = NA,
                                    version = NA, exact = F) {
  
  # filter with where clause
  where_clause <- ""
  for(i in seq_len(length(column_name))) {
    
    if(i == 1) {
      where_clause <- "WHERE"
    } else {
      where_clause <- glue("{where_clause} AND")
    }
    
    if(exact[i]) {
      where_clause <- glue("{where_clause} {column_name[i]} = '{col_value[i]}'")
    } else {
      where_clause <- glue("{where_clause} {column_name[i]} LIKE '%{col_value[i]}%'")
    }
  }
  
  select_clause <- "*"
  if (!is.na(select[1])) {
    select_clause <- paste0(select, collapse = ', ')
  }
  
  # get filtered data query for specified version
  if (is.na(version)) {
    query <- glue("SELECT {select_clause} FROM {synid} {where_clause}")
  } else {
    query <- glue("SELECT {select_clause} FROM {synid}.{version} {where_clause}")
  }
  
  results <- synTableQuery(query, includeRowIdAndRowVersion = F)
  data <- as.data.frame(results)
  
  return(data)
}

get_data_filtered_file <- function(synid, column_name, col_value, select = NA,
                                   version = NA, exact = F) {
  
  ent <- synGet(as.character(synid), version = version)
  data <- read.csv(ent$path, check.names = F, na.strings = c(""),
                   stringsAsFactors = F,
                   colClasses = "character")
  
  idx_filter <- c()
  for(i in seq_len(length(column_name))) {
    if (exact[i]) {
        idx_new <- which(data[, column_name[i]] == col_value[i])
      
    } else {
        idx_new <- grep(pattern = col_value[i], x = data[, column_name[i]], fixed = T)
    }
    
    idx_filter <- unique(append(idx_filter, idx_new))
  }
  
  if (length(idx_filter) > 0) {
    data <- data[idx_filter, ]
  } else {
    data <- NULL
  }

  if (is.na(select[1])) {
    return(data)
  }
  
  return(data[,select])
}

#' Get all data stored at a Synapse ID that corresponds to a specified
#' value on the specified column.
#'
#' @param synid Synapse ID
#' @param column_name Name of a column(s)
#' @param col_value Value(s) in named column from which to select rows
#' @param version Version of the Synapse ID entity to retrieve
#' @param exact If T perform an exact match for col_value;
#' otherwise, pattern match.
#' @return Data frame repereneting stored at the Synapse ID entity.
#' @example
#' get_data_filtered(synid = "syn12345", column_name = "cohort",
#' col_value = "my_favorite_cohort")
#' (synid = "syn12345", column_name = c("cohort", "record_id"),
#' col_value = c("my_favorite_cohort", "-my_favorite_site-"), exact = c(T,F))
get_data_filtered <- function(synid, column_name, col_value, select,
                              version = NA, exact = F) {

  # determine synapse id data type and query table or read csv
  if (is_synapse_table(synid)) {
    data <- get_data_filtered_table(synid = synid, 
                                    column_name = column_name,
                                    col_value = col_value,
                                    select = select,
                                    version = version,
                                    exact = exact)
   } else {
    data <- get_data_filtered_file(synid = synid, 
                                   column_name = column_name,
                                   col_value = col_value,
                                   select = select,
                                   version = version,
                                   exact = exact)
   }

  return(data)
}

#' Get Synapse entity IDs from a table view on Synapse.
#' 
#' @param synapse_id Synapse ID of table representing a file view.
#' @return Vector of Synapse IDs.
#' @example 
#' get_entities_from_table(synapse_id = "syn1234")
get_entities_from_table <- function(synapse_id, condition = NA) {
  
  if (is.na(condition)) {
    query <- glue("SELECT id FROM {synapse_id}")
  } else {
    query <- glue("SELECT id FROM {synapse_id} WHERE {condition}")
  }
  res <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  
  return(as.character(unlist(res)))
}

get_synapse_folder_children <- function(synapse_id, 
                                        include_types=list("folder", "file", "table", "link", "entityview", "dockerrepo")) {
  
  ent <- as.list(synGetChildren(synapse_id, includeTypes = include_types))
  
  children <- c()
  
  if (length(ent) > 0) {
    for (i in 1:length(ent)) {
      children[ent[[i]]$name] <- ent[[i]]$id
    }
  }
  
  return(children)
}

#' Get a synapse ID by following a traditional file path from a root synapse folder entity.
#' 
#' @param synid_folder_root Synapse ID of the root folder
#' @param path Folder path starting in the first subfolder ending in desired folder and delimited with '/'
#' @return Synapse ID of the final subfolder in the path
#' @example get_folder_synid_from_path("syn12345", "first/second/final")
get_folder_synid_from_path <- function(synid_folder_root, path) {
  
  synid_folder_current <- synid_folder_root
  subfolders <- strsplit(path, split = "/")[[1]]
  
  for (i in 1:length(subfolders)) {
    synid_folder_children <- get_synapse_folder_children(synid_folder_current, 
                                                         include_types = list("folder"))
    
    if (!is.element(subfolders[i], names(synid_folder_children))) {
      return(NA)
    }
    
    synid_folder_current <- as.character(synid_folder_children[subfolders[i]])
  }
  
  return(synid_folder_current)
}

#' Get the Synapse ID of a named file entity given a file path and root Synapse ID. 
#' 
#'  @param synid_folder_root  Synapse ID of the root folder
#'  @param path Folder path starting in the first subfolder ending in desired 
#'  file and delimited with '/'
#'  @return Synapse ID of file entity
#'  @example get_file_synid_from_path("syn12345", "first/second/my_file.csv")
get_file_synid_from_path <- function(synid_folder_root, path) {
  
  path_part <- strsplit(path, split = "/")[[1]] 
  file_name <- tail(path_part, n = 1)
  path_abbrev <- paste0(path_part[1:(length(path_part) - 1)], collapse = "/")
  
  synid_folder_dest <- get_folder_synid_from_path(synid_folder_root, 
                                                  path_abbrev)
  
  synid_folder_children <- get_synapse_folder_children(synid_folder_dest, 
                                                       include_types = list("file"))
  
  if (!is.element(file_name, names(synid_folder_children))) {
    return(NA)
  }
  
  return(as.character(synid_folder_children[file_name]))
}


# helper functions ------------------------------------

#' Get elements added to current from previous instance of a vector.
#'
#' @param current Current instance of the vector
#' @param previous Previous instance of the vector
#' @return Elements in current but not in previous (added).
#' @example
#' get_added(current = c('A','B','C','D'), previous = c('A','B','C'))
get_added <- function(current, previous) {
  return(setdiff(current, previous))
}

#' Get elements removed from current relative to previous
#' instance of a vector.
#'
#' @param current Current instance of the vector
#' @param previous Previous instance of the vector
#' @return Elements in previous but not current (removed).
#' @example
#' get_removed(current = c('B','C','D'), previous = c('A','B','C'))
get_removed <- function(current, previous) {
  return(setdiff(previous, current))
}

get_vector_index <- function(array_index, nrow) {
  
  if (length(array_index) != 2) {
    return(NA)
  }
  
  row_index <- array_index[1]
  col_index <- array_index[2]
  
  if (row_index > nrow) {
    return(NA)
  }
  
  vector_index <- (col_index - 1) * nrow + row_index
  return(vector_index)
}

#  vector functions -----------------------------------

#' Count the number of non-empty elements in a vector.
#' 
#' @param vector Vector to examine
#' @param exclude Names of elements to exclude from the count.
#' @param na.strings Strings representing empty elements
#' @return Number of non-excluded elements in the vectors that do
#' not match any empty values.
#' @example 
#' count_not_empty(c(1,NA,2), exclude = NA, na.strings = NA)
count_not_empty <- function(vector, exclude = NA, na.strings = NA) {
  
  mod <- vector
  idx_na <- c()
  
  if(!(length(exclude) == 1 && is.na(exclude))) {
    idx <- which(is.element(names(vector), exclude))
    if(length(idx) > 0) {
      mod <- mod[-idx]
    }
  }
  
  idx_na <- which(is.na(mod) | is.element(mod, na.strings))
  return(length(mod) - length(idx_na))
}

#' Determine if a vector is empty.
#' 
#' @param vector Vector to examine
#' @param exclude Names of elements to exclude from the count.
#' @param na.strings Strings representing empty elements
#' @return TRUE if number of non-excluded elements in the vectors is 0;
#' FALSE, otherwise.
#' @example 
#' is_empty(c(1,NA,2), exclude = NA, na.string = NA)
is_empty <- function(vector, exclude = NA, na.strings = NA) {
  
  return(!count_not_empty(vector, exclude, na.strings))
}

#' Determine if a vector is not empty.
#' 
#' @param vector Vector to examine
#' @param exclude Names of elements to exclude from the count.
#' @param na.strings Strings representing empty elements
#' @return TRUE if number of non-excluded elements in the vectors is >=1;
#' FALSE, otherwise.
#' @example 
#' is_not_empty(c(1,NA,2), exclude = NA, na.string = NA)
is_not_empty <- function(vector, exclude = NA, na.strings = NA) {
  return(as.logical(count_not_empty(vector, exclude, na.strings)))
}

fraction_empty <- function(vector, na.strings = NA) {
  return(length(which(is.element(vector, na.strings))) / length(vector))
}

is_double <- function(x) {
  
  res <- tryCatch(
    {
      is.double(as.double(x))
    }, error = function(cond) {
      return(FALSE)
    }, warning = function(cond) {
      return(FALSE)
    }, finally = {
    }
  )
  
  return(res)
}

infer_data_type <- function(x) {
  
  if (is_double(x)) {
    return("numeric")
  }
  
  return("character")
}

# column functions ------------------------------------

#' Get columns added to current file or table relative to previous instance
#'
#' @param data_current matrix or data frame of current file or table
#' @param data_previous matrix or data frame of previous file or table
#' @return columns in the current but not previous file or table
get_columns_added <- function(data_current, data_previous) {
  return(get_added(current = colnames(data_current),
                   previous = colnames(data_previous)))
}

#' Get columns removed from current file or table relative to previous instance
#'
#' @param data_current matrix or data frame of current file or table
#' @param data_previous matrix or data frame of previous file or table
#' @return columns in the previous but not current file or table
get_columns_removed <- function(data_current, data_previous) {
  return(get_removed(current = colnames(data_current),
                     previous = colnames(data_previous)))
}

#' Get elements from the specified column that were added
#' to the current representation relative to a previous version.
#' 
#' @param data_current matrix or data frame of current file or table
#' @param data_previous matrix or data frame of previous file or table
#' @return elements of column in the current but not previous version
get_column_elements_added <- function(data_current, data_previous, column_name) {
  return(get_added(data_current[,column_name], data_previous[,column_name]))
}

#' Get elements from the specified column that were removed
#' from the current representation relative to a previous version.
#' 
#' @param data_current matrix or data frame of current file or table
#' @param data_previous matrix or data frame of previous file or table
#' @return elements of column in the previous but not current version
get_column_elements_removed <- function(data_current, data_previous, column_name) {
  return(get_removed(data_current[,column_name], data_previous[,column_name]))
}

# date and time functions -------------------------------------

#' For a timestamp in a given format and time zone, convert time zone 
#' to desired time zone and return timestamp as a string.
#' 
#' @param ts Time stamp
#' @param ts_format Format of ts
#' @param tz_current Time zone of ts
#' @param tz_target Target time zone to which ts should be converted
#' @return ts in tz_target time zone, represented as a string.
get_date_as_string <- function(ts, ts_format = "%Y-%m-%dT%H:%M:%OS", 
                               tz_current = Sys.timezone(),
                               tz_target = "US/Pacific") {
  
  ts_posix <- as.POSIXct(ts, tz = tz_current, format = ts_format)
  ts_mod <- as.character(as.Date(ts_posix, tz = tz_target))
  
  return(ts_mod)
}

#' Check that timestamps 
is_timestamp_format_correct <- function(timestamps, 
                                       formats = c("%Y-%m-%d %H:%M:%S")) {
  res <- as.POSIXct(x = as.character(timestamps), tryFormats = formats, optional = T)
  
  idx_na <- which(is.na(timestamps))
  if (length(idx_na)) {
    res[idx_na] <- Sys.time()
  }
  
  return(!is.na(res))
}

is_date_format_correct <- function(dates, formats = c("%Y-%m-%d")) {
  
  res <- is_timestamp_format_correct(timestamps = as.character(dates), 
                                     formats = formats)
  return(res)
}

# synapse -----------------------------

#' Extract personal access token from .synapseConfig
#' located at a custom path. 
#' 
#' @param path Path to .synapseConfig
#' @return personal acccess token
get_auth_token <- function(path) {
  
  lines <- scan(path, what = "character", sep = "\t", quiet = T)
  line <- grep(pattern = "^authtoken = ", x = lines, value = T)
  
  token <- strsplit(line, split = ' ')[[1]][3]
  return(token)
}

#' Override of synapser::synLogin() function to accept 
#' custom path to .synapseConfig file or personal authentication
#' token.  If no arguments are supplied, performs standard synLogin().
#' 
#' @param auth full path to .synapseConfig file or authentication token
#' @param silent verbosity control on login
#' @return TRUE for successful login; F otherwise
#' Override of synapser::synLogin() function to accept 
#' custom path to .synapseConfig file or personal authentication
#' token.  If no arguments are supplied, performs standard synLogin().
#' 
#' @param auth full path to .synapseConfig file or authentication token
#' @param silent verbosity control on login
#' @return TRUE for successful login; F otherwise
synLogin <- function(auth = NA, silent = T) {
  
  secret <- Sys.getenv("SCHEDULED_JOB_SECRETS")
  if (secret != "") {
    # Synapse token stored as secret in json string
    syn = synapser::synLogin(silent = T, authToken = fromJSON(secret)$SYNAPSE_AUTH_TOKEN)
  } else if (auth == "~/.synapseConfig" || is.na(auth)) {
    # default Synapse behavior
    syn <- synapser::synLogin(silent = silent)
  } else {
    
    # in case pat passed directly
    token <- auth
    
    # extract token from custom path to .synapseConfig
    if (grepl(x = auth, pattern = "\\.synapseConfig$")) {
      token = get_auth_token(auth)
      
      if (is.na(token)) {
        return(F)
      }
    }
    
    # login with token
    syn <- tryCatch({
      synapser::synLogin(authToken = token, silent = silent)
    }, error = function(cond) {
      return(F)
    })
  }
  
  # NULL returned indicates successful login
  if (is.null(syn)) {
    return(T)
  }
  return(F)
}

#' Store a file on Synapse with options to define provenance.
#' 
#' @param path Path to the file on the local machine.
#' @param parent_id Synapse ID of the folder or project to which to load the file.
#' @param file_name Name of the Synapse entity once loaded
#' @param prov_name Provenance short description title
#' @param prov_desc Provenance long description
#' @param prov_used Vector of Synapse IDs of data used to create the current
#' file to be loaded.
#' @param prov_exec String representing URL to script used to create the file.
#' @return Synapse ID of entity representing file
save_to_synapse <- function(path, 
                            parent_id, 
                            file_name = NA, 
                            prov_name = NA, 
                            prov_desc = NA, 
                            prov_used = NA, 
                            prov_exec = NA) {
  
  if (is.na(file_name)) {
    file_name = path
  } 
  file <- File(path = path, parentId = parent_id, name = file_name)
  
  if (!is.na(prov_name) || !is.na(prov_desc) || !is.na(prov_used) || !is.na(prov_exec)) {
    act <- Activity(name = prov_name,
                    description = prov_desc,
                    used = prov_used,
                    executed = prov_exec)
    file <- synStore(file, activity = act)
  } else {
    file <- synStore(file)
  }
  
  return(file$properties$id)
}

#' Determine if a Synapse entity is a CSV like file.  
#' 
#' @param synapse_id Synapse ID
#' @return TRUE is text/csv file type; FALSE otherwise
#' @example is_synapse_entity_csv("syn12345")
is_synapse_entity_csv <- function(synapse_id) {
  
  data <- tryCatch({
    get_data(synapse_id)
  }, error = function(cond) {
    return(NULL)
  })
  
  if (is.null(data)) {
    return(F)
  }
  return(T)
}

# misc -------------------------------

#' Print out a current timestamp.  Mostly used for debuggging statements.
#'
#' @param timeOnly Indicate whether to remove date from the timestamp
#' @param tz Time zone designation for the timestamp
#' @return String representing the current timestamp.
#' @example
#' now(timeOnly = T)
now <- function(timeOnly = F, tz = "US/Pacific") {

  Sys.setenv(TZ = tz)

  if (timeOnly) {
    return(format(Sys.time(), "%H:%M:%S"))
  }

  return(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
}

#' Function to wait indefinitely upon a certain condition.
#' 
#' @param cond Boolean value, usually from an evaluated condition
#' @param msg A string to print upon condition being TRUE
#' @return NA
waitifnot <- function(cond, msg = "") {
  if (!cond) {
    
    for (str in msg) {
      message(str)
    }
    message("Press control-C to exit and try again.")
    
    while(T) {}
  }
}

#' Capitalize the first letter in a string.  
#' 
#' @param str Character string
#' @return Character string with first letter capatilized
#' @example capitalize("hello world")
capitalize <- function(str) {
  first <- toupper(substr(str, 1, 1))
  suffix <- tolower(substr(str, 2, nchar(str)))
  
  return(glue(first, suffix))
}

# string operations -------------------

trim_string <- function(str) {
  front <- gsub(pattern = "^[[:space:]]+", replacement = "", x = str)
  back <- gsub(pattern = "[[:space:]]+$", replacement = "", x = front)
  
  return(back)
}

merge_last_elements <- function(x, delim) {
  
  y <- c()
  y[1] = x[1]
  y[2] <- paste0(x[2:length(x)], collapse = delim)
  return(y)
}

#' Perform string split operation but only on the first
#' occurrence of the split character.
strsplit_first <- function(x, split) {
  
  unmerged <- strsplit(x = x, split = split)
  remerge <- lapply(unmerged, merge_last_elements, delim = split)
  
  return(remerge)
}

#' Parse a REDCap data dictionary choices mapping string.
#' @param str REDCap data dictionary mapping string
#' @return data frame
parse_mapping <- function(str) {
  
  clean <- trim_string(gsub(pattern = "\"", replacement = "", x = str))
  splt <- strsplit_first(strsplit(x = clean, split = "|", fixed = T)[[1]], split = ",")
  
  codes <- unlist(lapply(splt, FUN = function(x) {return(trim_string(x[1]))}))
  values <- unlist(lapply(splt, FUN = function(x) {return(trim_string(x[2]))}))
  mapping <- data.frame(cbind(codes, values), stringsAsFactors = F)
  
  return(mapping)
}
