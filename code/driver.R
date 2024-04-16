# Vector of additional packages to load before executing the request
config_append('extra_packages', c())

#' Execute the request
#'
#' This function presumes the environment has been set up, and executes the
#' steps of the request.
#'
#' In addition to performing queries and analyses, the execution path in this
#' function should include periodic progress messages to the user, and logging
#' of intermediate totals and timing data through [append_sum()].
#'
#' @return The return value is dependent on the content of the request, but is
#'   typically a structure pointing to some or all of the retrieved data or
#'   analysis results.  The value is not used by the framework itself.
#' @md
.run  <- function() {

    setup_pkgs() # Load runtime packages as specified above

    message('Starting execution with framework version ',
          config('framework_version'))

  # Set up the step log with as many attrition columns as you need.
  # For example, this call sets up the log with a `persons` count that will be
  # required at each step.
  init_sum(cohort = 'Start', persons = 0)

  # By convention, accumulate execution results in a list rather than as
  # independent variables, in order to make returning the entire set easier
  rslt <- list()

  message('Insert message here')
  rslt$starter <- first_function(config('db_src')) %>%
    # If final table, output here.  If you need a temp or raw table, use
    # compute_new(), and see below for ID replacement
    output_tbl(name = 'first_tbl', indexes = list('person_id'))

  # counts the number of persons for this part of the query
  append_sum(cohort = 'Name of first computation or cohort',
             persons = distinct_ct(rslt$starter))


  rslt$next_cohort <- function_call_for_study(rslt$starter) %>%
    .other_functions_if_applicable('') %>% # if applicable
    output_tbl(name = 'next_tbl', indexes = list('person_id', 'visit_occurrence_id'))
  append_sum(cohort = 'Name of second computation or cohort',
             persons = distinct_ct(rslt$next_cohort))


  more_functions_as_needed()


  # If needed, replace person_ids and write output
  pers_xwalk <- gen_xwalk(rslt$starter, 'person_id')
  output_tbl(new_id(rslt$starter, 'person_id', pers_xwalk, replace = TRUE),
             name = 'base_cohort')
  output_tbl(new_id(rslt$next_cohort, 'person_id', pers_xwalk, replace = TRUE),
             name = 'next_cohort')
  # Don't forget to save the crosswalk
  output_tbl(pers_xwalk, name = 'person_xwalk', local = TRUE)

  # Write step summary log to CSV and/or database,
  # as determined by configuration
  output_sum()

  message('Done.')

  invisible(rslt)

}
