#' Tabulate the unique values of a given column
#' 
#' @param data: The data frame that will be analyzed
#' @param column: The column that will be used 
#' @return A list of unique values from the column

get_unit_names<-function(data, column){
  return(levels(data[[column]]))
}