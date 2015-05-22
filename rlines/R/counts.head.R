#' Test fucntion
#' 
#' @return some data from counts dataframe

counts.head<-function(region){
  return(counts[counts$region == region,c('MonthDate','counts')])
}