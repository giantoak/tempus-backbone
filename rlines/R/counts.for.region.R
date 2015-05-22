#' Test fucntion
#' 
#' @param region: The region of interest, matching a backpage domain
#' @return some data from counts dataframe

counts.for.region<-function(region){
  return(counts[counts$region == region,c('MonthDate','counts')])
}