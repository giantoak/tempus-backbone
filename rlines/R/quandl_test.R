#' Take region names and a comparison date and does diff-in-diff
#' 
#' @param target.region: The region of interest, matching a backpage domain
#' @param comparison.region.set: A set of regions (e.g. c('nova','abilene')) to compare to
#' @param event.date: a YYYY-MM-DD date string for the actual event date
#' @return A list with dd, main_diff, comparison_diff, pre, and post values

quandl_test<-function(start_date='2012-01-01', end_date=''){
  if (length(end_date) < 2){
    # There is no end_date given, so let's use today
    end_date = strftime(Sys.Date(),'%Y-%m-%d')
  }
  Quandl.auth('a4r7bmyM6Yqs6xqVgxZ4')
  mytimeseries <- Quandl("GOOG/NYSE_IBM", type="ts", start_date=start_date, end_date=end_date)
  df<-as.data.frame(mytimeseries[,'Close'])
  df$DateTime<-rownames(df)
  names(df) <- c('Comparison', 'DateTime')
  df$Target <- df$Comparison - 10
  return(list(
    raw=df,
    seasonal=df,
    trend=df,
    remainder=df
    ))
}