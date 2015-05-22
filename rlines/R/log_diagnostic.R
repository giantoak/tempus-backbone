#' Determine whether to apply logs to a data column
#' 
#' @param df: The data frame that will be analyzed
#' @param column: The column that will be used 
#' @return A list with values:
#' log: a boolean which is TRUE if log is better, and FALSE if not

log_diagnostic<-function(df, column){
  data<-df[[column]]
  if (min(data) < 0){
    return(list(log=FALSE))
  } else{
    ld<-log(data)
    log.skewness<-skewness(ld)
    log.sd<-sd(ld)
    log.skewness.factor<-log.skewness/log.sd
    level.skewness<- skewness(data)
    level.sd <-sd(data)
    level.skewness.factor<-level.skewness/level.sd
    skewness.factor<-abs(log.skewness.factor)/abs(level.skewness.factor)
    out<-list(level.sd=level.sd, level.skewness=level.skewness, log.skewness=log.skewness, log.sd=log.sd)
    if (skewness.factor > 1 & level.skewness > 0){
      # This means that skewness is more positive before logging
      out['log']<-TRUE
    } else{
      out['log']<-FALSE
    }
  }
  return(out)
}