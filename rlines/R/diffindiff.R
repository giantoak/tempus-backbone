#' Take region names and a comparison date and does diff-in-diff
#' 
#' @param target.region: The region of interest, matching a backpage domain
#' @param comparison.region.set: A set of regions (e.g. c('nova','abilene')) to compare to
#' @param event.date: a YYYY-MM-DD date string for the actual event date
#' @param logged: A boolean as to whether to perform a log transform on the data first
#' @return A list with dd, main_diff, comparison_diff, pre, and post values



diffindiff<-function(target.region, comparison.region.set, event.date, logged=FALSE, normalize=FALSE){
  data=twolines(target.region=target.region, comparison.region.set=comparison.region.set)
  data$date<-as.Date(data$MonthDate, "%Y-%m-%d")
  data$MonthDate <-NULL
  
  ed<-as.Date(event.date, "%Y-%m-%d")
  
  data$post = data$date > ed
  data <- data[data$date > as.Date("2013-09-01","%Y-%m-%d"),]
  data <- data[data$date < as.Date("2014-06-01","%Y-%m-%d"),]
  data<-melt(data, id=c("date","post"), variable.name="group", value.name="counts")
  if (logged){
    data$counts<-log(1+data$counts)
  }
  pre.target.avg<-mean(data[data$post==FALSE & data$group == "Target",'counts'])
  pre.comparison.avg<-mean(data[data$post==FALSE & data$group == "Comparison",'counts'])
  if (normalize){
   data$counts[data$group == "Comparison"] <- data$counts[data$group == "Comparison"] * pre.target.avg/pre.comparison.avg
  }
  data <- within(data, group <- relevel(group, ref = "Comparison")) # Set comparison as base group
  model<-lm(counts ~ post*group, data=data)
  msum<-summary(model)
  df<-msum$df[2]
  print(summary(model))
  
  # The idea for this model is for the results to be in the order:
  #  1: (Intercept)           
  #  2: postTRUE            
  #  3: groupTarget         
  #  4: postTRUE:groupTarget
  model.results<-coef(summary(model))
  vcov.matrix<-vcov(model)
  dd<-list(
    b=model.results[4,'Estimate'], 
    se=model.results[4, "Std. Error"],
    t=model.results[4,"t value"]
  )
  dd$p<-2*pt(-abs(dd$t),df=df-1)
  # The diff-in-diff estimate is just 4: postTRUE:groupTarget
#  target.change<-
#    list(
#      b=model.results[1,'Estimate'], 
#      se=model.results[1, "Std. Error"],
#      t=model.results[1,"t value"]
#    )# The pre-post difference in the target variable
  target.change.vec<-c(0,1,0,1)
  # The target change is the sum of the 2nd and 4th variables (set target=True and change post from 1 to 0)
  b.target<-target.change.vec %*% model.results[,'Estimate']
  se.target<-sqrt(target.change.vec %*% vcov.matrix %*% target.change.vec)
  target.change<-list(
    b=b.target[1,1],
    se=se.target[1,1],
    t=b.target[1,1]/se.target[1,1]
    )
  target.change$p<-2*pt(-abs(target.change$t),df=df-1)
  # Note: we have to compute p manually since non-unit vectors will have covariance terms in SE
  # and hence won't be in the main results
  
  comparison.vec<-c(0,1,0,0)
  # The comparison group is the sum of the 1st and 4th variables
  b.comparison<-comparison.vec %*% model.results[,'Estimate']
  se.comparison<-sqrt(comparison.vec %*% vcov.matrix %*% comparison.vec)
  comparison.change<-list(
    b=b.comparison[1,1],
    se=se.comparison[1,1],
    t=b.comparison[1,1]/se.comparison[1,1]
  )
  comparison.change$p<-2*pt(-abs(comparison.change$t),df=df-1)
  # Note: we have to compute p manually since non-unit vectors will have covariance terms in SE
  # and hence won't be in the main results
#comparison.change<-mean(data[data$group == "comparison" & data$post,'counts']) - mean(data[data$group == "comparison" & data$post == FALSE,'counts'])
  comparison<-data[data$group == "Comparison",c('date','counts')]
  comparison$date <- strftime(comparison$date,"%Y-%m-%d")
  target<-data[data$group == "Target",c('date','counts')]
  target$date <- strftime(target$date,"%Y-%m-%d")
  data<-reshape2::dcast(data=data, formula=date~group, value=counts)
  return(list(data=data,
              comparison=comparison,
              target=target,
              #model=model, 
              diff_in_diff=dd, 
              target_diff=target.change, 
              comparison_diff=comparison.change))
}