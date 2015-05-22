#' Take region names and lookup series from a panel
#' 
#' @param target.region: The region of interest, matching a backpage domain
#' @param comparison.region.set: A set of regions (e.g. c('nova','abilene')) to compare to
#' @param data: The data frame to split up - needs columns 'region', 'MonthDate', and 'counts'
#' @return A dataframe with 'MonthDate', target.varname and 'comparison' columns



twolines_data<-function(target.region, comparison.region.set, data,date.var="MonthDate", var.of.interest="counts", group.var="region"){
 print(names(data))
  target.varname<-"Target"
  comparison.varname<-"Comparison"
  output<-data[data[group.var] == target.region,c(date.var,var.of.interest)]
  names(output)<-c(date.var,target.varname)
  comparison<-data[data[[group.var]] %in% comparison.region.set,c(date.var,var.of.interest)]
  comparison<-ddply(comparison, date.var, function(x) {sum(x[var.of.interest])})
  names(comparison)<-c(date.var,comparison.varname)
  output<-merge(output,comparison)
  return(output)
}
