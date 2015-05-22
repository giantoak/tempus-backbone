#' Take region names and a comparison date and does diff-in-diff
#' 
#' @param target.region: The region of interest, matching a backpage domain
#' @param num.matches: the number of matches to return. Defaults to 4.
#' @return A data frame with:
#' - matched regions as rows
#' - features as columns
#' - score as a final column measuring similarity

get_features_data<-function(target.region='dc', num.matches=4,region.var='region', comparison.vars, input_data){
  target.varname<-'target'
  score.varname<-'score'
  data<-input_data[,c(comparison.vars, region.var)] # Select columns to use
  data[target.varname] <- data[[region.var]] == target.region # Create a binary 1 for the region we care about
  #print(data)
  #return(target.region)
  #return(data)
  matching.formula<-paste(target.varname,'~',paste(comparison.vars,collapse=' + '))
  match<-matchit(as.formula(matching.formula), data=data)
  data[score.varname] <- match$distance
  target.row <- data[data[[target.varname]],]
  data<-data[!data[target.varname],]
  data<-data[order(data[score.varname], decreasing=TRUE),]
  data<-data[1:num.matches,]
  data<-rbind(target.row,data)
  data<-subset(data, select=-c(target))
  return(data)
}