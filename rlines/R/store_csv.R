#' Take region names and lookup series from a panel
#' 
#' @param data: a csv file which is written to disk
#' @return The dimensions of the loaded dataframe



store_csv<-function(data, t='asfs'){
  #d<-file
  #print(file)
  #print(data)
  #temp<-1
  df <-read.csv(file=data)
  #print(summary(df))
  #con<-textConnection(file)
  #df<-read.csv(con)
  # return(dim(df))
  return(df)
}