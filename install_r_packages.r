install.packages(c('plyr','reshape2','Quandl','MASS','MatchIt','TSA', 'devtools',
                   'bcp', 'forecast'),
                 repos='http://cran.rstudio.com/')

devtools::install_github("google/CausalImpact")
devtools::install_github("twitter/BreakoutDetection")
