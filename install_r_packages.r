install.packages(c('plyr','reshape2','Quandl','MASS','MatchIt','TSA', 'devtools',
                   'bcp', 'forecast'),
                 repos='http://cran.rstudio.com/')

install.packages('/home/ubuntu/giantoak/tempus-backbone/rlines/R/diffindiff_data.R', repos = NULL, type = 'source')

devtools::install_github("google/CausalImpact")
devtools::install_github("twitter/BreakoutDetection")
