/*
MatchIt --> Return comparison groups for a given location

Arguments:
    table: table that contains both treatment and covariate columns
    treatment_col: column name of the column that contains the comparison group
    treatment_selection: row name of the treatment
    covariate_cols: a list of column names
 
 */
CREATE OR REPLACE FUNCTION matchit(data_table TEXT, treatment_col TEXT,
    treatment_selection TEXT, covariate_cols TEXT[]) 
    RETURNS SETOF RECORD AS'
    library(MatchIt)
    num.matches <- 3

    covs <- do.call(paste, c(treatment_col, as.list(covariate_cols), sep=", "))

    not_null_list <- paste(append(covariate_cols, treatment_col), "IS NOT NULL")
    conditions <- do.call(paste, c(as.list(not_null_list), sep=" AND "))
    target <- "target_var"
    score.varname <- "score_var"
    q <- do.call(paste, list("SELECT", covs, 
                             "FROM", data_table,
                             "WHERE", conditions))

    data <- pg.spi.exec(q)

    data[target] <- data[[treatment_col]] == treatment_selection
     
    covs_plus = do.call(paste, c(as.list(covariate_cols), sep=" + "))
    matching.formula <- paste(target, "~", covs_plus)
    match <- matchit(as.formula(matching.formula), data)
    
    data[score.varname] <- match$distance
    target.row <- data[data[[target]],]
    data<-data[!data[target],]
    data<-data[order(data[score.varname], decreasing=TRUE),]
    results <- unique(data[[treatment_col]])[1:num.matches]
    return(results)
' LANGUAGE 'plr' STRICT;


/*
bcp (Bayesian Analysis of Change Point Problems)

Arguments:

       x: a vector or matrix of numerical data (with no missing
          values). For the multivariate change point problems, each
          column corresponds to a series.

      w0: an optional numeric value for the prior, U(0, w0), on the
          signal-to-noise ratio.  If no value is specified, the default
          value of 0.2 is used, as recommended by Barry and Hartigan
          (1993).

      p0: an optional numeric value for the prior, U(0, p0), on the
          probability of a change point at each location in the
          sequence. If no value is specified, the default value of 0.2
          is used, as recommended by Barry and Hartigan (1993).

  burnin: the number of burnin iterations.

    mcmc: the number of iterations after burnin.

return.mcmc: if ‘return.mcmc=TRUE’ the posterior means and the
          partitions after each iteration are returned.
*/
CREATE OR REPLACE FUNCTION bcp(data_table TEXT, burnin INTEGER, mcmc INTEGER) 
    RETURNS SETOF RECORD AS'
    library(bcp)
' LANGUAGE 'plr' STRICT;

/*
Usage:

             breakout(Z, min.size = 30, method = 'amoc', ...)
     
Arguments:

       Z: The input time series. This is either a numeric vector or a
          data.frame which has 'timestamp' and 'count' components.

min.size: The minimum number of observations between change points.

  method: Method must be one of either 'amoc' (At Most One Change) or
          'multi' (Multiple Changes). For 'amoc' at most one change
          point location will be returned.

---
See ?BreakoutDetection::breakout for more arguments, unimplemented here.
*/
CREATE OR REPLACE FUNCTION breakout(data_table TEXT, timestamp_col TEXT,
    data_col TEXT, min_size INTEGER, method TEXT) 
    RETURNS SETOF RECORD AS'
    library(bcp)
' LANGUAGE 'plr' STRICT;

/*
Usage:

     auto.arima(x, d=NA, D=NA, max.p=5, max.q=5,
          max.P=2, max.Q=2, max.order=5, max.d=2, max.D=1, 
          start.p=2, start.q=2, start.P=1, start.Q=1, 
          stationary=FALSE, seasonal=TRUE,
          ic=c("aicc", "aic", "bic"), stepwise=TRUE, trace=FALSE,
          approximation=(length(x)>100 | frequency(x)>12), xreg=NULL,
          test=c("kpss","adf","pp"), seasonal.test=c("ocsb","ch"),
          allowdrift=TRUE, allowmean=TRUE, lambda=NULL, parallel=FALSE, num.cores=2)
     
Arguments:

       x: a univariate time series
...

For additional arguments, see ?forecast::best.arima

*/
CREATE OR REPLACE FUNCTION auto_arima(data_table TEXT, timestamp_col TEXT,
    response_col TEXT)
    RETURNS SETOF RECORD AS '
    
    library(forecast)
    q <- paste("SELECT TRUNC(EXTRACT(year FROM ", timestamp_col, ")) as yr, 
          TRUNC(EXTRACT(month FROM ", timestamp_col, ")) as mo, AVG(", response_col, "), 
          COUNT(", response_col, ") FROM")
    agg <- "GROUP BY yr, mo ORDER BY yr, mo ASC"
    query <- paste(q, data_table, agg)
    df <- pg.spi.exec(query)
    
    res <- auto.arima(df$avg)
    residuals_variance <- res$residuals/var(res$residuals)
    return(residuals_variance)
    
' LANGUAGE 'plr' STRICT;

CREATE OR REPLACE FUNCTION causalimpact(data_table TEXT, preperiod TEXT,
    postperiod TEXT, response_col TEXT, date_col TEXT) RETURNS SETOF RECORD
AS '
    library(CausalImpact)
    df <- pg.spi.exec(paste("SELECT * FROM", data_table))
    
    return "NOT DONE YET"

' LANGUAGE 'plr' STRICT;

CREATE OR REPLACE FUNCTION diffindiff (TEXT, TEXT[], TEXT) RETURNS SETOF RECORD AS '
    library(rlines)
    return(diffindiff(arg1, arg2, arg3))
' LANGUAGE 'plr' STRICT;

-- Not yet working:
/*CREATE OR REPLACE FUNCTION diffindiff_data (target TEXT, comparison TEXT[],
    date TEXT, logged BOOLEAN, normalize BOOLEAN, data_table TEXT,
    date_col TEXT, group_col TEXT, dv_col TEXT, region_col TEXT) RETURNS SETOF
    RECORD AS '
    library(rlines)
    df <- pg.spi.exec(paste("SELECT * FROM", data_table))

    results <- diffindiff_data(target, comparison, date, logged, normalize, df,
        date_col, group_col, dv_col, region_col)

    return(results)

' LANGUAGE 'plr' STRICT;*/

CREATE OR REPLACE FUNCTION diffindiff_data(region TEXT, comparison_regions TEXT[], comparison_date DATE) RETURNS SETOF RECORD AS'
    library(rlines)
    comparisons <- paste("(",toString(paste("''",comparison_regions,"''", sep="")),")", sep="")
    query <- paste("SELECT msaname AS region, to_char(timestamp, ''YYYY-MM'') AS monthdate, count(*) AS counts
        FROM escort_ads
        WHERE msaname = ''", region, "'' OR msaname IN ", comparisons, " 
        GROUP BY region, monthdate
        ORDER BY monthdate", sep = "")

    df <- pg.spi.exec(query)
' LANGUAGE 'plr' STRICT;

CREATE OR REPLACE FUNCTION r_max (integer, integer) RETURNS integer AS '
if (arg1 > arg2)
return(arg1)
else
return(arg2)
' LANGUAGE 'plr' STRICT;
