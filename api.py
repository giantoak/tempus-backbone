from flask import Flask, make_response, request
from initdb import tables, session, table_cols, table_conf

import agg
import json
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig()

app = Flask(__name__)

@app.route('/api')
def api_root():
    ''' GET Tempus configuration file '''
    return make_response(json.dumps(table_cols))

@app.route('/api/arima')
def api_arima():
    data['table'], data['response']

@app.route('/api/outliers')
def api_outliers():
    data = request.args
    outliers = agg.outliers(data['table'], data['group_col'],
                            data['response_col'])
    logger.debug(outliers)
    return make_response(json.dumps(outliers))

@app.route('/api/diffindiff')
def api_diffindiff():
    data = request.args
    covs = data['covs'].split('|')
    comps = agg.get_comparisons(data['table'], data['group_col'],
                                data['group'], covs)

    return dd

@app.route('/api/comparison')
def api_get_comparison():
    data = request.args
    covs = data['covs'].split('|')
    try:
        comps = agg.get_comparisons(data['table'], data['group_col'],
                                    data['group'], covs)
    except ValueError:
        raise

    cdata = agg.get_comparison_ts(data['table'], data['group_col'],
                                  comps, data['response_col'],
                                  sort=data.get('sort', False))
    response = []
    for (dt, v) in cdata:
        response.append((str(dt), v))
    return make_response(json.dumps({'result': response,
                                    'groups': comps
                                    }))

@app.route('/api/series')
def api_get_series():
    data = request.args
    res = agg.get(data['table'], data['response_col'],
            data.get('group_col', None), data.get('group', None),
            start=data.get('start', None), end=data.get('end', None),
            sort=data.get('sort', False))

    response = []
    for (dt, v) in res:
        response.append((str(dt), v))
    return make_response(json.dumps({'result': response}))

if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    app.run(debug=True, port=int(sys.argv[1]))
