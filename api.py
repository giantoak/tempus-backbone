from functools import wraps
from flask import Flask, make_response, request, jsonify
from initdb import tables, session, table_cols, table_conf
import agg
import json
import logging
import sys
from jsonschema import validate
from jsonschema.exceptions import ValidationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig()

app = Flask(__name__)

def validate_schema(schema):
    def decorator(f):
        @wraps(f)
        def validation_wrapper(*args, **kwargs):
            # Without any arguments, return the raw schema
            if len(request.args) == 0:
                return jsonify(schema), 200
            try:
                validate(request.args, schema)
            except ValidationError, e:
                return jsonify({"error": e.message}), 400
            return f(*args, **kwargs)
        return validation_wrapper
    return decorator

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

comparison_schema = {
        "title": "Comparison group selection",
        "description": "Select comparison groups based off a comparison"\
                       " column, a target group, a set of covariates, and a"\
                       " response variable.",
        "type": "object",
        "properties": {
            "table": {
                "type": "string"
                },
            "group_col": {
                "type": "string"
                },
            "group": {
                "type": "string"
                },
            "covs": {
                "type": "string"
                },
            "response_col": {
                "type": "string"
                }
            },
        "required": ["table", "group_col", "group", "covs", "response_col"]
        }
@app.route('/api/comparison')
@validate_schema(comparison_schema)
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

get_series_schema = {
        "title": "Time series selection",
        "description": "Get raw counts of a response variable by time series"\
                       ", optionally filtering by a groupable variable.",
        "type": "object",
        "properties": {
            "table": {
                "type": "string"
                },
            "group_col": {
                "type": "string"
                },
            "group": {
                "type": "string"
                },
            "start": {
                "type": "string"
                },
            "end": {
                "type": "string"
                },
            "response_col": {
                "type": "string"
                },
            "sort": {
                "type": "string"
                }
            },
        "required": ["table", "response_col"]
        }
@app.route('/api/series')
@validate_schema(get_series_schema)
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

get_groups_schema = {
        "title": "Display group selection",
        "description": "Show all variables within a given groupable column",
        "type": "object",
        "properties": {
            "table": {
                "type": "string"
                },
            "group_col": {
                "type": "string"
                }
            },
        "required": ["table", "group_col"]
        }
@app.route('/api/groups')
@validate_schema(get_groups_schema)
def api_get_groups():
    data = request.args
    res = agg.get_groups(data['table'], data['group_col'])
    results = []
    for x in res:
        results.append(x[0])
    return jsonify({data['group_col']: results})

if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    app.run(debug=True, port=int(sys.argv[1]))
