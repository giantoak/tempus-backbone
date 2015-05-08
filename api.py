from flask import Flask, make_response, request
from initdb import tables, session, table_cols, table_conf
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


@app.route('/api/outliers', methods=['POST'])
def api_outliers():
    data = request.form
    logger.debug(data)

    return data

if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    app.run(debug=True, port=5000)
