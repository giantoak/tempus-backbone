# coding: utf-8
import yaml
import json
import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy import func, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine import reflection
from collections import defaultdict
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_time_range(table, timestamp):
    ts = getattr(tables[table], timestamp)
    q = session.query(func.min(ts), func.max(ts))
    resp = q.first()

    logger.debug(resp)
    time_min, time_max = resp
    return map(str, (time_min, time_max))

# Initialize application by reading in configuration files
with open('application.yml.conf') as f:
    conf = yaml.safe_load(f)


# Load configured tables into memory
Base = automap_base()
tables = {}
table_cols = {}
table_conf = defaultdict(dict)

# Generate default base
engine = sqlalchemy.create_engine(conf['db'])
insp = reflection.Inspector.from_engine(engine)
Base.prepare(engine, reflect=True)
session = Session(engine)

# Generate geography, library bases
metadata_geography = MetaData(schema='tempus_geography')
metadata_geography.reflect(engine, schema='tempus_geography',
                           only=conf['tempus_geography'].keys())

metadata_library = MetaData(schema='tempus_library')
metadata_library.reflect(engine, schema='tempus_library')

Base_geo = automap_base(metadata=metadata_geography)
Base_geo.prepare(engine, reflect=True)

Base_lib = automap_base(metadata=metadata_library)
Base_lib.prepare(engine, reflect=True)

for table in conf['tables']:
    tables[table] = Base.classes[table]

    table_cols[table] = conf['tables'][table]
    min_time, max_time = get_time_range(table, table_cols[table]['timestamp'])
    table_cols[table]['_min_time'] = min_time
    table_cols[table]['_max_time'] = max_time

    table_conf[table]['PK'] = \
        insp.get_pk_constraint(table)['constrained_columns']
    table_conf[table]['indexes'] = insp.get_indexes(table)
