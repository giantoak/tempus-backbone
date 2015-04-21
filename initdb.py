# coding: utf-8

import json
import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine import reflection
from collections import defaultdict

# Initialize application by reading in configuration files
with open('application.conf') as f:
    conf = json.load(f)


# Load configured tables into memory
Base = automap_base()
tables = {}
table_cols = {}

engine = sqlalchemy.create_engine(conf['db'])
insp = reflection.Inspector.from_engine(engine)
Base.prepare(engine, reflect=True)
for table in conf['tables']:
    tables[table] = Base.classes[table]

    table_cols[table] = conf['tables'][table]
    table_cols[table]['PK'] = \
        insp.get_pk_constraint(table)['constrained_columns']
    table_cols[table]['indexes'] = insp.get_index(table)

session = Session(engine)
