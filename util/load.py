from __future__ import print_function
from initdb import tables, session, engine, conf, Base, Base_geo, Base_lib
from geoalchemy2 import Geography
from sqlalchemy import MetaData, Column, Table
from sqlalchemy.schema import ForeignKey
meta_internal = MetaData(schema='tempus_internal')

def geospatial_name(table):
    ''' Returns corresponding geospatial table name '''
    return '_{}_geo'.format(table)

def make_geospatial_table(table, pk, pk_type):
    geo = Table(geospatial_name(table), meta_internal,
        Column(pk, pk_type, ForeignKey('tempus_geography.{}.{}'.format(
            table, pk)), primary_key=True),
        Column('coords', Geography(geometry_type='POINT', srid=4326)),
        schema='tempus_geography'
    )
    return geo

def _init_once(args):
    geo_conf = conf['tempus_geography']
    # Create geospatial tables that link back to geographies
    for table in geo_conf:
        link_col = geo_conf[table]['linkage']
        geo_table = make_geospatial_table(table, link_col,
                                      getattr(Base_geo.classes[table], link_col).\
                                              property.columns[0].type)
        print(geo_table.create(engine))
    return

