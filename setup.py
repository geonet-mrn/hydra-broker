# -*- coding: utf-8 -*-

import psycopg2, json

configFilePath = "hydraconfig.json"

try:
    with open(configFilePath) as json_file:
        config = json.load(json_file)
except:
    print("Failed to read config file: " + str(configFilePath))
    exit(-1)

connString = "host='%s' dbname='%s' user='%s' password='%s'" % (config['db_host'], config['db_name'], config['db_user'], config['db_password'])


dbconn = psycopg2.connect(connString)

cursor = dbconn.cursor()

# Create extension POSTGIS:
query = "CREATE EXTENSION postgis;"
cursor.execute(query)

# Create data table:
query = "CREATE TABLE " + config['db_table_data'] + "(id character varying NOT NULL, type character varying, json_data jsonb, CONSTRAINT " + config['db_table_data'] + "_pkey PRIMARY KEY (id));"
cursor.execute(query)

# Create geometry table:
query = "CREATE TABLE " + config['db_table_geom'] + "(eid character varying NOT NULL, property character varying NOT NULL, geom geometry, CONSTRAINT " + config['db_table_geom'] + "_pkey PRIMARY KEY (eid, property));"
cursor.execute(query)

dbconn.commit()

dbconn.close()