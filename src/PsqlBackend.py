# -*- coding: utf-8 -*-

import json
import psycopg2
import datetime

from .util import validate as valid
from .util import util as util


############################# BEGIN Class InsertQuery ##############################
class InsertQuery:
    def __init__(self, tableName):

        self.tableName = tableName

        self.values = []
        self.placeholders = []
        self.columnNames = []

      
    def add(self, columnName, value, placeholder):

        self.columnNames.append(columnName)
        self.placeholders.append(placeholder)
        self.values.append(value)

    def getString(self):
        return "insert into %s(%s) values(%s)" % (self.tableName, ','.join(self.columnNames), ','.join(self.placeholders))
############################# END Class InsertQuery ##############################



class PsqlBackend:

    ######################## BEGIN init method ######################
    def __init__(self, config):

        self.config = config

        connString = "host='%s' dbname='%s' user='%s' password='%s'" % (
            self.config['db_host'], self.config['db_name'], self.config['db_user'], self.config['db_password'])

        self.dbconn = psycopg2.connect(connString)

        # Create a cursor that returns rows as dictionaries with column name keys:
        #self.cursor = self.dbconn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.cursor = self.dbconn.cursor()        

    ######################## END init method ######################





    ############################# BEGIN Create Entity by object ################################     
    
    # ATTENTION: This method expects a Python object, *not* an NGSI-LD string!
         
    def write_entity(self, entity):   

        # Convert entity to temporal form:
        entity = util.entity_to_temporal(entity)     


        ################# BEGIN Validate entity #####################
        error = valid.validate_entity(entity, temporal = True)

        if error != None:
            return None, error
        ################# END Validate entity #####################
        

        for key, value in entity.items():

            if key == 'id' or key == 'type' or key == '@context':
                continue

            ############# BEGIN Add 'createdAt' and 'modifiedAt' if they are still missing (shouldn't be the case though!) ##########
            for instance in value:
                print(instance)
                
                if not 'createdAt' in instance:
                    instance['createdAt'] =  datetime.datetime.utcnow().isoformat() + "Z"

                if not 'modifiedAt' in instance:
                    instance['modifiedAt'] =  datetime.datetime.utcnow().isoformat() + "Z"
            ############# END Add 'createdAt' and 'modifiedAt' if they are still missing (shouldn't be the case though!) ##########

            # TODO: 1 Modifying a dictionary inside a loop which iterates over that dictionary is ugly!

            # Make sure that the attribute instance with the latest 'modifiedAt' value is the first entry of the list:
            entity[key] = sorted(entity[key], key=lambda i: (i['modifiedAt'], i['createdAt']), reverse=True) 



        ################ BEGIN Write main table entry ##############
        insertQuery = InsertQuery(self.config['db_table_data'])

        insertQuery.add("id", entity['id'], "%s")
        insertQuery.add("type", entity['type'], "%s")
        insertQuery.add("json_data", json.dumps(entity), "%s")
        
        # Write main table entry:
        try:
            self.query(insertQuery.getString(), insertQuery.values)
        except psycopg2.IntegrityError:

            return None, util.NgsiLdError("AlreadyExists", f"An entity with id {entity['id']} already exists.")
            
        
        except Exception as e:
            return None, util.NgsiLdError("InternalError","An unspecified database error occured while trying to create the entity.")
            
            
        finally:
            self.dbconn.commit()
        ################ END Write main table entry ##############
        

        ################## BEGIN Process GeoProperties ################
        # Delete all old geometry table entries for this entity's id:
        self.query("DELETE FROM %s WHERE eid = '%s'" % (self.config['db_table_geom'], entity['id']))

        ################### BEGIN Write new geo properties ###############
        for key in entity:
            
            
            if not isinstance(entity[key], list):
                continue

            if len(entity[key]) == 0:
                continue

            # Always use data from first instance:
            # TODO: 1 this is not correct
            prop = entity[key][0]

                    
            ############ BEGIN Check if property is a GeoProperty ###########
            if not type(prop) is dict:
                continue

            if not 'type' in prop:
                continue

            if prop['type'] != 'GeoProperty':
                continue
            ############ END Check if property is a GeoProperty ###########

            geojson = prop['value']

            insertQuery_geom = InsertQuery(self.config['db_table_geom'])

            # Add values to geometry insert query:
            insertQuery_geom.add("eid", entity['id'], "%s")
            insertQuery_geom.add("property", key, "%s")
            insertQuery_geom.add("geom", json.dumps(geojson), "ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)")
        
            # TODO: 2 Catch error that might happen if ID already exists
            self.query(insertQuery_geom.getString(), insertQuery_geom.values)
        ################### END Write new geo properties ###############

        ################## END Process GeoProperties ################
        
        return util.NgsiLdResult(None, 201), None
    ######################### END Create Entity by object #############################        



    def deleteAllEntities(self):
        self.query("DELETE FROM %s"  % (self.config['db_table_data']))
        self.query("DELETE FROM %s"  % (self.config['db_table_geom']))
        
        return util.NgsiLdResult(None, 204), None



    def delete_entity_by_id(self, id):
        ############# BEGIN Check if entity with the passed ID exists ###########
        result, error = self.get_entity_by_id(id)

        if error != None:
            # TODO: 3 Correct status code for this?
            return None, error
        ############# END Check if entity with the passed ID exists ###########

        # Delete geometry table rows:
        self.query("DELETE FROM %s WHERE eid = '%s'" % (self.config['db_table_geom'], id))

        # Delete main table row:        
        self.query("DELETE FROM %s WHERE id = '%s'" % (self.config['db_table_data'], id))

        return util.NgsiLdResult(None, 204), None


    #################### BEGIN 5.7.1 - Retrieve Entity ######################
    def get_entity_by_id(self, id, args = []):

        # TODO: 2 Implement "attrs" parameter (see NGSI-LD spec 6.5.3.1-1)    
        
        rows = self.query("SELECT json_data FROM %s WHERE id = '%s'" % (self.config['db_table_data'], id))

        if len(rows) == 0:
            return None, util.NgsiLdError("ResourceNotFound", "No entity with id '" + id + "' found.")
        
        elif len(rows) > 1:
            return None, util.NgsiLdError("InternalError", "Multiple entities with id '" + id + "' found. This is a database inconsistency and should never happen.")
               
        return util.NgsiLdResult(rows[0][0], 200), None
       
    #################### END 5.7.1 - Retrieve Entity ######################


    ####################### BEGIN PSQL query method #######################
    def query(self, query, values=None):

        result = None

        if values != None:
            self.cursor.execute(query, values)
        else:
            self.cursor.execute(query)

        try:
            result = self.cursor.fetchall()            
        except:
            pass

        self.dbconn.commit()

        return result
    ####################### END PSQL query method #######################




    #################### BEGIN 5.7.2 - Query Entities ####################
    def query_entities(self, args):        

        t1 = self.config['db_table_data']
        t2 = self.config['db_table_geom']


        arg_propQuery = args.get("q")
        arg_type = args.get('type')
        arg_attrs = args.get('attrs')
      

        sql_query = None
        sql_where_parts = []

        sql_select_fields = []


        sql_select_fields.append((t1, "json_data"))

        ###################### BEGIN Filter by type(s) ###############
        # NOTE: According to the NGSI-LD specification, an error message 
        # should be returned if neither 'type' nor 'attrs' parameter is passed.
        # We do not yet implement this behavior for development purposes.

        if arg_type != None:            
            types = arg_type.split(',')            
            sql_types = []
            
            for arg_type in types:
                sql_types.append("%s.type = '%s'" % (t1, arg_type))

            sql_where_parts.append("(" + " OR ".join(sql_types) + ")")
            #sql_where_parts.append("%s.type = '%s'" % (t1, type))
        ###################### END Filter by type(s) ###############

        # TODO: 3 Move temporal and spatial query processing to separate functions.
   

        ################################## BEGIN Process temporal query, if there is one ############################
        isTemporalQuery, error = valid.validateTemporalQuery(args)

        if isTemporalQuery:

            if error != None:
                return None, error

            # NOTE: Since we have already validated the temporal query parameters, there is no need to do it again here.

            timeRel = args['timerel']
            time = args['time']
            
            ########## BEGIN Set time property #########
            timeProperty = 'observedAt'

            if 'timeproperty' in args:
                timeProperty = args['timeproperty']
            ########## END Set time property #########

            # NOTE: Time comparison as strings works without conversion to a date object, as long as the time zone is the same. 
            # This should always be the case, since NGSI-LD expects all times to be expressed in UTC (see NGSI-LD spec 4.6.3)
        
            if timeRel == 'before':
               sql_where_parts.append(f"{t1}.json_data->'{timeProperty}'->0->>'value' < '{time}'")               
            
            elif timeRel == 'after':
                sql_where_parts.append(f"{t1}.json_data->'{timeProperty}'->0->>'value' > '{time}'")               
                
            elif timeRel == 'between':

                endtime = args['endtime']

                sql_where_parts.append(f"{t1}.json_data->'{timeProperty}'->0->>'value' > '{time}'")               
                sql_where_parts.append(f"{t1}.json_data->'{timeProperty}'->0->>'value' < '{endtime}'")               

        ################################# END Process temporal query, if there is one ##############################

        
        ################################ BEGIN Process geo query arguments #############################

        isGeoQuery, error_geo = valid.validateGeoQuery(args)

        if isGeoQuery:
          
            if error_geo != None:
                return None, error_geo

            # see NGSI-LD spec section 4.10
         
            geo_property = 'location'

            if 'geoproperty' in args:
                geo_property = args.get('geoproperty')

            geo_rel = args.get('georel')
            geo_compareGeomType = args.get('geometry')
            geo_compareCoordinates = json.loads(args.get('coordinates'))
            

            sql_select_fields.append((t2, "property"))
            sql_select_fields.append((t2, "geom"))
        
            # JOIN with geometry table by entity id:            
            sql_where_parts.append("%s.eid = %s.id" % (t2, t1))

            # JOIN with geometry table by the property column:
            sql_where_parts.append("%s.property = '%s'" % (t2, geo_property))

        
            geoRelParts = geo_rel.split(';')

            geoOp = geoRelParts[0]            

            compareGeoJsonString = json.dumps({"type": geo_compareGeomType, "coordinates": geo_compareCoordinates})

            
            if geoOp == 'near':
                         
                distanceParts = geoRelParts[1].split('==')

              
                distValue = float(distanceParts[1])
               
                whereString = "ST_DWithin(%s.geom::geography, ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)::geography, %s, true);" % (t2, compareGeoJsonString, distValue)

                if distanceParts[0] == 'maxDistance':                
                    sql_where_parts.append(whereString)
                elif distanceParts[0] == 'minDistance':                    
                    sql_where_parts.append("NOT " + whereString)
                    
            
            elif geoOp == 'within':
                sql_where_parts.append("ST_Within(%s.geom, ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326))" % (t2, compareGeoJsonString))

            elif geoOp == 'contains':
                sql_where_parts.append("ST_Contains(%s.geom, ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326))" % (t2, compareGeoJsonString))                

            elif geoOp == 'intersects':                            
                sql_where_parts.append("ST_Intersects(%s.geom, ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326))" % (t2, compareGeoJsonString))

            elif geoOp == 'equals':
                sql_where_parts.append("ST_Equals(%s.geom, ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326))" % (t2, compareGeoJsonString))

            elif geoOp == 'disjoint':
                sql_where_parts.append("ST_Disjoint(%s.geom, ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326))" % (t2, compareGeoJsonString))

            elif geoOp == 'overlaps':
                sql_where_parts.append("ST_Overlaps(%s.geom, ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326))" % (t2, compareGeoJsonString))
                
        ############################# END Process geo query ##############################
        
        fields = []
        tables = []
    
        # Always add the field json_data first, so that we can be sure that it is at index 0 when we
        # process the result:
        fields.append(f"{t1}.json_data")

        ############################ BEGIN Build SQL query ###########################
        for field in sql_select_fields:
            
            if not field[0] in tables:
                tables.append(field[0])

            fullFieldName = field[0] + "." + field[1]
            
            if not fullFieldName in fields:
                fields.append(fullFieldName)
            
        sql_query = "SELECT " + (",").join(fields) + " FROM " + (",").join(tables)


        # Add WHERE query part:
        if len(sql_where_parts) > 0:
            sql_query += " WHERE " + " AND ".join(sql_where_parts)


        #print("SQL QUERY:", sql_query)

        ############################ END Build SQL query ########################
        

        # Perform PSQL query:
        rows = self.query(sql_query)

        ############## BEGIN Prepare results list #############
        result = []

        for row in rows:
            #print(row)
            # Extract json_data field:
            result.append(row[0])            
        ############## END Prepare results list #############


        return util.NgsiLdResult(result, 200), None
    #################### END 5.7.2 - Query Entities ####################

