# -*- coding: utf-8 -*-

# TODO: Understand:
# - when vs. when not prefix 'type' and 'id' with '@' 
# - How to declare properties as TemporalProperty so that the query parser doesn't look for 'value'
# - How are properties queried across relationships, i.e. through the "normal" query API?

# - When "@type" / "@value" and when "type" / "value" ?
# NOTE: For this, see NGSI-LD spec section 4.5.2

# - Is a type/value dictionary mandatory for properties?

# TODO: 1 Prevent infinite recursive following of circular relationships in queries
# TODO: 2 Implement path parsing for "@value"

# TODO: 2 Automatically add 'createdAt' and 'modifiedAt' to entities

# TODO: 2 Find out exact definition of "ProblemDetails" type

# TODO: 2 Implement property validation, e.g. 4.6.2


import json
import psycopg2
#import psycopg2.extras # Required to use a cursur that returns rows as dictionaries with colum name keys
import re

from .ngsildutil import *


from .QueryParser import QueryParser


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



class PsqlClient:

    ######################## BEGIN init method ######################
    def __init__(self, config):

        self.config = config

        connString = "host='%s' dbname='%s' user='%s' password='%s'" % (
            self.config['db_host'], self.config['db_name'], self.config['db_user'], self.config['db_password'])

        self.dbconn = psycopg2.connect(connString)

        # Create a cursor that returns rows as dictionaries with column name keys:
        #self.cursor = self.dbconn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.cursor = self.dbconn.cursor()

        self.parser = QueryParser(self)

    ######################## END init method ######################





    ################################### BEGIN Official API methods ###################################


    ############################# BEGIN 5.6.1 - Create Entity ################################          
    def api_createEntity(self, json_ld):
        error = validateJsonLd(json_ld)

        if error != None:
            return None, error

        entity = json.loads(json_ld)
     
        return self.createEntity_object(entity)
    ############################# END 5.6.1 - Create Entity ################################        


    ############################# BEGIN 5.6.2 - Update Entity Attributes ################################          
    def api_updateEntityAttributes(self, entity_id, entity_fragment_json_ld):        
        
        # TODO: 2 Validate entity ID

        ############### BEGIN Try to fetch entity from database ###############
        result, error = self.getEntityById(entity_id)
        
        if result == None:
            return None, NgsiLdError("ResourceNotFound", "An entity with the passed ID does not exist: " + entity_id)

        existing_entity = result.payload
        ############### BEGIN Try to fetch entity from database ###############


        ############# BEGIN Validate payload JSON ############
        error = validateJsonLd(entity_fragment_json_ld)

        if error != None:
            return None, error
        
        entity_fragment = json.loads(entity_fragment_json_ld)
        ############# END Validate payload JSON ############

    
        # See 5.2.19 for strucuture of UnchangedDetails type
        unchanged_details = []
        updated = []

        ################# BEGIN Iterate over fragment properties #################
        for key, new_value in entity_fragment.items():
            
            if key == 'id' or key == 'type':
                continue

            if key in existing_entity:
                
                if not 'datasetId' in new_value:
                    existing_entity[key] = new_value
                    updated.append(key)
                    continue
                
                if 'datasetId' in existing_entity[key] and existing_entity[key]['datasetId'] == new_value['datasetId']:
                    existing_entity[key] = new_value
                    updated.append(key)
                else:
                    unchanged_details.append({"attributeName" : key, "reason" : "datasetid does not match: Existing: " + existing_entity[key]['datasetId'] + ", Update Fragment: " + new_value['datasetId']})            
        ################# END Iterate over fragment properties #################

        # Write changes to database:
        result, error = self.upsertEntity_object(existing_entity)

        if error != None:
            return None, error


        if len(unchanged_details) > 0:
            # See 5.2.18 for structure of UpdateResult type     
            return NgsiLdResult( { "updated" : updated, "unchanged" : unchanged_details}, 207), None
        else:
            return NgsiLdResult(None, 204), None

    ############################# END 5.6.2 - Update Entity Attributes ################################        
    


    ################## BEGIN 5.6.3 - Append Entity Attributes ###################
    def api_appendEntityAttributes(self, entity_id, entity_fragment_jsonld, overwrite = True):

        # TODO: 2 Validate entity ID
        
        ############### BEGIN Try to fetch entity from database ###############
        result, error = self.getEntityById(entity_id)
        
        if result == None:
            return None, NgsiLdError("ResourceNotFound", "An entity with the passed ID does not exist: " + entity_id)

        existingEntity = result.payload
        ############### BEGIN Try to fetch entity from database ###############
      
        
        ############# BEGIN Validate payload JSON ############
        error = validateJsonLd(entity_fragment_jsonld)

        if error != None:
            return None, error
        
        entityFragment = json.loads(entity_fragment_jsonld)
        ############# END Validate payload JSON ############


        if not isinstance(entityFragment, dict):
            # TODO: 4 Correct error type?
            return None, NgsiLdError("BadRequestData", "Entity fragments are JSON-LD dictionaries. The passed payload is not a JSON-LD dictionary.")

        if len(entityFragment) == 0:
            # TODO: 4 Correct error type?
            return None, NgsiLdError("BadRequestData", "Entity fragments contains no properties.")


        # TODO: 2 Consider term expansion rules as mandated by clause 5.5.7

        # TODO: 3 When overwriting without datasetId, should the attribute index (in existing entity and in
        # update fragment) be taken into account? If yes, how?
        
        # NOTE: 
        # Currently, if no datasetId is provided, attribute keys are 
        # compared *with* their (possible) index. Is this correct?

        ################## BEGIN Iterate over entity fragment ################
        for keyWithIndex, newValue in entityFragment.items():

            pieces = keyWithIndex.split('#')

            key = pieces[0]
            
            # TODO: 3 How to handle property arrays in the entity fragment?

            # If the property does not exist in the existing entity:            
            if not key in existingEntity:
                existingEntity[key] = newValue            
                continue
            
            # Otherwise:                

            # See 4.5.2 for description of the datasetId attribute.
            # Also see NGSI-LD section C.2.2 for an example for datasetId
        
             
            # NOTE: 
            # For better code readability, we have inverted the conditional check here as compared
            # to the text of the NGSI-LD specification:
                
            if not 'datasetId' in newValue: 
                if overwrite:
                    existingEntity[key] = newValue
            else:                    
                instance_keys = []

                highestIndex = 0

                for existing_key, existing_value in existingEntity.items():
                    pieces = existing_key.split('#')
                    print(pieces)

                    if pieces[0] == key:
                        instance_keys.append(existing_key)

                    # Sanity checks:
                    if len(pieces) == 2:
                        try:
                            number = int(pieces[1])

                            if number > highestIndex:
                                highestIndex = number
                        except(e):                        
                            return None, NgsiLdError("InternalError", "Update aborted. The entity which is to be updated has an invalid structure: Index not numeric: " + existingKey)
                    
                    if len(pieces) > 2:
                        return None, NgsiLdError("InternalError", "Update aborted. The entity which is to be updated has an invalid structure: Invalid key name: " + existingKey)

                

                instancesFound = 0

                for instance_key in instance_keys:
                    
                    instance = existingEntity[instance_key]

                    if 'datasetId' in instance and instance['datasetId'] == newValue['datasetId']:
                        
                        instancesFound += 1                        

                        if overwrite:
                            existingEntity[instance_key] = newValue                                            

                # Sanity check:
                if instancesFound > 1:
                    return None, NgsiLdError("InternalError", "Update aborted. The entity which is to be updated has an invalid structure: More than one property instance have the same datasetId: " + key)


                if instancesFound == 0:
                    appendKey = key + "#" + str(highestIndex + 1)

                    existingEntity[appendKey] = newValue

              

           
        ################## END Iterate over entity fragment ################

        result, error = self.upsertEntity_object(existingEntity)

        if error != None:
            return None, error


        return NgsiLdResult(None,204), None

    ################## END 5.6.3 - Append Entity Attributes ###################





    ############################# BEGIN 5.6.6 - Delete Entity ###############################
    def api_deleteEntity(self, id):

        ############# BEGIN Check if entity with the passed ID exists ###########
        result, error = self.getEntityById(id)

        if error != None:
            # TODO: 3 Correct status code for this?
            return None, error
        ############# END Check if entity with the passed ID exists ###########

        # Delete geometry table rows:
        self.query("DELETE FROM %s WHERE eid = '%s'" % (self.config['db_table_geom'], id))

        # Delete main table row:        
        self.query("DELETE FROM %s WHERE id = '%s'" % (self.config['db_table_data'], id))

        return NgsiLdResult(None, 204), None
    ############################# END 5.6.6 - Delete Entity ###############################



    ########################## BEGIN 5.6.8 - Batch Entity Creation or Update (Upsert) #####################
    def api_entityOperationsUpsert(self, json_ld, options = "replace"):

        # Validate options argument:
        if not (options == 'update' or options == 'replace'):
            # TODO: 4 Correct error type?
            return None, NgsiLdError("InvalidRequest", "'options' must be one of: 'replace', 'update' (default: 'replace').")


        # Validate JSON-LD:
        error = validateJsonLd(json_ld)

        if error != None:
            return None, error

        entities = json.loads(json_ld)

        # Check if payload is a list:
        if not isinstance(entities, list):
            return None, NgsiLdError("BadRequestData", "Request payload must be a JSON-LD array containing NGSI-LD entities.")

        # Check if payload is is not empty:
        if len(entities) == 0:
            return None, NgsiLdError("BadRequestData", "The list of entities must not be empty.")

        ########### BEGIN Validate entities in request payload list ###########
        for entity in entities:            
            error = validateEntity_object(entity)
            if error != None:
                return None, error
        ########### END Validate entities in request payload list ###########
    
        success = []
        errors = []

        
        ################## BEGIN Loop over entities in payload ###############
        for newEntity in entities:

            id = newEntity['id']

            result, error = self.getEntityById(id)

            if result != None:

                existingEntity = result.payload

                if options == "replace":
                
                    # TODO: 3 Handle delete errors? -> BatchEntityError
                    
                    # TODO: 2 Use upsert_entity_object method here?
                    
                    # Delete old entity:
                    self.api_deleteEntity(id)
                    
                    # Write new entity:
                    self.createEntity_object(newEntity)
                
                elif options == "update":
                    # Append attributes as specified in 5.6.3:
                    self.appendEntityAttributes(existingEntity, newEntity)

            else:
                # Write new entity:
                self.createEntity_object(newEntity)
                                
            success.append(id)
        ################## END Loop over entities in payload ###############

        # TODO: 3 Add BatchEntityError instance to errors array if something goes wrong

        # See section 5.2.16 for the structure of BatchOperationResult
        batchOperationResult = { "succcess" : success, "errors" : errors}

        return NgsiLdResult(batchOperationResult, 200), None
    
    ########################## END 5.6.8 - Batch Entity Creation or Update (Upsert) #####################
    

    # TODO: 3 Implement 5.6.9 - Batch Entity Update


    ############## BEGIN 5.6.10 - Batch Entity Delete #############
    def api_entityOperationsDelete(self, json_ld):

        error = validateJsonLd(json_ld)

        if error != None:
            return None, error

        listOfIds = json.loads(json_ld)

        # Check whether passed JSON is a list:
        if not isinstance(listOfIds, list):
            return None, NgsiLdError("InvalidRequest", "Payload JSON is not a list.") 

        ############ BEGIN Check all passed entities for errors before starting to write any of them #########
        for id in listOfIds:
            self.api_deleteEntity(id)
        ############ END Check all passed entities for errors before starting to write any of them #########

        return NgsiLdResult(None, 200), None
    
    ############## END 5.6.10 - Batch Entity Delete #############

    


    ############## BEGIN 5.6.11 - Create of Update Temporal Representation of an Entity #############

    def api_upsertTemporalEntities(self, json_ld):

        error = validateJsonLd(json_ld)

        if error != None:
            return None, error

   
        entity = json.loads(json_ld)

        # TODO: 1 Check if this is an EntityTemporal (or Entity?)

        existing_entity, statusCode, error = self.getEntityById(entity['id'])

        if existing_entity == None:
            return self.createEntity_object(entity)

        updatedEntity = addTemporalAttributeInstances(existing_entity, entity)

        self.upsertEntity_object(updatedEntity)
        # 201 - Created
        # 204 - Updated
        return NgsiLdResult(None, 201), None
        #existingEntity = self.getEntityById

        # TODO: 2 Implement

    ############## END 5.6.11 - Create of Update Temporal Representation of an Entity #############



    #################### BEGIN 5.7.1 - Retrieve Entity ######################
    def api_getEntityById(self, id, args = []):
        return self.getEntityById(id, args)
    #################### END 5.7.1 - Retrieve Entity ######################


    

    #################### BEGIN 5.7.2 - Query Entities ####################
    def api_queryEntities(self, args):        

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
        isTemporalQuery, error = validateTemporalQuery(args)

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
               sql_where_parts.append(f"{t1}.json_data->'{timeProperty}'->>'value' < '{time}'")               
            
            elif timeRel == 'after':
                sql_where_parts.append(f"{t1}.json_data->'{timeProperty}'->>'value' > '{time}'")               
                
            elif timeRel == 'between':

                endtime = args['endtime']

                sql_where_parts.append(f"{t1}.json_data->'{timeProperty}'->>'value' > '{time}'")               
                sql_where_parts.append(f"{t1}.json_data->'{timeProperty}'->>'value' < '{endtime}'")               
                
        ################################# END Process temporal query, if there is one ##############################

        
        ################################ BEGIN Process geo query arguments #############################

        isGeoQuery, error_geo = validateGeoQuery(args)

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
        
        
        #sql_query = "SELECT %s.json_data, %s.json_data->>'id' as blubb FROM %s" % (t1, t1, t1)
        #sql_query = "SELECT %s.json_data FROM %s" % (t1, t1, t1)
        #sql_query = f"SELECT {t1}.json_data FROM {t1}"

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
        
        #sql_query = "SELECT %s.json_data, %s.property as geoproperty, %s.geom as geom FROM %s, %s" % (t1, t2, t2, t1, t2)
        #sql_query = "SELECT * FROM %s, %s" % (t1, t1, t2)
        
        
        

      

        # Perform PSQL query:
        rows = self.query(sql_query)

        ############## BEGIN Prepare results list #############
        result = []

        for row in rows:
            #print(row)
            # Extract json_data field:
            result.append(row[0])            
        ############## END Prepare results list #############


        ##################### BEGIN Process properties query #####################
        if arg_propQuery != None:

            #print("Query: " + propQuery)

            tokens = self.parser.tokenize(arg_propQuery)
            pp, index = self.parser.parseParantheses(tokens, 0)
            ast = self.parser.buildAST(pp)

            result_filtered = []

            ########### BEGIN Filter entitiy ############
            for entity in result:

                passed = self.parser.evaluate(ast, entity)

                if not passed:                     
                    continue

                result_filtered.append(entity)
            ########### END Filter entitiy ############

            result = result_filtered
        ##################### END Process properties query #####################
        

        ####################### BEGIN Remove unrequested attributes ######################
        if arg_attrs != None:

            result_filtered = []

            attrs_list = arg_attrs.split(',')

            for entity in result:
                entity_cleaned = {}
            
                for key in entity:
                    if key in attrs_list:
                        entity_cleaned[key] = entity[key]

                result_filtered.append(entity_cleaned)

            result = result_filtered
        ####################### END Remove unrequested attributes ######################

        return NgsiLdResult(result, 200), None
    #################### END 5.7.2 - Query Entities ####################


    ############## BEGIN 5.7.3 - Retrieve temporal evolution of an Entity #############
    def api_getTemporalEntityById(self, entityId, args):

        # 5.7.3.4:

        # TODO: 2 check entityID -> if not present or valid, return BadRequestData error

        # Try to retrieve requested entity by id:
        #entity, responseCode, error = self.api_getEntityById(entityId)
        result, error = self.getEntityById(entityId)

        if error != None:
            return None, error

        existingEntity = result.payload

        isTemporalQuery = validateTemporalQuery(args)

        if isTemporalQuery:
            pass
        
        return NgsiLdResult(createEntityTemporal(existingEntity, args), 200), None


        pass
    ############## END 5.7.3 - Retrieve temporal evolution of an Entity #############


    ############## BEGIN 5.7.4 - Query temporal evolution of entities #############

    def api_getTemporalEntities(self, args):

        isTemporalQuery, error = validateTemporalQuery(args)

        # NGSI-LD spec section 5.7.4.4:

        if not isTemporalQuery or error != None:
            return None, error

        

        # 201 - Created
        # 204 - Updated
        return NgsiLdResult(None, 201), None
        #existingEntity = self.getEntityById

        # TODO: 2 Implement

    ############## END 5.7.4 - Query temporal evolution of entities #############

    ################################### END Official API methods ###################################
    











    ################### BEGIN Inofficial API methods (not part of NGSI-LD specification!) ###################


    ######### BEGIN Delete all entities (inofficial, only for testing!) ########
    def api_inofficial_deleteEntities(self):
                
        self.query("DELETE FROM %s"  % (self.config['db_table_data']))
        self.query("DELETE FROM %s"  % (self.config['db_table_geom']))
        
        return NgsiLdResult(None, 204), None
    ######### END Delete all entities (inoffocial, only for testing!) ##########


    ######### BEGIN Upsert entity (inofficial!) ############    
    def api_inofficial_upsertEntity(self, json_ld):      

        error = validateJsonLd(json_ld)

        if error != None:
            return None, error
  
     
        entity = json.loads(json_ld)

        error = validateEntity_object(entity)

        if error != None:
            return None, error

      
        # TODO: 4 Implement real upsert instead of delete+create
        
        existingEntity = self.getEntityById(entity['id'])

        if existingEntity != None:            
            result, error = self.api_deleteEntity(entity['id'])
        
        
        # TODO: 3 Return different status code for update/creation here?

        return self.createEntity_object(entity)

    ######### END Upsert entity (inofficial!) ###############

    ################### END Inofficial API methods (not part of NGSI-LD specification!) ###################








    ############################# BEGIN Create Entity by object ################################     
    
    # ATTENTION: This method expects a Python object, *not* an NGSI-LD string!
         
    def createEntity_object(self, entity):        
    
        error = validateEntity_object(entity)

        if error != None:
            return None, error


        # TODO: 3 Add system-generated property 'createdAt' (see NGSI-LD spec 4.5.2)
        # TODO: 3 Add system-generated property 'modifiedAt' (see NGSI-LD spec 4.5.2)

        # TODO: 3 Check datetime conformity as defined in NGSI-LD spec 4.6.3


        ################ BEGIN Write main table entry ##############
        insertQuery = InsertQuery(self.config['db_table_data'])

        insertQuery.add("id", entity['id'], "%s")
        insertQuery.add("type", entity['type'], "%s")
        insertQuery.add("json_data", json.dumps(entity), "%s")
        
        # Write main table entry:
        try:
            self.query(insertQuery.getString(), insertQuery.values)
        except psycopg2.IntegrityError:

            return None, NgsiLdError("AlreadyExists", f"An entity with id {entity['id']} already exists.")
            
        
        except Exception as e:
            return None, NgsiLdError("InternalError","An unspecified database error occured while trying to create the entity.")
            
            
        finally:
            self.dbconn.commit()
        ################ END Write main table entry ##############
        

        ################## BEGIN Process GeoProperties ################
        # Delete all old geometry table entries for this entity's id:
        self.query("DELETE FROM %s WHERE eid = '%s'" % (self.config['db_table_geom'], entity['id']))

        ################### BEGIN Write new geo properties ###############
        for key in entity:
            
            prop = entity[key]

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
        
        return NgsiLdResult(None, 201), None
    ######################### END Create Entity by object #############################        


    #################### BEGIN 5.7.1 - Retrieve Entity ######################
    def getEntityById(self, id, args = []):

        # TODO: 2 Implement "attrs" parameter (see NGSI-LD spec 6.5.3.1-1)    
        
        rows = self.query("SELECT json_data FROM %s WHERE id = '%s'" % (self.config['db_table_data'], id))

        if len(rows) == 0:
            return None, NgsiLdError("ResourceNotFound", "No entity with id '" + id + "' found.")
        
        elif len(rows) > 1:
            return None, NgsiLdError("InternalError", "Multiple entities with id '" + id + "' found. This is a database inconsistency and should never happen.")
               
        return NgsiLdResult(rows[0][0], 200), None
       
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





    ######################### BEGIN Upsert entity #############################    
    def upsertEntity_object(self, entity):        
     
        error = validateEntity_object(entity)

        if error != None:
            return None, error

      
        # TODO: 4 Implement real upsert instead of delete+create
        self.api_deleteEntity(entity['id'])
        
        #response, statusCode, error = self.createEntity_object(entity)
        result, error = self.createEntity_object(entity)

        if error != None:
            return None, error

        # TODO: Return proper status code: 201 for create, 204 for update
        return NgsiLdResult(None, 204), None
    ######################### END Upsert entity #############################

