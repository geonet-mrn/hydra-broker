# -*- coding: utf-8 -*-

# TODO: Understand:

# TODO: 2 Implement path parsing for "@value"

# TODO: 2 Automatically add 'createdAt' and 'modifiedAt' to entities

# TODO: 2 Find out exact definition of "ProblemDetails" type

# TODO: 2 Implement property validation, e.g. 4.6.2

# TODO: 2 What happens when an update to a temporal entity is done via the normal API?


import json

from .PsqlBackend import PsqlBackend
from .QueryParser import QueryParser
from .util import validate as valid
from .util import util as util

import datetime
import uuid


class PsqlClient:

    ######################## BEGIN init method ######################
    def __init__(self, config):

        self.config = config
        self.backend = PsqlBackend(config)

        self.parser = QueryParser(self)
    ######################## END init method ######################



    ############################# BEGIN 5.6.1 - Create Entity ################################          
    def api_createEntity(self, json_ld):
        error = valid.validateJsonLd(json_ld)

        if error != None:
            return None, error

        entity = json.loads(json_ld)        
     
        return self.backend.write_entity(entity)
    ############################# END 5.6.1 - Create Entity ################################        


    ############################# BEGIN 5.6.2 - Update Entity Attributes ################################          
    def api_updateEntityAttributes(self, entity_id, entity_fragment_json_ld):        
        
        # TODO: 2 Validate entity ID

        ############### BEGIN Try to fetch entity from database ###############
        result, error = self.backend.get_entity_by_id(entity_id)
        
        if result == None:
            return None, util.NgsiLdError("ResourceNotFound", "An entity with the passed ID does not exist: " + entity_id)

        existing_entity = util.entity_to_single(result.payload)
        ############### BEGIN Try to fetch entity from database ###############


        ############# BEGIN Validate payload JSON ############
        error = valid.validateJsonLd(entity_fragment_json_ld)

        if error != None:
            return None, error
        
        entity_fragment = json.loads(entity_fragment_json_ld)
        ############# END Validate payload JSON ############

    
        # See 5.2.19 for strucuture of UnchangedDetails type
        unchanged_details = []
        updated = []

        ################# BEGIN Iterate over fragment properties #################
        for key, new_value in entity_fragment.items():
            
            if key == 'id' or key == 'type' or key == '@context':
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


        ################ BEGIN Write changes to database ############

        # Delete old entity instance: 
        self.backend.delete_entity_by_id(existing_entity['id'])
        
        # Write new entity instance:
        result, error = self.backend.write_entity(existing_entity)

        if error != None:
            return None, error
        ################ END Write changes to database ############


        if len(unchanged_details) > 0:
            # See 5.2.18 for structure of UpdateResult type     
            return util.NgsiLdResult( { "updated" : updated, "unchanged" : unchanged_details}, 207), None
        else:
            return util.NgsiLdResult(None, 204), None

    ############################# END 5.6.2 - Update Entity Attributes ################################        
    


    ################## BEGIN 5.6.3 - Append Entity Attributes ###################
    def api_appendEntityAttributes(self, entity_id, entity_fragment_jsonld, overwrite = True):

        # TODO: 2 Validate entity ID
        
        ############### BEGIN Try to fetch entity from database ###############
        result, error = self.backend.get_entity_by_id(entity_id)
        
        if result == None:
            return None, util.NgsiLdError("ResourceNotFound", "An entity with the passed ID does not exist: " + entity_id)

        existingEntity = util.entity_to_single(result.payload)
        ############### BEGIN Try to fetch entity from database ###############
      
        
        ############# BEGIN Validate payload JSON ############
        error = valid.validateJsonLd(entity_fragment_jsonld)

        if error != None:
            return None, error
        
        entityFragment = json.loads(entity_fragment_jsonld)
        ############# END Validate payload JSON ############


        if not isinstance(entityFragment, dict):
            # TODO: 4 Correct error type?
            return None, util.NgsiLdError("BadRequestData", "Entity fragments are JSON-LD dictionaries. The passed payload is not a JSON-LD dictionary.")

        if len(entityFragment) == 0:
            # TODO: 4 Correct error type?
            return None, util.NgsiLdError("BadRequestData", "Entity fragments contains no properties.")


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

            if key == 'id' or key == 'type' or key == '@context':
                continue

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
                            return None, util.NgsiLdError("InternalError", "Update aborted. The entity which is to be updated has an invalid structure: Index not numeric: " + existingKey)
                    
                    if len(pieces) > 2:
                        return None, util.NgsiLdError("InternalError", "Update aborted. The entity which is to be updated has an invalid structure: Invalid key name: " + existingKey)
                

                instancesFound = 0

                for instance_key in instance_keys:
                    
                    instance = existingEntity[instance_key]

                    if 'datasetId' in instance and instance['datasetId'] == newValue['datasetId']:
                        
                        instancesFound += 1                        

                        if overwrite:
                            existingEntity[instance_key] = newValue                                            

                # Sanity check:
                if instancesFound > 1:
                    return None, util.NgsiLdError("InternalError", "Update aborted. The entity which is to be updated has an invalid structure: More than one property instance have the same datasetId: " + key)


                if instancesFound == 0:
                    appendKey = key + "#" + str(highestIndex + 1)

                    existingEntity[appendKey] = newValue 
        ################## END Iterate over entity fragment ################


        ################ BEGIN Write changes to database ############        
        # Delete old entity instance: 
        self.backend.delete_entity_by_id(existingEntity['id'])
        
        # Write new entity instance:
        result, error = self.backend.write_entity(existingEntity)

        if error != None:
            return None, error
        ################ END Write changes to database ############


        return util.NgsiLdResult(None,204), None

    ################## END 5.6.3 - Append Entity Attributes ###################





    ############################# BEGIN 5.6.6 - Delete Entity ###############################
    def api_deleteEntity(self, id):
        return self.backend.delete_entity_by_id(id)
    ############################# END 5.6.6 - Delete Entity ###############################

  

    # TODO: 2 Implement
    def api_patchEntityAttributeById(self, entityId, attrId):
        return None, util.NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")

    # TODO: 2 Implement
    def api_deleteEntityAttributeById(self, entityId, attrId, deleteAll, datasetId):
        return None, util.NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")




    ########################## BEGIN 5.6.8 - Batch Entity Creation or Update (Upsert) #####################
    def api_entityOperationsUpsert(self, json_ld, options = "replace"):

        # Validate options argument:
        if not (options == 'update' or options == 'replace'):
            # TODO: 4 Correct error type?
            return None, util.NgsiLdError("InvalidRequest", "'options' must be one of: 'replace', 'update' (default: 'replace').")


        # Validate JSON-LD:
        error = valid.validateJsonLd(json_ld)

        if error != None:
            return None, error

        entities = json.loads(json_ld)

        # Check if payload is a list:
        if not isinstance(entities, list):
            return None, util.NgsiLdError("BadRequestData", "Request payload must be a JSON-LD array containing NGSI-LD entities.")

        # Check if payload is is not empty:
        if len(entities) == 0:
            return None, util.NgsiLdError("BadRequestData", "The list of entities must not be empty.")

        ########### BEGIN Validate entities in request payload list ###########
        for entity in entities:            
            error = valid.validate_entity(entity, temporal = False)
            if error != None:
                return None, error
        ########### END Validate entities in request payload list ###########
    
        success = []
        errors = []

        
        ################## BEGIN Loop over entities in payload ###############
        for newEntity in entities:

            id = newEntity['id']

            result, error = self.backend.get_entity_by_id(id)

            if result != None:

                existingEntity = util.entity_to_single(result.payload)

                if options == "replace":
                
                    # TODO: 3 Handle delete errors? -> BatchEntityError
                    
                    # TODO: 2 Use upsert_entity_object method here?
                    
                    # Delete old entity:
                    self.backend.delete_entity_by_id(id)
                    
                    # Write new entity:                    
                    self.backend.write_entity(newEntity)
                
                elif options == "update":
                    # Append attributes as specified in 5.6.3:
                    
                    self.api_appendEntityAttributes(existingEntity, newEntity)

            else:
                # Write new entity:
                self.backend.write_entity(newEntity)
                                
            success.append(id)
        ################## END Loop over entities in payload ###############

        # TODO: 3 Add BatchEntityError instance to errors array if something goes wrong

        # See section 5.2.16 for the structure of BatchOperationResult
        batchOperationResult = { "succcess" : success, "errors" : errors}

        return util.NgsiLdResult(batchOperationResult, 200), None
    
    ########################## END 5.6.8 - Batch Entity Creation or Update (Upsert) #####################
    
    
    # NOTE: 5.6.9 - Batch entity update is implemented by 5.6.8 - Batch Entity Creation or Update. The logic is identical.


    ############################# BEGIN 5.6.10 - Batch Entity Delete ###########################
    def api_entityOperationsDelete(self, json_ld):

        error = valid.validateJsonLd(json_ld)

        if error != None:
            return None, error

        listOfIds = json.loads(json_ld)

        # Check whether passed JSON is a list:
        if not isinstance(listOfIds, list):
            return None, util.NgsiLdError("InvalidRequest", "Payload JSON is not a list.") 

        ############ BEGIN Check all passed entities for errors before starting to write any of them #########
        for id in listOfIds:
            self.backend.delete_entity_by_id(id)
        ############ END Check all passed entities for errors before starting to write any of them #########

        return util.NgsiLdResult(None, 200), None
    
    ############################### END 5.6.10 - Batch Entity Delete ##########################

    


    ############## BEGIN 5.6.11 - Create or Update Temporal Representation of an Entity #############

    def api_upsertTemporalEntities(self, json_ld):

        return None, util.NgsiLdError("OperationNotSupported", "This operation is not yet implemented")
    ############## END 5.6.11 - Create or Update Temporal Representation of an Entity #############



    ############## BEGIN 5.6.12 -  Add Attributes to Temporal Representation of an Entity #############
    def api_addTemporalEntityAttributes(self, entity_id, json_ld):

        # 5.6.12.4

        # -

        # TODO: 2 Check entity_id

        ############## BEGIN Try to fetch existing entity ###############
        result, error = self.backend.get_entity_by_id(entity_id)

        if result == None:            
            return None, util.NgsiLdError("ResourceNotFound", "An entity with the passed ID does not exist: " + entity_id)
        
        existing_entity = result.payload
        ############## END Try to fetch existing entity ###############

        # -

        ############# BEGIN Validate temporal entity fragment #############
        error = valid.validateJsonLd(json_ld)

        if error != None:
            return None, error
   
        entity_temporal_fragment = json.loads(json_ld)
        
        error = valid.validate_entity(entity_temporal_fragment, temporal = True)

        if error != None:
            return None, error
        ############# END Validate temporal entity fragment #############


        # -
       
        #################### BEGIN Iterate over the attributes of the passed EntityTemporal fragment ##################
        for key, value in entity_temporal_fragment.items():

            # Skip id, type and @context:
            if key == 'id' or key == 'type' or key == '@context':
                continue
            
            # Check if attribute is in temporal form. If not, abort with error:
            if not isinstance(entity_temporal_fragment[key], list):
                return None, util.NgsiLdError("BadRequestData", "Property is not in temporal form: " + key)

            # If attribute does not exist in original entity, create it:
            if not key in existing_entity:
                existing_entity[key] = []


            # Set system-generated values for the added attribute instances:
            for instance in value:                
                currentTime = datetime.datetime.utcnow().isoformat() + "Z"
                instance['createdAt'] = currentTime
                instance['modifiedAt'] = currentTime

                instance['instanceId'] = str(uuid.uuid4())

            # Append instances from fragment:
            existing_entity[key].extend(value)

        #################### END Iterate over the attributes of the passed EntityTemporal fragment ##################


        ############## BEGIN Write changes to databse #############
        # Delete old entity:
        self.backend.delete_entity_by_id(existing_entity['id'])
                
        # Write new entity:
        result, error = self.backend.write_entity(existing_entity)

        if error != None:
            return None, error
        ############## END Write changes to databse #############
        

        return util.NgsiLdResult(None,204), None
    ############## END 5.6.12 -  Add Attributes to Temporal Representation of an Entity #############


    ############## BEGIN 5.6.13 - Delete Attribute from Temporal Representation of an Entity #############
    def api_deleteTemporalEntityAttributeById(self, entityId, attrId):
        # TODO: 2 Implement
        return None, util.NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")

    ############## END 5.6.13 - Delete Attribute from Temporal Representation of an Entity #############

    
    ############## BEGIN 5.6.14 - Modify attribute instance from Temporal Representation of an Entity #############
    def api_modifyTemporalEntityAttributeInstance(self, entityId, attrId, instanceId):
        # TODO: 2 Implement
        return None, util.NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")
    ############## END 5.6.14 - Modify attribute instance from Temporal Representation of an Entity #############


    ############## BEGIN 5.6.15 - Delete attribute instance from Temporal Representation of an Entity #############
    def api_deleteTemporalEntityAttributeInstance(self, entityId, attrId, instanceId):
        # TODO: 2 Implement
        return None, util.NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")
    ############## END 5.6.15 - Delete attribute instance from Temporal Representation of an Entity #############


    ############### BEGIN 5.6.16 - Delete Temporal Representation of an Entity ################
    def api_deleteTemporalEntityById(self, entity_id):
        return self.backend.delete_entity_by_id(entity_id)
    ############### END 5.6.16 - Delete Temporal Representation of an Entity ################



    #################### BEGIN 5.7.1 - Retrieve Entity ######################
    def api_getEntityById(self, id, args = []):
        
        result, error = self.backend.get_entity_by_id(id, args)

        if error != None:
            return None, error

        entity = util.entity_to_single(result.payload)

        return util.NgsiLdResult(entity, result.statusCode), None
    #################### END 5.7.1 - Retrieve Entity ######################


    def post_geoquery(self, request):
      
        args = request.args.copy()
      
        ############ BEGIN Read geo query coordinates data from request payload #########
        
        # This is a workaround for the problem that the maximum length of the 
        # GET request parameters string might be too short for geo query geometries.
        blubb = None
        
        try:
            blubb = json.loads(request.data)            
        except:
            pass
   
        if blubb != None:
            print("PAYLOAD IS JSON!")
            args["coordinates"] = request.data
            
        
        return self.api_queryEntities(args)

        
        ##################### BEGIN Process properties query #####################
        
        

    ######################## BEGIN 5.7.2 - Query Entities ######################
    def api_queryEntities(self, args):

        arg_propQuery = args.get("q")
        arg_type = args.get('type')
        arg_attrs = args.get('attrs')


        result, error =  self.backend.query_entities(args)

        if error != None:
            return None, error

        result2 = []

        for entity in result.payload:
            result2.append(util.entity_to_single(entity))
            
        
        if arg_propQuery != None:

            #print("Query: " + propQuery)

            tokens = self.parser.tokenize(arg_propQuery)
            pp, index = self.parser.parseParantheses(tokens, 0)
            ast = self.parser.buildAST(pp)

            result_filtered = []

            ########### BEGIN Filter entitiy ############
            for entity in result2:

                passed = self.parser.evaluate(ast, entity)

                if not passed:                     
                    continue

                result_filtered.append(entity)
            ########### END Filter entitiy ############

            result2 = result_filtered
        ##################### END Process properties query #####################
        

        ####################### BEGIN Remove unrequested attributes ######################
        if arg_attrs != None:

            result_filtered = []

            attrs_list = arg_attrs.split(',')

            for entity in result2:
                entity_cleaned = {}
            
                for key in entity:
                    if key in attrs_list:
                        entity_cleaned[key] = entity[key]

                result_filtered.append(entity_cleaned)

            result2 = result_filtered
        ####################### END Remove unrequested attributes ######################


        return util.NgsiLdResult(result2, result.statusCode), None
    ######################## END 5.7.2 - Query Entities ######################



    ############## BEGIN 5.7.3 - Retrieve temporal evolution of an Entity #############
    def api_getTemporalEntityById(self, entityId, args):

        # TODO: 2 check entityID -> if not present or valid, return BadRequestData error

        # TODO: 2 Implement list of attributes to be retrieved
        # TODO: 2 Implement temporal query
        # TODO: 2 Implement lastN

    

        # Try to retrieve requested entity by id:        
        result, error = self.backend.get_entity_by_id(entityId)

        if error != None:
            return None, error




        return result, None

        # TODO: 2 Complete
       
    ############## END 5.7.3 - Retrieve temporal evolution of an Entity #############



    ############## BEGIN 5.7.4 - Query temporal evolution of entities #############

    def api_getTemporalEntities(self, args):

        isTemporalQuery, error = valid.validateTemporalQuery(args)

        # NGSI-LD spec section 5.7.4.4:

        if not isTemporalQuery or error != None:
            return None, error

        return None, util.NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")
        
    ############## END 5.7.4 - Query temporal evolution of entities #############






    ######### BEGIN Delete all entities (inofficial, only for testing!) ########
    def api_inofficial_deleteEntities(self):
        return self.backend.deleteAllEntities()                
    ######### END Delete all entities (inoffocial, only for testing!) ##########


