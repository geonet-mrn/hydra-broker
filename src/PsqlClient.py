# -*- coding: utf-8 -*-

# TODO: Understand:

# TODO: 2 Implement path parsing for "@value"

# TODO: 2 Automatically add 'createdAt' and 'modifiedAt' to entities

# TODO: 2 Find out exact definition of "ProblemDetails" type

# TODO: 2 Implement property validation, e.g. 4.6.2


import json


import re

from .PsqlBackend import PsqlBackend
from .NgsiLdUtil import *


#from .QueryParser import QueryParser


class PsqlClient:

    ######################## BEGIN init method ######################
    def __init__(self, config):

        self.config = config
        self.backend = PsqlBackend(config)
    ######################## END init method ######################




    ############################# BEGIN 5.6.1 - Create Entity ################################          
    def api_createEntity(self, json_ld):
        error = validateJsonLd(json_ld)

        if error != None:
            return None, error

        entity = json.loads(json_ld)
     
        return self.backend.createEntity_object(entity)
    ############################# END 5.6.1 - Create Entity ################################        


    ############################# BEGIN 5.6.2 - Update Entity Attributes ################################          
    def api_updateEntityAttributes(self, entity_id, entity_fragment_json_ld):        
        
        # TODO: 2 Validate entity ID

        ############### BEGIN Try to fetch entity from database ###############
        result, error = self.backend.getEntityById(entity_id)
        
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

        # Write changes to database:
        result, error = self.backend.upsertEntity_object(existing_entity)

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
        result, error = self.backend.getEntityById(entity_id)
        
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

        result, error = self.backend.upsertEntity_object(existingEntity)

        if error != None:
            return None, error


        return NgsiLdResult(None,204), None

    ################## END 5.6.3 - Append Entity Attributes ###################





    ############################# BEGIN 5.6.6 - Delete Entity ###############################
    def api_deleteEntity(self, id):

        return self.backend.deleteEntityById(id)
       
    ############################# END 5.6.6 - Delete Entity ###############################

  

    # TODO: 2 Implement
    def api_patchEntityAttributeById(self, entityId, attrId):
        return None, NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")

    # TODO: 2 Implement
    def api_deleteEntityAttributeById(self, entityId, attrId, deleteAll, datasetId):
        return None, NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")




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

            result, error = self.backend.getEntityById(id)

            if result != None:

                existingEntity = result.payload

                if options == "replace":
                
                    # TODO: 3 Handle delete errors? -> BatchEntityError
                    
                    # TODO: 2 Use upsert_entity_object method here?
                    
                    # Delete old entity:
                    self.backend.deleteEntityById(id)
                    
                    # Write new entity:
                    self.backend.createEntity_object(newEntity)
                
                elif options == "update":
                    # Append attributes as specified in 5.6.3:
                    
                    self.api_appendEntityAttributes(existingEntity, newEntity)

            else:
                # Write new entity:
                self.backend.createEntity_object(newEntity)
                                
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
            self.backend.deleteEntityById(id)
        ############ END Check all passed entities for errors before starting to write any of them #########

        return NgsiLdResult(None, 200), None
    
    ############## END 5.6.10 - Batch Entity Delete #############

    


    ############## BEGIN 5.6.11 - Create or Update Temporal Representation of an Entity #############

    def api_upsertTemporalEntities(self, json_ld):

        error = validateJsonLd(json_ld)

        if error != None:
            return None, error

   
        entity = json.loads(json_ld)

        # TODO: 1 Check if this is an EntityTemporal (or Entity?)

        existing_entity, statusCode, error = self.backend.getEntityById(entity['id'])

        if existing_entity == None:
            return self.createEntity_object(entity)

        updatedEntity = addTemporalAttributeInstances(existing_entity, entity)

        self.upsertEntity_object(updatedEntity)
        # 201 - Created
        # 204 - Updated
        return NgsiLdResult(None, 201), None
        #existingEntity = self.getEntityById

        # TODO: 2 Implement

    ############## END 5.6.11 - Create or Update Temporal Representation of an Entity #############


    ############## BEGIN 5.6.12 -  Add Attributes to Temporal Representation of an Entity #############
    def api_addTemporalEntityAttributes(self, entityId):
        # TODO: 2 Implement
        return None, NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")
    ############## END 5.6.12 -  Add Attributes to Temporal Representation of an Entity #############


    ############## BEGIN 5.6.13 - Delete Attribute from Temporal Representation of an Entity #############
    def api_deleteTemporalEntityAttributeById(self, entityId, attrId):
        # TODO: 2 Implement
        return None, NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")

    ############## END 5.6.13 - Delete Attribute from Temporal Representation of an Entity #############

    
    ############## BEGIN 5.6.14 - Modify attribute instance from Temporal Representation of an Entity #############
    def api_modifyTemporalEntityAttributeInstance(self, entityId, attrId, instanceId):
        # TODO: 2 Implement
        return None, NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")
    ############## END 5.6.14 - Modify attribute instance from Temporal Representation of an Entity #############


    ############## BEGIN 5.6.15 - Delete attribute instance from Temporal Representation of an Entity #############
    def api_deleteTemporalEntityAttributeInstance(self, entityId, attrId, instanceId):
        # TODO: 2 Implement
        return None, NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")
    ############## END 5.6.15 - Delete attribute instance from Temporal Representation of an Entity #############


    ############### BEGIN 5.6.16 - Delete Temporal Representation of an Entity ################
    def api_deleteTemporalEntityById(self, entityId):
        # TODO: 2 Implement
        return None, NgsiLdError("OperationNotSupported", "This operation is not implemented yet.")
    ############### END 5.6.16 - Delete Temporal Representation of an Entity ################



    #################### BEGIN 5.7.1 - Retrieve Entity ######################
    def api_getEntityById(self, id, args = []):
        return self.backend.getEntityById(id, args)
    #################### END 5.7.1 - Retrieve Entity ######################


    def api_queryEntities(self, args):
        return self.backend.queryEntities(args)


    ############## BEGIN 5.7.3 - Retrieve temporal evolution of an Entity #############
    def api_getTemporalEntityById(self, entityId, args):

        # 5.7.3.4:

        # TODO: 2 check entityID -> if not present or valid, return BadRequestData error

        # Try to retrieve requested entity by id:
        #entity, responseCode, error = self.api_getEntityById(entityId)
        result, error = self.backend.getEntityById(entityId)

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











    ################### BEGIN Inofficial API methods (not part of NGSI-LD specification!) ###################

    ######### BEGIN Delete all entities (inofficial, only for testing!) ########
    def api_inofficial_deleteEntities(self):
        return self.backend.deleteAllEntities()        
        
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
        
        existingEntity = self.backend.getEntityById(entity['id'])

        if existingEntity != None:            
            result, error = self.backend.deleteEntityById(entity['id'])
        
        
        # TODO: 3 Return different status code for update/creation here?

        return self.backend.createEntity_object(entity)

    ######### END Upsert entity (inofficial!) ###############

    ################### END Inofficial API methods (not part of NGSI-LD specification!) ###################


