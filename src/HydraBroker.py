# -*- coding: utf-8 -*-

# TODO: 3 Implement "/entities/<entityId>/attrs"
# TODO: 4 Implement 6.3.11
# TODO: 4 Implement 6.3.12
# TODO: 3 Implement context sources (NGSI-LD spec section 6.8 to 6.13)
# TODO: 3 Implement 6.20
# TODO: 3 Implement 6.21

import os
import json
import psycopg2
import sys
from flask import Flask, request, render_template, url_for, redirect, Response
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

from .PsqlClient import PsqlClient

from .NgsiLdUtil import *

if sys.version_info[0] < 3:
    raise Exception("Hydra must be run with Python 3!")

################## BEGIN Reading config from JSON file ###############
config = None
#configFilePath = sys.argv[1]
configFilePath = "hydraconfig.json"

try:
    with open(configFilePath) as json_file:
        config = json.load(json_file)
except:
    print("Failed to read config file: " + str(configFilePath))
    exit(-1)

if config == None:
    exit(-1)
################## END Reading config from JSON file ###############

backend = PsqlClient(config)

app = Flask(__name__)
auth = HTTPBasicAuth()

urlBasePath = "/ngsi-ld/v1"


###################### BEGIN Official endpoints as defined by NGSI-LD specification #######################

# NOTE: The route for the /entities/ endpoints *must* end with a '/' character!
# Otherwise, the server returns response code 308 on requests with basic authentication.
# Also, the trailing '/' is expected according to the NGSI-LD specification.

# 6.4.3.1 - POST /entities/
@app.route(urlBasePath + "/entities/", methods=['POST'])
@auth.login_required
def postEntities():
    return createResponse(backend.api_createEntity(request.data))


# 6.4.3.2 - GET /entities/:
@app.route(urlBasePath + "/entities/", methods=['GET'])
def getEntities():
    return createResponse(backend.api_queryEntities(request.args))




# 6.5.3.1 - GET /entities/<entityId>:
@app.route(urlBasePath + "/entities/<entityId>", methods=['GET'])
def getEntityById(entityId):

    attrs = None
    if 'attrs' in request.args:
        attrs = request.args

    return createResponse(backend.api_getEntityById(entityId, attrs))


# 6.5.3.2 - DELETE /entities/<entityId>:
@app.route(urlBasePath + "/entities/<entityId>", methods=['DELETE'])
@auth.login_required
def deleteEntityById(entityId):
    return createResponse(backend.api_deleteEntity(entityId))




# 6.6.3.1 - POST /entities/<entityId>/attrs/:
@app.route(urlBasePath + "/entities/<entityId>/attrs/", methods=['POST'])
@auth.login_required
def postEntityAttributes(entityId):

    overwrite = True
    if 'options' in request.args and request.args['options'] == "noOverwrite":
        overwrite = False

    return createResponse(backend.api_appendEntityAttributes(entityId, request.data, overwrite))


# 6.6.3.2 - PATCH /entities/<entityId>/attrs/:
@app.route(urlBasePath + "/entities/<entityId>/attrs/", methods=['PATCH'])
@auth.login_required
def patchEntityAttributes(entityId):
    return createResponse(backend.api_updateEntityAttributes(entityId, request.data))




# 6.7.3.1 - PATCH /entities/<entityId>/attrs/<attrId>:
@app.route(urlBasePath + "/entities/<entityId>/attrs/<attrId>", methods=['PATCH'])
@auth.login_required
def patchEntityAttributeById(entityId, attrId):
    return createResponse(backend.api_patchEntityAttributeById(entityId, attrId))


# 6.7.3.2 - DELETE /entities/<entityId>/attrs/<attrId>:
@app.route(urlBasePath + "/entities/<entityId>/attrs/<attrId>", methods=['DELETE'])
@auth.login_required
def deleteEntityAttributeById(entityId, attrId):

    deleteAll = False
    if 'deleteAll' in request.args:
        deleteAll = request.args['deleteAll']

    datasetId = None
    if 'datasetId' in request.args:
        datasetId = request.args['datasetId']

    return createResponse(backend.api_deleteEntityAttributeById(entityId, attrId, deleteAll, datasetId))




# 6.14.3.1 - POST entityOperations/create:
@app.route(urlBasePath + "/entityOperations/create", methods=['POST'])
@auth.login_required
def entityOperationsCreate():
    # NOTE: /entityOperations/create has the same behavior as /entityOperations/upsert 
    # with "options" parameter set to "replace", so we can use the upsert method to implement this endpoint:
    return createResponse(backend.api_entityOperationsUpsert(request.data, "replace"))


# 6.15.3.1 - POST entityOperations/upsert:
@app.route(urlBasePath + "/entityOperations/upsert", methods=['POST'])
@auth.login_required
def entityOperationsUpsert(options="replace"):
    return createResponse(backend.api_entityOperationsUpsert(request.data, options))


# 6.17.3.1 - POST entityOperations/delete:
@app.route(urlBasePath + "/entityOperations/delete", methods=['POST'])
@auth.login_required
def entityOperationsDelete():
    return createResponse(backend.api_entityOperationsDelete(request.data))



# TODO: 3 Implement context sources (NGSI-LD spec section 6.8 to 6.13)


# 6.18.3.1 - POST temporal/entities/:
@app.route(urlBasePath + "/temporal/entities/", methods=['POST'])
@auth.login_required
def postTemporalEntities():
    return createResponse(backend.api_upsertTemporalEntities(request.data))


# 6.18.3.2 - GET temporal/entities/:
@app.route(urlBasePath + "/temporal/entities/", methods=['GET'])
def getTemporalEntities():
    return createResponse(backend.api_getTemporalEntities(request.args))




# 6.19.3.1 - GET temporal/entities/<entityId>:
@app.route(urlBasePath + "/temporal/entities/<entityId>", methods=['GET'])
def getTemporalEntityById(entityId):
    return createResponse(backend.api_getTemporalEntityById(entityId, request.args))


# 6.19.3.2 - DELETE temporal/entities/<entityId>:
@app.route(urlBasePath + "/temporal/entities/<entityId>", methods=['DELETE'])
def deleteTemporalEntityById(entityId):
    return createResponse(backend.api_deleteTemporalEntityById(entityId))



# 6.20.3.1 - POST temporal/entities/<entityId>/attrs/:
@app.route(urlBasePath + "/temporal/entities/<entityId>/attrs/", methods=['POST'])
def postTemporalEntityAttributes(entityId):
    return createResponse(backend.api_addTemporalEntityAttributes(entityId))


# 6.21.3.1 - DELETE temporal/entities/<entityId>/attrs/<attrId>:
@app.route(urlBasePath + "/temporal/entities/<entityId>/attrs/<attrId>", methods=['DELETE'])
def deleteTemporalEntityAttributeById(entityId, attrId):
    return createResponse(backend.api_deleteTemporalEntityAttributeById(entityId, attrId))


# 6.22.3.1 - PATCH temporal/entities/<entityId>/attrs/<attrId>/<instanceId>:
@app.route(urlBasePath + "/temporal/entities/<entityId>/attrs/<attrId>/<instanceId>", methods=['PATCH'])
def patchTemporalEntityAttributeInstance(entityId, attrId, instanceId):
    return createResponse(backend.api_modifyTemporalEntityAttributeInstance(entityId, attrId, instanceId))


# 6.22.3.2 - DELETE temporal/entities/<entityId>/attrs/<attrId>/<instanceId>:
@app.route(urlBasePath + "/temporal/entities/<entityId>/attrs/<attrId>/<instanceId>", methods=['DELETE'])
def deleteTemporalEntityAttributeInstance(entityId, attrId, instanceId):
    return createResponse(backend.api_deleteTemporalEntityAttributeInstance(entityId, attrId, instanceId))

###################### END Official endpoints as defined by NGSI-LD specification #######################








########################## BEGIN Inofficial endpoints (not defined in NGSI-LD specification) #########################

# DELETE /entities/ - Delete all entities. Not part of the official NGSL-LD specification!
@app.route(urlBasePath + "/entities/", methods=['DELETE'])
@auth.login_required
def entitiesDelete():
    return createResponse(backend.api_inofficial_deleteEntities())


# PUT /entities/<entityId> - Upsert entity. Not part of the official NGSL-LD specification!
@app.route(urlBasePath + "/entities/<entityId>", methods=['PUT'])
@auth.login_required
def entitiesIdPut(entityId):
    return createResponse(backend.api_inofficial_upsertEntity(request.data))

########################## END Inofficial endpoints (not defined in NGSI-LD specification) #########################







################################## BEGIN Response builder function ##############################

def createResponse(apiResult):

    #result, statusCode, error = apiResult
    result, error = apiResult

    if result == None and error == None:
        error = NgsiLdError("InternalError", "The broker back-end did not return a specific result or error.")

    # HTTP status codes according to NGSI-LD spec section 6.3.2
    # Detailed error messages according to NGSI-LD spec section 5.5.2

    errors = {
        "InvalidRequest": [400, "The request associated to the operation is syntactically invalid or includes wrong content"],
        "BadRequestData": [400, "The request includes input data which does not meet the requirements of the operation"],
        "AlreadyExists": [409, "The referred element already exists"],
        "OperationNotSupported": [422, "The operation is not supported"],
        "ResourceNotFound": [404, "The referred resource has not been found"],
        "InternalError": [500, "There has been an error during the operation execution"],
        "TooComplexQuery": [403, "The query associated to the operation is too complex and cannot be resolved"],
        "TooManyResults": [403, "The query associated to the operation is producing so many results that can exhaust client or server resources. It should be made more restrictive"]
    }

    if error != None:
       
        ############ BEGIN Create Error payload according to NGSL-LD spec 5.5.3 ###########
        errorStatusCode = errors[error.type][0]

        responseData = {"type": "http://uri.etsi.org/ngsi-ld/errors/" + error.type,
                        "title": errors[error.type][1],
                        "status": errorStatusCode,
                        "detail": error.detail                    
                        }

        response = Response(json.dumps(responseData), mimetype = 'application/json', status = errorStatusCode)
        ############ END Create Error payload according to NGSL-LD spec 5.5.3 ###########

    else:   
        # success response:     
        response = Response(json.dumps(result.payload), mimetype = 'application/json', status = result.statusCode)


    # TODO: 3 Make addition of access-control-allow-origin configurable
    response.headers['Access-Control-Allow-Origin'] = '*'

    return response
############################### END Response builder function ###############################


################## BEGIN Password verification function ####################
@auth.verify_password
def verifyPassword(username, password):
    if username in config['hydra_users']:
        return config['hydra_users'].get(username) == password
    return False
################## END Password verification function ####################



# ATTENTION: The '__name__' check is required to run multiple Gunicorn3 workers!
if __name__ == '__main__':

    ########## BEGIN Create PID file ##########

    # NOTE:
    # The PID file can be used by shell scripts to shut down Hydra's
    # python3 instance, e.g. afer unit tests are completed.

    pid = str(os.getpid())
    pidfile = open("hydrabroker.pid", 'w')
    pidfile.write(pid)
    pidfile.close()
    ########## BEGIN Create PID file ##########

    print("Starting Hydra broker")

    # For Gunicorn3:
    app.run()

    print("Shutting down Hydra broker")
    # os.unlink(pidfile)

    # TODO: 3 Read port from command line

    # For development:
    #app.run(host='0.0.0.0', port = config['hydra_port'])
