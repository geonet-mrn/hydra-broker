import json, re

# TODO: 3 Understand temporal properties (string or not? When string, when object?, See NGSI-LD spec 4.8)

# This is with variable time zone:
#regexp_dateTime_iso8601 = re.compile("^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$")

# This is with fixed zulu time zone (UTC), as required by NGSI-LD:
regexp_dateTime_iso8601 = re.compile("^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?Z")

regexp_date_iso8601 = re.compile("^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$")
    


################## BEGIN Class NgsiLdError ################
class NgsiLdError:
    def __init__(self, type, detail = ""):
        self.type = type
        self.detail = detail
################## END Class NgsiLdError ##################


class NgsiLdResult:
    def __init__(self, payload, statusCode):
        self.payload = payload
        self.statusCode = statusCode


def addTemporalAttributeInstances(entity_temporal, entity_temporal_fragment):

    print(entity_temporal)

    result = {}

    for key, value in entity_temporal.items():
        result[key] = value


    # TODO: 2 Add random instance id?

    for key, value in entity_temporal_fragment.items():

        if key == 'id' or key == 'type' or key == "@context":
            continue

        source = entity_temporal_fragment[key]

        if not isinstance(source, list):
            source = [source]


        target = []

        if key in entity_temporal:

            target = entity_temporal[key]

            if not isinstance(target, list):
                target = [target]

        target.extend(source)
            
        result[key] = target

    return result
    


def createEntityTemporal(entity, temporalQuery):

    # See NGSI-LD spec section 5.2.4
    #
    # properties "id" and "type" always have cardinality 1, 
    # i.e. they are not converted to arrays for the temporal representation
    
    result = {}

    for key, value in entity.items():

        if key == 'id' or key == 'type':
            result[key] = value
            continue

        if isinstance(value, list):
            result[key] = value
            continue

        # TODO: 2 Take into account the temporal query (5.7.3.4)

        result[key] = [value]


    return result



def validateDatetimeString(datetime):

    # Valiate DateTime strings according to NGSI-LD spec section 4.6.3
    
    # NOTE: 
    # - NGSI-LD expects the full ISO 8601 representation. Short forms (e.g. date only) are not permitted!
    # - Time zone must be "Z" / zulu (UTC)

    return regexp_dateTime_iso8601.match(datetime)# or regexp_date_iso8601.match(datetime)
    

############# BEGIN Validate entity according to NGSL-LD spec section 4.5.1 ############### 
def validateEntity_object(entity):

    # TODO: 2 Check whether the error type here should really be "BadRequestData" or "InvalidRequest"

    # NOTE: Actually, we shouldn't return NGSI-LD errors here since this is not an API function

    if not isinstance(entity, dict):
        return NgsiLdError("BadRequestData", "NGSI-LD entities are represented as JSON-LD dictionaries. However, the passed object is not a JSON-LD dictionary: " + str(entity))


    
    if not 'id' in entity:
        return NgsiLdError("BadRequestData", "Entity is missing 'id' property.")
        
    if not 'type' in entity:
        return NgsiLdError("BadRequestData", "Entity is missing 'type' property.")

    if not '@context' in entity:
        return NgsiLdError("BadRequestData", "Entity is missing '@context' property")

    if not isinstance(entity['@context'], list):
        return NgsiLdError("BadRequestData", "Member '@context' is not an array")


    ################# BEGIN Validate properties ################
    for key, value in entity.items():

        # Skip required default properties (these are already checked above):
        if key == 'id' or key == 'type' or key == '@context':
            continue

        error = validate_entity_member(entity[key])

        if error != None:
            return error


    ################# END Validate properties ################

    return None

############# END Validate entity according to NGSL-LD spec section 4.5.1 ############### 



############# BEGIN Validate entity property according to NGSL-LD spec section 4.5.2 ############### 
def validate_entity_member(member):

    # NOTE: A member can be a property or a relationship

    if not isinstance(member, dict):
        return NgsiLdError("BadRequestData", "Member is not a JSON object: " + str(member))

    if not 'type' in member:
        return NgsiLdError("BadRequestData", "Member is missing attribute 'type': " + str(member))
        

    
    if member['type'] == 'GeoProperty':
        # TODO: 3 Take into account NGSI-LD spec section 4.7.2!

        if not 'value' in member:
            return NgsiLdError("BadRequestData", "GeoProperty is missing attribute 'value'")

        geojson = member['value']

        if not 'type' in geojson:
            return NgsiLdError("BadRequestData", "GeoJSON is missing attribute 'type'")

        geometryTypes = ['Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']

        if not geojson['type'] in geometryTypes:
            return NgsiLdError("BadRequestData", "Invalid GeoJSON geometry type: " + geojson['type'])

        if not 'coordinates' in geojson:
            return NgsiLdError("BadRequestData", "GeoJSON is missing attribute 'coordinates'")

        coordinates = geojson['coordinates']
        type == geojson['type']

        if not isinstance(coordinates, list):
            return NgsiLdError("BadRequestData", "GeoJSON 'coordinates' attribute is not an array.")

        if type == 'Point':
            for item in coords:
                if not isinstance(item, numbers.Number):
                    return NgsiLdError("BadRequestData", "Element in GeoJSON 'Point' type coordinates array is not a number: " + item)
        
        # TODO: 3 Implement checking or remaining GeoJSON geometry types


    

    elif member['type'] == 'Property':

        if not 'value' in member:
            return NgsiLdError("BadRequestData", "Property is missing attribute 'value'")

        if isinstance(member['value'], dict):
            if not '@type' in member['value']:
                return NgsiLdError("BadRequestData", "Property value is missing attribute '@type': " + str(member))

            if not '@value' in member['value']:
                return NgsiLdError("BadRequestData", "Property value is missing attribute '@value': " + str(member))


        if 'observedAt' in member and not validateDatetimeString(member['observedAt']):
            return NgsiLdError("BadRequestData", "'observedAt' is not a valid ISO-8601 datetime string.")

        if 'createdAt' in member and not validateDatetimeString(member['createdAt']):
            return NgsiLdError("BadRequestData", "'createdAt' is not a valid ISO-8601 datetime string.")
        
        if 'modifiedAt' in member and not validateDatetimeString(member['modifiedAt']):
            return NgsiLdError("BadRequestData", "'modifiedAt' is not a valid ISO-8601 datetime string.")
        
        # TODO: 3 Add remaining checks for optional attributes


    elif member['type'] == 'Relationship':
        # TODO: 2 Validate relationships

        if not 'object' in member:
            return NgsiLdError("BadRequestData", "Relationship is missing attribute 'object'")
        
    else:
        return NgsiLdError("BadRequestData", "Invalid member type: " + member['type'])

    return None
############# END Validate entity property according to NGSL-LD spec section 4.5.2 ############### 
    


def validateGeoQuery(args):

    # NGSI-LD spec section 4.10

    ##### BEGIN Check whether one of the geo query arguments are present. If not, this is no geo query #####
    geoQueryArgs = ['georel', 'geometry', 'coordinates']

    hasGeoQueryArg = False
    
    for arg in geoQueryArgs:
        if arg in args:
            hasGeoQueryArg = True
            break

    
    if not hasGeoQueryArg:
        return False, None

    ##### END Check whether one of the geo query arguments are present. If not, this is no geo query #####
    
    ############# BEGIN Check whether all required geo query args are present #############
    for arg in geoQueryArgs:
        if not arg in args:
            return True, NgsiLdError("BadRequestData", "Missing geo query parameter: " + arg)
    ############# END Check whether all required geo query args are present #############
    

    geo_rel = args.get('georel')
    geo_compareGeomType = args.get('geometry')

    
    ############ BEGIN Try to parse coordinates as JSON ##############

    # ATTENTION: This is no check for GeoJSON conformity!

    try:
        geo_compareCoordinates = json.loads(args.get('coordinates'))
    except:
        return True, NgsiLdError("BadRequestData", "geo query coordinates are not valid JSON: " + args.get('coordinates'))
    ############ END Try to parse coordinates as JSON ##############

            

    geoRelParts = geo_rel.split(';')

    ############# BEGIN Check geo operation parameter ##########
    geoOp = geoRelParts[0]            

    compareGeoJsonString = json.dumps({"type": geo_compareGeomType, "coordinates": geo_compareCoordinates})

    validGeoOps = ['near', 'within', 'contains', 'intersects', 'equals', 'disjoint', 'overlaps']
    
    if not geoOp in validGeoOps:
        return True, NgsiLdError("BadRequestData", "Invalid value for 'georel': '" + geoOp + "'. Must be one of: " + ', '.join(validGeoOps))                                
    ############# END Check geo operation parameter ##########


    if geoOp == 'near':
        
        if len(geoRelParts) != 2:
            return True, NgsiLdError("BadRequestData", "Invalid geo query")                

        # TODO: 4 Allow '=' in addition to '==' in distance part (would be non-standard) ?
        distanceParts = geoRelParts[1].split('==')

        if len(distanceParts) != 2:
            return True, NgsiLdError("BadRequestData", "Invalid geo query")                

        ################### BEGIN Read distance value ###############
        distValue = -1

        try:
            distValue = float(distanceParts[1])
        except:
            return True, NgsiLdError("BadRequestData", "Invalid distance value in geo query")                                    

        if distValue < 0:
            return True, NgsiLdError("BadRequestData", "Distance value must be a positive number")                                    
        ################### END Read distance value ###############

        if not (distanceParts[0] == 'maxDistance' or distanceParts[0] == 'minDistance'):
            return None,  None, NgsiLdError("BadRequestData", "Invalid geo query: georel must be either 'maxDistance' or 'minDistance'.")                
   
    return True, None




################### BEGIN 5.5.4 - JSON-LD Validation ######################        
def validateJsonLd(json_ld):
        
    try:
        json_ld = json.loads(json_ld)
    except:
        return NgsiLdError("InvalidRequest", "The request payload is not valid NGSI-LD.")
        
    return None
################### END 5.5.4 - JSON-LD Validation ######################        



###################### BEGIN Validate temporal query ######################
def validateTemporalQuery(args):

    ##### BEGIN Check whether all required geo query arguments are present #####
    temporalQueryArgs = ['timerel', 'time', 'endtime', 'before', 'after', 'between']

    hasTimeRelArg = False
    
    for arg in temporalQueryArgs:
        if arg in args:
            hasTimeRelArg = True
            break
    ####### END Check whether all required geo query arguments are present ######


    if not hasTimeRelArg:
        return False, None


    # TODO: 2 Implement matching with short form properties 
    # (e.g. {"observedAt" : "2017-05-04T12:30:00Z"}, as opposed to full property object notation?)

    if not 'timerel' in args:
        # TODO: 4 Return correct error message
        return True, NgsiLdError("BadRequestData", "Missing temporal query parameter 'timerel'")  

    timerel = args['timerel']
    
    if not 'time' in args:
        # TODO: 4 Return correct error message
        return True, NgsiLdError("BadRequestData", "Missing temporal query parameter 'time'")  

    time = args['time']

    # Check argument 'time' for ISO-8601 format:
    if not validateDatetimeString(time):
        # TODO: 4 Return correct error message
        return True, NgsiLdError("BadRequestData", "'time' is not a valid ISO-8601 datetime string: " + time)                  

    
    # NOTE: 
    # Time comparison as strings works without conversion to a date object, 
    # as long as the time zone is the same. This should always be the case, 
    # since NGSI-LD expects all times to be expressed in UTC (see NGSI-LD spec 4.6.3).


    if timerel == 'before':
        return True, None
    
    elif timerel == 'after':
        return True, None
        
    elif timerel == 'between':
        if not 'endtime' in args:
            return True, NgsiLdError("BadRequestData", "Missing temporal query parameter 'endtime'")

        endtime = args['endtime']

        if not validateDatetimeString(endtime):
            return True, NgsiLdError("BadRequestData", "'endtime' is not a valid ISO-8601 datetime string: " + endtime)                    
        
        if endtime < time:
            return True, NgsiLdError("BadRequestData", "'endtime' must represent a moment after 'time'.")


    else:
        return True, NgsiLdError("BadRequestData", "Invalid value for 'timerel': " + timerel + ". Must be one of: 'before', 'after', 'between'.") 

    # TODO: 1 Understand why this is necessary
    return True, None

###################### END Validate temporal query ######################