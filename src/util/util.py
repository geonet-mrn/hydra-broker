import json, re
    


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




def entity_to_single(entity):

    print(entity)

    result = {
     
    }

    
    for key, value in entity.items():

        # Skip required default properties (these are already checked above):
        if key == 'id' or key == 'type' or key == '@context':
            result[key] = value
            continue

        if isinstance(value, list):
            if len(value) > 0:
                result[key] = value[0]
        else:
            result[key] = value


    return result



def entity_to_temporal(entity):

    result = {
      
    }
    
    for key, value in entity.items():

        # Skip required default properties (these are already checked above):
        if key == 'id' or key == 'type' or key == '@context':
            result[key] = value
            continue

        if not isinstance(entity[key], list):
            result[key] = [value]
        else:
            result[key] = value
    
    return result
