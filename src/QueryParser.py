# -*- coding: utf-8 -*-
import re
import dateutil.parser

class QueryParser:

    def __init__(self, backend):

        self.backend = backend
        # ATTENTION: It is important that the symbols are ordered by decreasing length!
        #self.symbols =   ['==', '!=', '<=', '>=', '~=', '<', '>', '|', ';', ]

        # NOTE: The '~~' operator ("contains substring") is nof official NGSI-LD!
        self.operators = ['==', '!=', '<=', '>=', '~=', '~~', '<', '>', '|', ';']

        self.symbols = self.operators.copy()
        self.symbols.extend(['(', ')'])

        # Check for ISO 8601 format:
        # see https://stackoverflow.com/questions/28020805/regex-validate-correct-iso8601-date-string-with-time/28022901                                   
        # Doesn't work because it expects milliseconds?
        #regExp = "^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:Z|[+-][01]\d:[0-5]\d)$"                    
                    
        # Works for NGSI DateTime:
        self.regexp_dateTime_iso8601 = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T([0-9]{2}:){2}[0-9]{2}[+|-][0-9]{2}:[0-9]{2}")
  

    def tokenize(self, query):

        result = []

        collect = ""

        while(len(query) > 0):

            symbolFound = None

            ########### BEGIN Test for known symbol #########

            # ATTENTION: The following for loop only works correctly
            # if self.symbols is ordered by item string length!
            # TODO: 2 Work with an ordered copy of self.symbols here

            for symbol in self.symbols:

                if query[:len(symbol)] == symbol:
                    symbolFound = symbol
                    break
            ########### END Test for known symbol #########

            if symbolFound:

                if len(collect) > 0:
                    result.append(collect)

                collect = ""

                result.append(symbolFound)
                query = query[len(symbolFound):]

            else:
                collect += query[:1]
                query = query[1:]

        if len(collect) > 0:
            result.append(collect)

        return result

    ##################### END Tokenize ##########################

    ################## BEGIN Parse parantheses #######################
    def parseParantheses(self, tokens, index):

        result = []

        while index < len(tokens):

            token = tokens[index]

            if token == '(':
                group, index = self.parseParantheses(tokens, index+1)
                result.append(group)

            elif token == ')':
                return (result, index)

            else:
                result.append(token)

            index += 1

        return (result, index)
    ################## END Parse parantheses #######################

    def buildAST(self, items):
       
        for ii in range(len(items)):
            if isinstance(items[ii], list):
                items[ii] = self.buildAST(items[ii])

        # ATTENTION: This copy is required!
        result = items

        for operator in self.operators:
            result = self.processOperator(result, operator)

        return result

    def processOperator(self, items, operator):

        if not isinstance(items, list):
            return items

        result = []

        index = 0

        didReplace = False

        while index < len(items):

            if index < len(items) - 1 and items[index+1] == operator:
                result.append([items[index], items[index+1], items[index+2]])
                index += 2
                didReplace = True
            else:
                result.append(items[index])

            index += 1

        # NOTE: In order to make sure that we process all operators, we need to repeat
        # until no further change is made:
        if didReplace:
            result = self.processOperator(result, operator)

        # Remove unneccessary double-nested parantheses:
        if len(result) == 1:
            result = result[0]

        return result

    ############################ BEGIN Evaluate AST #############################
    def evaluate(self, ast, entity):

        # AST is not a list:
        if not isinstance(ast, list):
            
            value = (self.getPropertyValue(entity, ast) != None)
            #print("NOT A LIST ", ast, "value ", value)
            return value

        values = []
        ############# BEGIN Evaluate subterm elements ##############
        for item in ast:

            # NOTE: We need to do the isistance() check here and not in evaluate() since it must not
            # be performed when evaluate() is called first, i.e. at the top level of the AST. This is required
            # for queries that only check the existence of a property (independent of value) and should return True if it exists.

            if isinstance(item, list):
                item = self.evaluate(item, entity)
            
            values.append(item)
        ############# END Evaluate subterm elements ##############

        # Binary operator:
        if len(values) == 3:

            # Get value of left-side property path expression:
            left = self.getPropertyValue(entity, values[0])                       

            if left == None:
                return False           
              
            # Prepare operands and operator:
            left = self.smartCast(left)
            right = self.smartCast(values[2])
            op = values[1]

            # TODO: 3 NGSI-LD section 4.9:
            # "If the target element corresponds to a Relationship, the combination of such target element with any operator different
            # than equal or unequal shall result in not matching.""


            try:
                if op == '==':
                    return left == right
                elif op == '!=':
                    return left != right                
                elif op == '~=':
                    regexp = re.compile(right)
                    return regexp.match(left)
                # NOTE: '~~' is no official NGSI-LD operator!
                elif op == '~~':
                    return right in left    
                elif op == '>=':
                    return left >= right            
                elif op == '<=':
                    return left <= right              
                elif op == '<':
                    return left < right                  
                elif op == ';':            
                    return left and right
                elif op == '>':
                    return left > right
                elif op == '|':
                    return left or right
            except Exception as e:
                print(e)
                print("Compare: ", left, type(left), op, right, type(right))
                
        else:
            print("Unsupported term: " + str(values))
        
        return False
    ############################ END Evaluate AST #############################  
  
    def getPropertyValue(self, entity, pathExpression):

        if not isinstance(pathExpression, str):
            return pathExpression

        ############# BEGIN Parse property path ############
        trailingPathStart = pathExpression.find('[')

        trailingPathList = []

        #print("PATH EXPRESSION:",pathExpression)

        if trailingPathStart > -1 and pathExpression[-1] == ']':                                                            
            propertyPath = pathExpression[:trailingPathStart]
            trailingPathList = pathExpression[trailingPathStart + 1: -1].split("][")                                    
        else:            
            propertyPath = pathExpression
        ############# END Parse property path ############

        ############# BEGIN Get property value ##############
        value = entity

        #print("PROPERTY PATH: " + propertyPath)

        ######################## BEGIN Step through property path ########################
        for key in propertyPath.split('.'):        

            # TODO: 1 Do we need this check?
            # If a property that is mentioned in the query does not exist in the entity, 
            # the respective query term evaluates as None:
            #if not isinstance(value,dict):                
            #    return None


            # NOTE: The order of checking here is important for querying of property metadata:
            # We *first* check if the key exists on the current level. 
            # -> If it *does*, we step into the branch of the key, and can query metadata this way.
            # -> if it *doesn't*, we assume that the key is a child of the property's 'value' node and thus
            #    step into the value node. Not sure whether this conforms to the NGSI-LD specification, but
            #    at the moment (2019-11-03) it appears to the solution which combines my current
            #    interpretation of the spec with query feature richness in the best way. sbecht 2019-11-03.

            if key in value:
                # One step deeper:
                value = value[key]  
                                              
            else:            
                if 'value' in value:
                    value = value['value']                    
                #elif '@value' in value:
                #    value = value['@value']                    

            ################## BEGIN Follow relationship link ################
            # Properties and Relationships must have a type:
            if isinstance(value,dict) and 'type' in value and value['type'] == 'Relationship':                
                                
                if not 'object' in value:                
                    return None

                objectUri = value['object']

                # TODO: 2 Cache referenced entities?
                result, error = self.backend.api_getEntityById(objectUri, [])

                if result == None:
                    return None

                value = result.payload
            ################## END Follow relationship link ################
        
            

        #################### END Step through property path ########################
            
        ################## BEGIN Try to step into property value attribute ##################
        # If we have arrived at the end of the property path
        # and the last value is a dictionary with a 'value' key,
        # i.e. an NGSI property, then set its value as the 'value' variable:
        
        if isinstance(value, dict):
            if 'value' in value:
                value = value['value']
            #elif '@value' in value:
            #    value = value['@value']                        
        ################## END Try to step into property value attribute ##################
        
        ############## BEGIN Process trailing path ###############
        for key in trailingPathList:
            if not isinstance(value,dict):
                return None

            if key in value:
                # One step deeper:
                value = value[key]                
            else:            
                return None
        ############## END Process trailing path ###############

        # If 'value' is *still* the whole entity, i.e. the the level where we started, this means
        # that the property we searched for does not exist in the entity. In this case, return None:
        if value == entity:
            return None


        return value
        

    def smartCast(self, value):

        if not isinstance(value,str):
            return value
 
        ############### BEGIN Try to parse value to float ###########
        try:
            value = float(value)
        except:
            pass
        ############### END Try to parse value to float ###########

        ################## BEGIN Try to parse value to datetime ###############
        
        # NOTE: We only want to try to convert *strings* to DateTime. If a value has already been casted to a float,
        # we assume that it really should be just a float!
        if isinstance(value,str):
            try:
                #if self.regexp_dateTime_iso8601.match(value):
                #    value = dateutil.parser.parse(value)                
                value = dateutil.parser.parse(value)                
            except:
                pass

            # NOTE: Uppercase 'True' and 'False' are against official NGSI-LD spec!
            if value == 'True' or value == 'true':
                return True
            
            if value == 'False' or value == 'false':
                return False
        ################## END Try to parse value to datetime ###############

        return value
      