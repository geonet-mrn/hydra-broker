# -*- coding: utf-8 -*-

import requests, sys, os
import unittest
import urllib.parse

ngsiBaseUrl = os.environ["NGSI_ENDPOINT"]
username = os.environ["USERNAME"]
password = os.environ["PASSWORD"]

entitiesUrl = ngsiBaseUrl + "ngsi-ld/v1/entities/"
temporalEntitiesUrl = ngsiBaseUrl + "ngsi-ld/v1/temporal/entities/"

class TestTemporalApi(unittest.TestCase):

    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)


      


    def test(self):

        ############# BEGIN Create entity via normal API (not temporal) ###########
        createEntity = {
            "id": "test",
            "type": "Test",
            "@context": [],
            "testprop" : 
                {
                    "type" : "Property",
                    "value": "test",
                    "observedAt" : "2010-08-31T16:47:00Z"
                }
            
        }
        
        r = requests.post(entitiesUrl, json= createEntity, auth=(username, password))
        self.assertEqual(r.status_code, 201)
        print(r.text)
        
        ############# END Create entity via normal API (not temporal) ###########


        
        ################# BEGIN Test whether temporal representation has property arrays ###############
        
        # GET Temporal representation of entity:

        r = requests.get(temporalEntitiesUrl + "test", auth=(username, password))
        self.assertEqual(r.status_code, 200)

        entity = r.json()
        print(r.json())

        # Check whether property "testprop" is an array, as it is expected for the temporal reprsentation:
        self.assertTrue(isinstance(entity['testprop'], list))


        self.assertEqual(len(entity['testprop']), 1)
        
        ################# END Test whether temporal representation has property arrays ###############







        
        ################# BEGIN Append property instance temporal API (6.20.3.1 / 5.6.12) ###############
        updateEntity = {
            "id": "test",
            "type": "Test",
            "@context": [],
            "testprop" : 
                [{
                    "type" : "Property",
                    "value": "test",
                    "observedAt" : "2012-08-31T16:47:00Z"
                }]
            
        }
        # TODO: 2 What if observedAt is the same?

        
        r = requests.post(temporalEntitiesUrl + "test/attrs/", json= updateEntity, auth=(username, password))
        print(r.text)
        self.assertEqual(r.status_code, 204)
        ################# END Append property instance temporal API (6.20.3.1 / 5.6.12) ###############

        
        r = requests.get(temporalEntitiesUrl + "test", auth=(username, password))
        self.assertEqual(r.status_code, 200)

     
        entity = r.json()
        print(entity)

        # Property 'testprop' should now consist of 2 instances:
        self.assertEqual(len(entity['testprop']), 2)
