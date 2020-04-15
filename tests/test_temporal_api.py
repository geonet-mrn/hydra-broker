# -*- coding: utf-8 -*-

import requests, sys, os
import time
import unittest
import urllib.parse
from faker import Faker
fake = Faker()


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


      

    @unittest.skip("demonstrating skipping")
    def test_append_multiple_instances_at_once(self):

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
            "testprop" : []    
        }
        # TODO: 2 What if observedAt is the same?

        for i in range(5):
            
            fakeDate = fake.date_time_between(start_date='-10d', end_date='now')
            
            updateEntity['testprop'].append(
                {
                    "type" : "Property",
                    "value": "test",                    
                    "observedAt" : fakeDate.isoformat() + "Z"
                })
        

        #print(updateEntity)
        r = requests.post(temporalEntitiesUrl + "test/attrs/", json= updateEntity, auth=(username, password))
        
        self.assertEqual(r.status_code, 204)
        ################# END Append property instance temporal API (6.20.3.1 / 5.6.12) ###############

        
        r = requests.get(temporalEntitiesUrl + "test", auth=(username, password))
        self.assertEqual(r.status_code, 200)
     
        entity = r.json()
        
        # Property 'testprop' should now have instances:
        self.assertGreater(len(entity['testprop']), 1)



    def test_append_instances_at_different_times(self):

        ############# BEGIN Create entity via normal API (not temporal) ###########
        createEntity = {
            "id": "test",
            "type": "Test",
            "@context": []            
        }
        
        r = requests.post(entitiesUrl, json = createEntity, auth=(username, password))
        self.assertEqual(r.status_code, 201)
        print(r.text)
        
        ############# END Create entity via normal API (not temporal) ###########

        for i in range(0,3):
     
            ################# BEGIN Append property instance temporal API (6.20.3.1 / 5.6.12) ###############
            updateEntity = {
                "id": "test",
                "type": "Test",
                "@context": [],
                "testprop" : []    
            }
            
            fakeDate = fake.date_time_between(start_date='-10d', end_date='now')
                
            updateEntity['testprop'].append(
                {
                    "type" : "Property",
                    "value": "test",                    
                "observedAt" : fakeDate.isoformat() + "Z"
                })
            

            #print(updateEntity)
            r = requests.post(temporalEntitiesUrl + "test/attrs/", json= updateEntity, auth=(username, password))
            
            self.assertEqual(r.status_code, 204)
            ################# END Append property instance temporal API (6.20.3.1 / 5.6.12) ###############

            time.sleep(1)

        
        r = requests.get(temporalEntitiesUrl + "test", auth=(username, password))
        self.assertEqual(r.status_code, 200)
     
        entity = r.json()

        print(entity)
        
        # Property 'testprop' should now have instances:
        self.assertGreater(len(entity['testprop']), 1)