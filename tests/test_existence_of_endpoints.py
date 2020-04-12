# -*- coding: utf-8 -*-

# This is Python 3

# TODO: 2 Implement test for NGSI-LD dateTime properties - Done?
# TODO: 2 Implement test of path parsing for "@value" - Done?

import requests, sys, os
import unittest
import urllib.parse

ngsiBaseUrl = os.environ["NGSI_ENDPOINT"]
username = os.environ["USERNAME"]
password = os.environ["PASSWORD"]

entitiesUrl = ngsiBaseUrl + "ngsi-ld/v1/entities/"
temporal_entities_url = ngsiBaseUrl + "ngsi-ld/v1/temporal/entities/"
batch_operations_url = ngsiBaseUrl + "ngsi-ld/v1/entityOperations/"


class TestExistenceOfEndpoints(unittest.TestCase):

        
    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)

        # Create Point entity:
        entity = {
            "id": "test",
            "type": "Test",
            "@context": [],
            "test_attr": {
                "type" : "Property",
                "value" : "test"
            }
        }

        r = requests.post(entitiesUrl, json=entity, auth=(username, password))
        self.assertEqual(r.status_code, 201)

    
    def test_existence_of_endpoint_entities(self):

        r = requests.get(entitiesUrl + "?type=Test", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)

        r = requests.post(entitiesUrl, auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)


    
    def test_existence_of_endpoint_entities_entityid_attrs(self):

        r = requests.post(entitiesUrl + "test/attrs", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
        

        r = requests.patch(entitiesUrl + "test/attrs", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
        


    def test_existence_of_endpoint_entities_entityid_attrs_attrid(self):

        r = requests.patch(entitiesUrl + "test/attrs/test_attr", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
        

        r = requests.delete(entitiesUrl + "test/attrs/test_attr", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    


    def test_existence_of_endpoint_entity_operations_create(self):

        r = requests.post(batch_operations_url + "create", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
        

    def test_existence_of_endpoint_entity_operations_upsert(self):

        r = requests.post(batch_operations_url + "upsert", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    

    def test_existence_of_endpoint_entity_operations_update(self):

        r = requests.post(batch_operations_url + "update", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    
    
    def test_existence_of_endpoint_entity_operations_delete(self):

        r = requests.post(batch_operations_url + "delete", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    

    def test_existence_of_endpoint_temporal_entities(self):

        r = requests.post(temporal_entities_url, auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    

        r = requests.get(temporal_entities_url + "test", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    

        r = requests.delete(temporal_entities_url + "test", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    


    def test_existence_of_endpoint_temporal_entities_entityid(self):

        r = requests.get(temporal_entities_url + "test", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    

        r = requests.delete(temporal_entities_url + "test", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    

    def test_existence_of_endpoint_temporal_entities_entityid_attr(self):

        r = requests.post(temporal_entities_url + "test/attrs/", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    

    def test_existence_of_endpoint_temporal_entities_entityid_attr_attrid(self):

        r = requests.delete(temporal_entities_url + "test/attrs/test_attr", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)
    

    def test_existence_of_endpoint_temporal_entities_entityid_attr_attrid_instanceid(self):

        r = requests.patch(temporal_entities_url + "test/attrs/test_attr/instance1", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)

    
        r = requests.delete(temporal_entities_url + "test/attrs/test_attr/instance1", auth=(username, password))        
        self.assertNotEqual(r.status_code, 500)
        self.assertNotEqual(r.status_code, 405)

    
 