# -*- coding: utf-8 -*-

import requests, sys, os
import unittest
import urllib.parse

ngsiBaseUrl = os.environ["NGSI_ENDPOINT"]
username = os.environ["USERNAME"]
password = os.environ["PASSWORD"]

entitiesUrl = ngsiBaseUrl + "ngsi-ld/v1/entities/"
operationsUrl = ngsiBaseUrl + "ngsi-ld/v1/entityOperations/"

class TestEntityOperations(unittest.TestCase):

    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)


    def test_create_invalid(self):

        #################### BEGIN Create entities #################
        # Create payload:
        payload = "this is not valid json"

        # POST entities:
        r = requests.post(operationsUrl + "create", json=payload, auth=(username, password))        
        self.assertEqual(r.status_code, 400)

        # Check whether there are really two entities now:
        r = requests.get(entitiesUrl, auth=(username, password))
        self.assertEqual(len(r.json()), 0)
        #################### END Create entities #################


    def test_create(self):

        #################### BEGIN Create entities #################
        # Create payload:
        payload = [{
            "@context": [],
            "id": "test1",
            "type": "Test",
            "name": {"type" : "Property", "value": "Peter"}
        },
        {
            "@context": [],
            "id": "test2",
            "type": "Test",
            "name": {"type" : "Property", "value": "Paul"}
        }]

        # POST entities:
        r = requests.post(operationsUrl + "create", json=payload, auth=(username, password))        
        self.assertEqual(r.status_code, 200)

        # Check whether there are really two entities now:
        r = requests.get(entitiesUrl, auth=(username, password))

       
        self.assertEqual(len(r.json()), 2)
        #################### END Create entities #################


    def test_delete(self):

       
        #################### BEGIN Create entities #################
        # Create payload:
        payload = [{
            "@context": [],
            "id": "test1",
            "type": "Test",
            "name": {"type" : "Property", "value": "Peter"}
        },
        {
            "@context": [],
            "id": "test2",
            "type": "Test",
            "name": {"type" : "Property", "value": "Paul"}
        }]

        # POST entities:
        r = requests.post(operationsUrl + "create", json=payload, auth=(username, password))        
        self.assertEqual(r.status_code, 200)

        # Check whether there are really two entities now:
        r = requests.get(entitiesUrl, auth=(username, password))
        self.assertEqual(len(r.json()), 2)
        #################### END Create entities #################

        # Delete entities using batch operation:
        r = requests.post(operationsUrl + "delete", json = ['test1', 'test2'], auth=(username, password))
        self.assertEqual(r.status_code, 200)
        
        # Check whether all entities are deletete:
        r = requests.get(entitiesUrl, auth=(username, password))
        self.assertEqual(len(r.json()), 0)
        


    def test_upsert(self):

        upsertUrl = operationsUrl + "upsert"

        #################### BEGIN Create entities #################
        # Create payload:
        payload = [{
            "@context": [],
            "id": "test1",
            "type": "Test",
            "name": {"type" : "Property", "value": "Peter"}
        },
        {
            "@context": [],
            "id": "test2",
            "type": "Test",
            "name": {"type" : "Property", "value": "Paul"}
        }]

        # POST entities:
        r = requests.post(upsertUrl, json=payload, auth=(username, password))        
        self.assertEqual(r.status_code, 200)

        # Check whether there are really two entities now:
        r = requests.get(entitiesUrl, auth=(username, password))
        self.assertEqual(len(r.json()), 2)
        #################### END Create entities #################


         #################### BEGIN Upsert entities #################
        # Create payload:
        payload = [{
            "@context": [],
            "id": "test1",
            "type": "Test",
            "name": {"type" : "Property", "value": "Mary"}
        }]

        # POST upsert of existing entity with id 'test1':
        r = requests.post(upsertUrl, json=payload, auth=(username, password))
        
        self.assertEqual(r.status_code, 200)

        r = requests.get(entitiesUrl + "test1", auth=(username, password))

        entity = r.json()

        self.assertEqual(entity['name']['value'], 'Mary')

        #################### END Upsert entities #################
       

 