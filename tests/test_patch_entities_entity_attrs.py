# -*- coding: utf-8 -*-

import requests
import sys
import os
import unittest
import urllib.parse

ngsiBaseUrl = os.environ["NGSI_ENDPOINT"]
username = os.environ["USERNAME"]
password = os.environ["PASSWORD"]

entitiesUrl = ngsiBaseUrl + "ngsi-ld/v1/entities/"


class TestPatchEntitiesEntityAttrs(unittest.TestCase):

    def setUp(self):

        self.initial_property_value = "Here since the beginning"

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)

        # Create initial entity:
        payload = {
            "type": "Test", 
            "id": "test", 
            "@context": [],

            "existing_property": {
                "type": "Property", 
                "value": self.initial_property_value,
                "datasetId" : "dataset1"
                
            },
            "existing_property#2": {
                "type": "Property", 
                "value": self.initial_property_value,
                "datasetId" : "dataset2"
            }            
        }

        # POST entity:
        r = requests.post(entitiesUrl, json=payload, auth=(username, password))
        self.assertEqual(r.status_code, 201)

        # Check whether entity is present:
        r = requests.get(entitiesUrl, auth=(username, password))
        self.assertEqual(len(r.json()), 1)



    def test_update_property_no_datasetid(self):

        property_name = "existing_property"

        payload_append = {property_name: {"type" : "Property", "value": "patched"}}

        r = requests.post(entitiesUrl + "test/attrs/", json=payload_append, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # Fetch entity to check whether the new property is appended:
        r = requests.get(entitiesUrl + "test", auth=(username, password))

        entity = r.json()

        self.assertEqual(property_name in r.json(), True)
        self.assertEqual(entity[property_name]['value'], 'patched')



    def test_update_property_with_valid_datasetid(self):

        property_name = "existing_property"

        payload_append = {property_name: {"type": "Property", "value": "patched", "datasetId" : "dataset1"}}

        r = requests.patch(entitiesUrl + "test/attrs/", json=payload_append, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # Fetch entity to check whether the new property is appended:
        r = requests.get(entitiesUrl + "test", auth=(username, password))

        entity = r.json()

        print(entity)

        self.assertEqual(property_name in r.json(), True)
        self.assertEqual(entity[property_name]['value'], "patched")



    def test_update_property_with_invalid_datasetid(self):

        property_name = "existing_property"

        payload_append = {property_name: {"type": "Property", "value": "patched", "datasetId" : "this_dataset_id_does_not_exist"}}

        r = requests.patch(entitiesUrl + "test/attrs/", json=payload_append, auth=(username, password))

        # Since not all attributes are updated (actually, none), status code should be 207.
        self.assertEqual(r.status_code, 207)

        #print(r.json())

        # Fetch entity to check whether the new property is appended:
        r = requests.get(entitiesUrl + "test", auth=(username, password))

        entity = r.json()

        self.assertEqual(property_name in r.json(), True)
        self.assertEqual(entity[property_name]['value'], self.initial_property_value)


