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


class TestPostEntitiesEntityAttrs(unittest.TestCase):

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


    def test_append_new_property(self):

        # Append a not yet existing property:

        property_name = "new_property"

        payload_append = {property_name: "hello world"}

        r = requests.post(entitiesUrl + "test/attrs/", json=payload_append, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # Fetch entity to check whether the new property is appended:
        r = requests.get(entitiesUrl + "test", auth=(username, password))

        entity = r.json()

        self.assertEqual(property_name in r.json(), True)
        self.assertEqual(entity[property_name], 'hello world')


    def test_overwrite_property_no_datasetid_overwrite(self):

        # Try to overwrite existing property with permission, no datasetId present:

        property_name = "existing_property"

        payload_append = {property_name: {"type": "Property", "value": "overwritten"}}

        r = requests.post(entitiesUrl + "test/attrs/", json=payload_append, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # Fetch entity to check whether the new property is appended:
        r = requests.get(entitiesUrl + "test", auth=(username, password))

        entity = r.json()

        self.assertEqual(property_name in r.json(), True)
        self.assertEqual(entity[property_name]['value'], "overwritten")


    def test_overwrite_property_no_datasetid_no_overwrite(self):
        # Try to overwrite existing property without permission, no datasetId present:

        property_name = "existing_property"

        payload_append = {property_name: {"type": "Property", "value": "overwritten"}}

        r = requests.post(entitiesUrl + "test/attrs/?options=noOverwrite", json=payload_append, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # Fetch entity to check whether the new property is appended:
        r = requests.get(entitiesUrl + "test", auth=(username, password))

        entity = r.json()

        # Since overwriting was disabled, the property should still have its initial value:
        self.assertEqual(property_name in r.json(), True)
        self.assertEqual(entity[property_name]['value'], self.initial_property_value)


    def test_overwrite_property_with_datasetid_overwrite(self):

        # Try to overwrite existing property without permission, datasetId present:

        property_name = "existing_property"

        payload_append = {property_name: {"type": "Property", "value": "overwritten", "datasetId" : "dataset2"}}

        r = requests.post(entitiesUrl + "test/attrs/", json=payload_append, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # Fetch entity to check whether the new property is appended:
        r = requests.get(entitiesUrl + "test", auth=(username, password))

        entity = r.json()

        print(entity)

        # Since overwriting was disabled, the property should still have its initial value:
        self.assertEqual(property_name in r.json(), True)
        self.assertEqual(entity[property_name]['value'], self.initial_property_value)
