# -*- coding: utf-8 -*-

import requests, sys, os
import unittest
import urllib.parse

ngsiBaseUrl = os.environ["NGSI_ENDPOINT"]
username = os.environ["USERNAME"]
password = os.environ["PASSWORD"]

entitiesUrl = ngsiBaseUrl + "ngsi-ld/v1/entities/"

class TestPut(unittest.TestCase):

    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)

    def test_put_entity(self):

        entityUrl = entitiesUrl + "test1"

        #################### BEGIN Create entity #################
        # Create an entity:
        entity = {
            "@context": {},
            "id": "test1",
            "type": "Test",
            "name": "Peter"
        }

        r = requests.put(entityUrl, json=entity, auth=(username, password))
        
        self.assertEqual(r.status_code, 201)

        r = requests.get(entityUrl, auth=(username, password))

        self.assertEqual(r.json()['id'], 'test1')
        self.assertEqual(r.json()['name'], 'Peter')
        #################### END Create entity #################

        #################### BEGIN Replace entity and change name #################
        # Create an entity:
        entity = {
            "@context": {},
            "id": "test1",
            "type": "Test",
            "name": "Dieter"
        }

        # PUT Entity:
        r = requests.put(entityUrl, json=entity, auth=(username, password))

        self.assertEqual(r.status_code, 201)

        r = requests.get(entityUrl, auth=(username, password))

        self.assertEqual(r.json()['id'], 'test1')
        self.assertEqual(r.json()['name'], 'Dieter')
        #################### END Replace entity and change name #################

 