# -*- coding: utf-8 -*-

import requests, sys, os
import unittest
import urllib.parse

ngsiBaseUrl = os.environ["NGSI_ENDPOINT"]
username = os.environ["USERNAME"]
password = os.environ["PASSWORD"]

entitiesUrl = ngsiBaseUrl + "ngsi-ld/v1/entities/"

class TestPost(unittest.TestCase):

    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)


    
    def test_post_entity(self):

        entity = {
            "id": "test1",
            "type": "Test",
            "@context": {},
        }

        r = requests.post(entitiesUrl, json=entity, auth=(username, password))

        self.assertEqual(r.status_code, 201)

        # GET entity by id:
        r = requests.get(entitiesUrl + "test1", auth=(username, password))

        # Returned entity should have id 'test1':
        self.assertEqual(r.json()['id'], 'test1')

        # GET all entities:
        r = requests.get(entitiesUrl, auth=(username, password))

        # Exactly one entity (the one we have just created) should exist now:
        self.assertEqual(len(r.json()), 1)
