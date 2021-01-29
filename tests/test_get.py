# -*- coding: utf-8 -*-

# This is Python 3

import requests
import os
import unittest
import urllib.parse


ngsiBaseUrl = os.environ["NGSI_ENDPOINT"]

entitiesUrl = ngsiBaseUrl + "ngsi-ld/v1/entities/"
username = os.environ["USERNAME"]
password = os.environ["PASSWORD"]


class TestGet(unittest.TestCase):

    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)

    def test_delete_all_entities(self):

        # Create an entity:
        entity = {
            "@context": [],
            "id": "test1",
            "type": "Test"
        }

        r = requests.post(entitiesUrl, json=entity)

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities:
        r = requests.get(entitiesUrl, auth=(username, password))

        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)



class RelationshipsTest(unittest.TestCase):

    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)

        # Create Contact entity:
        entity_contact = {
            "id": "contact1",
            "type": "Contact",
            "@context": [],

            "firstname": {
                "type": "Property",
                "value": "Sebastian",
            },

            "age": {
                "type": "Property",
                "value":  40
            },

          

            "location": {
                "type": "GeoProperty",
                "value": {
                    "type": "Point",
                    "coordinates": [8.7, 49.6]
                }
            }
        }

        r = requests.post(entitiesUrl, json=entity_contact, auth=(username, password))
        self.assertEqual(r.status_code, 201)

        # Create Contact entity:
        entity_car = {
            "id": "car1",
            "type": "Car",
            "@context": [],

            "brand": {
                "type": "Property",
                "value": "Mercedes"
                
            },

            "owner": {
                "type": "Relationship",
                "object": "contact1"
            }

        }

        r = requests.post(entitiesUrl, json=entity_car, auth=(username, password))
        self.assertEqual(r.status_code, 201)

    '''
    def test_relationship(self):

        r = requests.get(entitiesUrl + "?q=brand==Mercedes", auth=(username, password))
        self.assertEqual(len(r.json()), 1)

        r = requests.get(
            entitiesUrl + "?q=brand.industry==cars;owner.firstname==Sebastian;owner.stuff[age][now]==40", auth=(username, password))

        self.assertEqual(len(r.json()), 1)
    '''
    

class PropertiesTest(unittest.TestCase):

    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)

        ################# BEGIN Create entity1 ###############
        entity1 = {
            "id": "test1",
            "type": "Test",
            "@context": [],

            "address": {
                "type": "Property",
                "value":  {
                    "@type": "PostalAddress",
                    "@value": {
                        "houseNumber": "22/10",
                        "town": "Heidelberg"
                    }
                }

            },

            "trailTest": {
                "type": "Property",
                "value": "works"
            },

            "trailTest2": {
                "type": "Property",
                "value": "works_too"

            }
        }

        r = requests.post(entitiesUrl, json=entity1, auth=(username, password))
        self.assertEqual(r.status_code, 201)

        ################# END Create entity1 ###############

        ################# BEGIN Create entity1 ###############
        entity2 = {
            "id": "test2",
            "type": "Test",
            "@context": [],

            "exclusiveProperty": {"type": "Property", "value": "onlyme"}
        }

        r = requests.post(entitiesUrl, json=entity2, auth=(username, password))
        self.assertEqual(r.status_code, 201)

        ################# END Create entity1 ###############

    '''
    def test_get_by_property_value(self):

        r = requests.get(entitiesUrl + "?q=address.street.category==Autobahn", auth=(username, password))
        self.assertEqual(len(r.json()), 1)

        self.assertEqual(r.json()[0]['id'], 'test1')
    '''
    '''
    def test_get_by_trailing_path(self):

        r = requests.get(entitiesUrl + "?q=trailTest[data]==works", auth=(username, password))
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test1')
        self.assertEqual(r.json()[0]['trailTest']['data'], 'works')

        r = requests.get(entitiesUrl + "?q=trailTest2[data][entry]==works_too", auth=(username, password))
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test1')
        self.assertEqual(r.json()[0]['trailTest2']['data']['entry'], 'works_too')
    '''

    def test_get_by_property_existence(self):

        # This query should only return all entities that have the property "exclusiveProperty".
        # The property value does not matter, but entities without the property should not be returned.

        # NGSI-LD spec 4.9, page 35:
        # "When a Query Term only defines an attribute path (production rule named Attribute), the matching Entities shall be
        # those which define the target element (Property or a Relationship), regardless of any target value or object."

        r = requests.get(entitiesUrl + "?q=exclusiveProperty", auth=(username, password))
        self.assertEqual(len(r.json()), 1)

        self.assertEqual(r.json()[0]['id'], 'test2')


class AttrsTest(unittest.TestCase):

    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)

        # Create Contact entity:
        entity = {
            "id": "test1",
            "type": "Place",
            "@context": [],

            "name": {
                "type": "Property",
                "value": "Reutlingen"
            }
        }

        r = requests.post(entitiesUrl, json=entity, auth=(username, password))
        self.assertEqual(r.status_code, 201)

        # Create Contact entity:
        entity = {
            "id": "test2",
            "type": "Place",
            "@context": [],

            "name": {
                "type": "Property",
                "value": "Heidelberg"
            }
        }

        r = requests.post(entitiesUrl, json=entity, auth=(username, password))
        self.assertEqual(r.status_code, 201)

    def test_attrs_request(self):

        r = requests.get(entitiesUrl + "?attrs=name", auth=(username, password))

        entities = r.json()

        self.assertEqual(len(entities), 2)

        self.assertEqual(entities[0]['name']['value'], "Reutlingen")
        self.assertEqual(entities[1]['name']['value'], "Heidelberg")
