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

class TestGetDatetime(unittest.TestCase):

    def setUp(self):

        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)

   


class DateTimeTest(unittest.TestCase):

    def setUp(self):
        # DELETE all entities:
        r = requests.delete(entitiesUrl, auth=(username, password))
        self.assertEqual(r.status_code, 204)

        # GET all entities ...
        r = requests.get(entitiesUrl, auth=(username, password))
        # ... should return an empty list:
        self.assertEqual(len(r.json()), 0)

        # How a DateTime property is supposed to look:
        # https://fiware-datamodels.readthedocs.io/en/latest/ngsi-ld_faq/index.html#q-how-datetime-is-represented-eg-timestamps-dates-time

        # Create DateTime entity:
        entity = {
            "id": "test_datetime1",
            "type": "TestDateTime",
            "@context": [],

            "date": {
                "type": "Property",
                "value": {
                    "@type" : "DateTime",
                    "@value": "2007-08-31T16:47+00:00"
                } 
            }

        }

        r = requests.post(entitiesUrl, json=entity, auth=(username, password))
        self.assertEqual(r.status_code, 201)

    def test_datetime(self):

        r = requests.get(entitiesUrl + "?q=" + urllib.parse.quote("date[@value]<2010-08-31T16:47+00:00"), auth=(username, password))
        self.assertEqual(len(r.json()), 1)

        r = requests.get(entitiesUrl + "?q=" + urllib.parse.quote("date[@value]>2010-08-31T16:47+00:00"), auth=(username, password))
        self.assertEqual(len(r.json()), 0)

        r = requests.get(entitiesUrl + "?q=" + urllib.parse.quote("date[@value]==2007-08-31T16:47+00:00"), auth=(username, password))
        self.assertEqual(len(r.json()), 1)
