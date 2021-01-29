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


class TemporalQueryTest(unittest.TestCase):

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

            "observedAt": {
                "type": "Property",
                "value": "2020-04-10T16:47:00Z"
                }
            }

        

        r = requests.post(entitiesUrl, json=entity, auth=(username, password))
        self.assertEqual(r.status_code, 201)


    def test_temporal_query(self):

        # 'timerel' argument present, but missing 'time'. Should return status code 400:
        r = requests.get(entitiesUrl + "?timerel=before", auth=(username, password))
        self.assertEqual(r.status_code, 400)
        

        # Nothing should be found:
        r = requests.get(entitiesUrl + "?timerel=before&time=" + urllib.parse.quote("2010-08-31T00:00:00Z"), auth=(username, password))
        
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()), 0)
        
        # The created test entity should be found since it fits the passed time frame:
        r = requests.get(entitiesUrl + "?timerel=after&time=" + urllib.parse.quote("2010-08-31T00:00:00Z"), auth=(username, password))
        
        print(r.json())

        self.assertEqual(r.status_code, 200)        
        self.assertEqual(len(r.json()), 1)

        # The created test entity should be found since it fits the passed time frame:
        r = requests.get(entitiesUrl + "?timerel=between&time=" + urllib.parse.quote("2010-08-31T00:00:00Z") + "&endtime=" + urllib.parse.quote("2022-08-31T00:00:00Z"), auth=(username, password))
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()), 1)

        # Nothing should exist in this time period:
        r = requests.get(entitiesUrl + "?timerel=between&time=" + urllib.parse.quote("2022-08-31T00:00:00Z") + "&endtime=" + urllib.parse.quote("2025-08-31T00:00:00Z"), auth=(username, password))
        self.assertEqual(len(r.json()), 0)
        