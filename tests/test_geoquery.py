# -*- coding: utf-8 -*-

# This is Python 3

import requests, sys,os
import unittest
import urllib.parse

ngsiBaseUrl = os.environ["NGSI_ENDPOINT"]
username = os.environ["USERNAME"]
password = os.environ["PASSWORD"]

entitiesUrl = ngsiBaseUrl + "ngsi-ld/v1/entities/"

class GeometryTest(unittest.TestCase):

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
            "id": "test_point",
            "type": "TestPoint",
            "@context": {},

            "location": {
                "type": "GeoProperty",
                "value": {
                    "type": "Point",
                    "coordinates": [2, 2]
                }
            }

        }

        r = requests.post(entitiesUrl, json=entity, auth=(username, password))
        self.assertEqual(r.status_code, 201)

        
        # Create another Point entity:
        entity = {
            "id": "test_point2",
            "type": "TestPoint",
            "@context": {},

            "location": {
                "type": "GeoProperty",
                "value": {
                    "type": "Point",
                    "coordinates": [50, 50]
                }
            }

        }

        r = requests.post(entitiesUrl, json=entity, auth=(username, password))
        self.assertEqual(r.status_code, 201)

        # Create MultiPolygon entity:
        entity = {
            "id": "test_multipolygon",
            "type": "TestMultiPolygon",
            "@context": {},

            "location": {
                "type": "GeoProperty",
                "value": {
                    "type": "MultiPolygon",
                    "coordinates": [[[[1, 1], [1, 4], [4, 4], [4, 1], [1, 1]]]]
                }
            }
        }

        r = requests.post(entitiesUrl, json=entity, auth=(username, password))
        self.assertEqual(r.status_code, 201)


    def test_geo_point_near_point(self):

        ####################### BEGIN Test exact coordinates match #########################
        georel = 'near;maxDistance==0'
        geoproperty = 'location'
        geometry = 'Point'
        coordinates = '[2,2]'

        url = entitiesUrl + "?type=TestPoint&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)

        r = requests.get(url, auth=(username, password))

        # Exactly one entity (the one we have just created) should exist now:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_point')
        ####################### END Test exact coordinates match #########################


        ####################### BEGIN Test maxDistance outside with distance (no match) #########################
        # According to Google Earth, the distance between lon/lat [2,2] and [3,3] is approx. 157 km
        georel = 'near;maxDistance==156000'
        geoproperty = 'location'
        geometry = 'Point'
        coordinates = '[3,3]'

        url = entitiesUrl + "?type=TestPoint&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)

        r = requests.get(url, auth=(username, password))

        # No entity should be returned:
        self.assertEqual(len(r.json()), 0)
        
        ####################### END Test maxDistance outside with distance (no match)  #########################

        ####################### BEGIN Test maxDistance inside with distance (match) #########################
        # According to Google Earth, the distance between lon/lat [2,2] and [3,3] is approx. 157 km
        georel = 'near;maxDistance==158000'
        geoproperty = 'location'
        geometry = 'Point'
        coordinates = '[3,3]'

        url = entitiesUrl + "?type=TestPoint&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)

        r = requests.get(url, auth=(username, password))

        # 1 entity should be returned:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_point')
        
        ####################### END Test maxDistance inside with distance (no match)  #########################

        
        ####################### BEGIN Test minDistance outside with distance (match) #########################
        # According to Google Earth, the distance between lon/lat [2,2] and [3,3] is approx. 157 km
        georel = 'near;minDistance==156000'
        geoproperty = 'location'
        geometry = 'Point'
        coordinates = '[3,3]'

        url = entitiesUrl + "?type=TestPoint&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)

        r = requests.get(url, auth=(username, password))

        # 2 entities should be returned:
        self.assertEqual(len(r.json()), 2)        
        ####################### END Test minDistance outside with distance (match)  #########################
 
        ####################### BEGIN Test minDistance inside with distance (no match) #########################
        # According to Google Earth, the distance between lon/lat [2,2] and [3,3] is approx. 157 km
        georel = 'near;minDistance==157000'
        geoproperty = 'location'
        geometry = 'Point'
        coordinates = '[3,3]'

        url = entitiesUrl + "?type=TestPoint&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)

        r = requests.get(url, auth=(username, password))

        # 1 entity should be returned (the point at (50,50)):        
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_point2')
        
        ####################### END Test minDistance inside with distance (no match)  #########################

    
    def test_geo_point_near_line(self):

        ####################### BEGIN Test exact coordinates match #########################
        georel = 'near;maxDistance==100000'
        geoproperty = 'location'
        geometry = 'LineString'
        coordinates = '[[2, 2.1], [3, 2.1]]'

        url = entitiesUrl + "?type=TestPoint&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)

        r = requests.get(url, auth=(username, password))

        
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_point')
        
        ####################### END Test exact coordinates match #########################


    def test_geo_point_within_multipolygon(self):

        georel = 'within'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[1,1],[1,3],[3,3],[3,1],[1,1]]]]'

        url = entitiesUrl + "?type=TestPoint&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)

        r = requests.get(url, auth=(username, password))

        # Exactly one entity (the one we have just created) should exist now:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_point')


    def test_geo_multipolygon_within_multipolygon(self):

        georel = 'within'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[0,0],[5,0],[5,5],[0,5],[0,0]]]]'

        url = entitiesUrl + "?type=TestMultiPolygon&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)
        
        r = requests.get(url, auth=(username, password))

        # Exactly one entity (the one we have just created) should exist now:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_multipolygon')


    def test_geo_multipolygon_contains_multipolygon(self):

        georel = 'contains'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[2,2],[3,2],[3,3],[2,3],[2,2]]]]'

        url = entitiesUrl + "?type=TestMultiPolygon&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)
        
        r = requests.get(url, auth=(username, password))

        # Exactly one entity (the one we have just created) should exist now:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_multipolygon')

    
    def test_geo_point_intersects_multipolygon(self):

        georel = 'intersects'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[1,1],[1,3],[3,3],[3,1],[1,1]]]]'

        url = entitiesUrl + "?type=TestPoint&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)

        r = requests.get(url, auth=(username, password))

        # Exactly one entity (the one we have just created) should exist now:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_point')


    def test_geo_point_not_intersects_multipolygon(self):

        # Without geo query, one entity should be returned:
        r = requests.get(entitiesUrl + "?type=TestPoint", auth=(username, password))
        self.assertEqual(len(r.json()), 2)

        # GET all entities with geometry query:

        georel = 'intersects'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[10,1],[10,3],[30,3],[30,1],[10,1]]]]'

        url = entitiesUrl + "?georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)

        r = requests.get(url, auth=(username, password))

        # No entity should be returned since the geometries don't intersect:
        self.assertEqual(len(r.json()), 0)


    def test_geo_multipolygon_intersects_multipolygon(self):

        georel = 'intersects'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[0,0],[0,2],[2,2],[2,0],[0,0]]]]'

        url = entitiesUrl + "?type=TestMultiPolygon&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)
        
        r = requests.get(url, auth=(username, password))

        # Exactly one entity (the one we have just created) should exist now:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_multipolygon')


    def test_geo_multipolygon_not_intersects_multipolygon(self):

        georel = 'intersects'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[10,10],[10,12],[12,12],[12,10],[10,10]]]]'

        url = entitiesUrl + "?georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)
        
        r = requests.get(url, auth=(username, password))

        # No entity should be returned:
        self.assertEqual(len(r.json()), 0)
        

    def test_geo_multipolygon_equals_multipolygon(self):

        georel = 'equals'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[1, 1], [1, 4], [4, 4], [4, 1], [1, 1]]]]'

        url = entitiesUrl + "?georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)
        
        r = requests.get(url, auth=(username, password))

        # 1 entity should be returned:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_multipolygon')

    
    def test_geo_point_equals_point(self):

        georel = 'equals'
        geoproperty = 'location'
        geometry = 'Point'
        coordinates = '[2,2]'

        url = entitiesUrl + "?georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)
        
        r = requests.get(url, auth=(username, password))

        # 1 entity should be returned:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_point')



    # TODO: 2 Understand why the disjoint test does not return the expected result.
    '''    
    def test_geo_multipolygon_disjoint_multipolygon(self):

        georel = 'disjoint'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[10, 10], [10, 11], [11, 11], [11, 10], [10, 10]]]]'

        url = entitiesUrl + "?type=MultiPolygon&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)
        
        r = requests.get(url)

        # 1 entity should be returned:
        self.assertEqual(len(r.json()), 1)
        #self.assertEqual(r.json()[0]['id'], 'test_multipolygon')
    '''


    def test_geo_multipolygon_overlaps_multipolygon(self):

        georel = 'overlaps'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[0,0],[0,2],[2,2],[2,0],[0,0]]]]'

        url = entitiesUrl + "?type=TestMultiPolygon&georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)
        
        r = requests.get(url, auth=(username, password))

        # Exactly one entity (the one we have just created) should exist now:
        self.assertEqual(len(r.json()), 1)
        self.assertEqual(r.json()[0]['id'], 'test_multipolygon')


    def test_geo_multipolygon_not_overlaps_multipolygon(self):

        georel = 'overlaps'
        geoproperty = 'location'
        geometry = 'MultiPolygon'
        coordinates = '[[[[10,10],[10,12],[12,12],[12,10],[10,10]]]]'

        url = entitiesUrl + "?georel=%s&geoproperty=%s&geometry=%s&coordinates=%s" % (
            georel, geoproperty, geometry, coordinates)
        
        r = requests.get(url, auth=(username, password))

        # No entity should be returned:
        self.assertEqual(len(r.json()), 0)