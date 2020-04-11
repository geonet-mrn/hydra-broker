# Hydra - An Ultra-Lightweight NGSI-LD Context Broker

Hydra ist an ultra-lightweight **NGSI-LD context broker** written in Python. It uses Flask as its HTTP interface and PostgreSQL+PostGIS as database.

## Overview

NGSI-LD is a [JSON-LD](https://json-ld.org/)-based RESTful API standard for the development of web services and distributed applications with a focus on "Internet of Things" (IoT), "right-time" sensor data and "smart cities". NGSI-LD is developed by the [FIWARE Foundation](https://www.fiware.org) and formally standardized by the [European Telecommunications Standards Institute (ETSI)](https://www.etsi.org/). On the ETSI website, you can find [the official specification document for the latest version of NGSI-LD](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf).

## A Note About Current Limitations
Please not that Hydra is still in a **very early stage of development**. It does currently not implement the full NGSI-LD specification. However, it does implement the most important parts of it, including support for spatial (NGSI-LD Geo-query language) and temporal (NGSI-LD Temporal Query Language) queries.

Also, Hydra's query code is not yet as optimized as it could be, since many query filter checks are not performed on the SQL level (i.e. directly in PostgreSQL). Instead, Hydra first fetches a superset of the actual result set through SQL and afterwards sorts out non-matching entities through checks in Python code. This can lead to sub-optimal query performance when lots of entities are returned.

On the pro side, Hydra can be get up and running quick and easy, and its source code is short and easy to understand, modify and extend. It can be understood as an "educational implementation" of an NGSI-LD broker.

## Project Context and Funding

Hydra is developed by [GeoNet.MRN e.V.](http://www.geonet-mrn.de) as part of *xDataToGO*, a cooperative research project funded by the Germany Ministry of Transport and Digital Infrastructure (BMVI) as part of the [mFund initiative](https://www.bmvi.de/EN/Topics/Digital-Matters/mFund/mFund.html). Goal of the xDataToGo project is to develop new methods and digital solutions to find, collect and make available data about the public street space, specifically to improve and simplify planning of large-volume and heavy goods transport.

## Installation

### 1. General Requirements and Assumptions

Make sure that your system fulfills the following requirements:

- Python 3 is installed. Hydra is a Python 3 program and does not run with older Python versions.
- Pip for Python 3 (pip3) is installed.
- You have a running PostgreSQL database server with the PostGIS extension installed.

So far, Hydra was only tested in a Linux environment (Ubuntu Linux 18.04). While it is likely that Hydra runs under other operating systems, including Microsoft Windows, as well, we can currently not provide any instructions/support for this. 

All following instructions apply to a Linux environment.

### 2. Installing Other Dependencies Using Pip3

Hydra comes with a *requirements.txt* file. You can automatically install all dependencies by switching your work directory to Hydra's installation folder and running the following command line:

```
pip3 install -r requirements.txt
```

### 2. Setting up the PostgreSQL/PostGIS Database
An SQL dump and/or Python script to set up Hydra's PostgreSQL/PostGIS database schema will be added soon!

#### 2.1 Creating a Database for Hydra
Connect to your PostgreSQL server with the database administration tool of your choice and create a database for Hydra. It is also recommended that you create a new PostgreSQL user account who owns this database and does not have privileges on other databases.

#### 2.2 Editing the Hydra Configuration File
Open the file *hydraconfig.json* in the Hydra installation folder with a text editor and enter proper values for the different parameters. You *must* edit the values for *db_host*, *db_user*, *db_password* and *db_name* so that they match your environment. Editing the other parameters is optional.

#### 2.3 Finalizing Database Setup
The last things that are still missing now are the database tables and the PostGIS extension on the database. These things can be set up automatically by the script *setup.py* in the Hydra installation directory. Switch your command line working 
directory to your Hydra installation folder and run the following command:

```
python3 setup.py
```
The script reads the settings from the *hydraconfig.json* file and sets up everything accordingly. Now your Hydra server is ready to run.


### 3. Running Hydra

#### 3.1 Starting Hydra Manually
You can start Hydra by running the included shell script *start.sh*.

#### 3.2 Running Hydra as a System Service

For production use, we recommend to set up Hydra as a system service, e.g. using *systemd* under Linux. You can find instructions for this [here](https://www.raspberrypi-spy.co.uk/2015/10/how-to-autorun-a-python-script-on-boot-using-systemd/), or just google something like "systemd python script".

## Unit Tests

The Hydra repository contains a set of PyUnit unit tests to verify Hydra's conformity with the NGSI-LD standard. Just as Hydra's NGSI-LD implementation itself, the unit tests are currently **not complete**.

In order to run the unit tests, you need a running Hydra instance. In the shell script "run-tests.sh", change the environment variable *NGSI_ENDPOINT* to the endpoint URL of the Hydra instance you want to test. Note that the base URL *must* end with a '/'! The preconfigured default value is "http://localhost:5000/". 

### ATTENTION:

**These unit tests will COMPETELY CLEAR YOUR HYDRA DATABASE each time you run them!**
**DO NOT run the unit tests on a production Hydra instance that holds important data!**

To run the unit tests, start the script *run-tests.sh*.
