#!/usr/bin/env python3 

from cloudant import CouchDB
from datetime import datetime
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import time

# Random for testing
import decimal
import random

def genRandomDecimal(li,hi,d):
    return decimal.Decimal('%d.%d' % (random.randint(li,hi),random.randint(0,d)))

# Load secrets
load_dotenv()

neutrino_id = os.getenv('NEUTRINO_ID')
token = os.getenv('TOKEN')
org = os.getenv('ORG')
bucket = os.getenv('BUCKET')
neutron = os.getenv('NEUTRON')
cdb_url = os.getenv('CDB_URL')
cdb_user = os.getenv('CDB_USER')
cdb_pass = os.getenv('CDB_PASS')

# Setup DB connection strings

client = InfluxDBClient(url=neutron, token=token)
cdb = CouchDB(cdb_user, cdb_pass, url=cdb_url, connect=True, auto_renew=True)
cdb_neutrinos = cdb['neutrinos']

write_api = client.write_api(write_options=SYNCHRONOUS)

# Collect reading from sensors

# Prepare reading to push
def prepareReading(fr1,fr2,neutrino_id,neutrino_name):
    point = Point("reading")\
        .tag("neutrino_id", neutrino_id)\
        .tag("neutrino_name", neutrino_name)\
        .field("fake_reading", fr1)\
        .field("fake_reading2", fr2)\
        .time(datetime.utcnow(), WritePrecision.NS)
    return point

# Try to write reading
def writeReading(bucket,org,point):
    while True:
        try:
            write_api.write(bucket=bucket, org=org, record=point)
            print("wrote new readings")
        except Exception as e:
            print("Cannot connect to {} with error {}".format(neutron,e))
            continue
        break

def checkNeutrinoReg(location,customer,neutrino_id):
    doc_exists = neutrino_id in cdb_neutrinos
    if doc_exists:
        neutrino_location_name = cdb_neutrinos[neutrino_id]['location_name']
        neutrino_customer = cdb_neutrinos[neutrino_id]['customer']
        print(neutrino_location_name)
        print(neutrino_customer)

        if neutrino_location_name == "":
            print("No location name assigned to {}".format(neutrino_id))
        else:
            location = "true"

        if neutrino_customer == "":
            print("No customer assigned to {}".format(neutrino_id))
        elif neutrino_customer == "default":
            print("Default customer assigned to {}".format(neutrino_id))
        else:
            customer = "true"
        
    else:
        newNeutrino = {"_id": neutrino_id, "location_name": "", "customer": ""}
        cdb_neutrinos.create_document(newNeutrino)
        customer = "false"
        location = "false"
    return customer,location

def getNeutrinoReg(neutrino_id):
    with cdb_neutrinos[neutrino_id] as doc:
        neutrino_location_name = doc['location_name']
        neutrino_customer = doc['customer']
        print("Retrieved neutrino location name: {}".format(neutrino_location_name))
        return neutrino_location_name,neutrino_customer

# MAIN APP
def main():
    location = "false"
    customer = "false"
    location,customer = checkNeutrinoReg(location,customer,neutrino_id)
    print("Before if statement in main: {}".format(location))
    if location == "true" and customer == "true":
        neutrino_location_name,bucket = getNeutrinoReg(neutrino_id)
        print("if location true in main")
        print(neutrino_location_name)
        print(bucket)
    else: 
        while location != "true" or customer != "true" :
            print("loop if customer or location is not true")
            location,customer = checkNeutrinoReg(location,customer,neutrino_id)
            print(customer)
            print(location)
            time.sleep(5)
        neutrino_location_name,bucket = getNeutrinoReg(neutrino_id)

    print("Neutrino Name grabbed, starting data collection")
    # Fake Readings for Testing
    while True:
        fr1 = genRandomDecimal(70,73,99) # Temp
        fr2 = genRandomDecimal(30,33,99) # Humidity
        point = prepareReading(fr1,fr2,neutrino_id,neutrino_location_name)
        writeReading(bucket,org,point)
        time.sleep(15)
    # Real talk
    # INSERT REAL CODE HERE

    
	
	
if __name__ == "__main__":
	# calling main function
	main()