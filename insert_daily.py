#!/usr/bin/env python3

import csv
import glob, os
import requests
import sys
import hashlib
import json
import logging
import time
import yaml

logging.basicConfig(filename='insert_daily.log',level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
configFile = "config.yaml"

# Try to load config file and exit on invalid yaml or OS error
try:
  with open( configFile, "r" ) as f:
    try:
      config = yaml.safe_load(f)
    except yaml.YAMLError as e:
      logging.fatal(f"Failed to load config file: {configFile}, Error: {e}" )
      print(e)
      exit(1)

except EnvironmentError as e:
  logging.fatal(f"Failed to load config file: {configFile}, Error: {e}" )
  print(e)
  exit(1)

config['elasticHeader'] = {'Content-Type': 'application/x-ndjson'}

# Grab secrets from env
config['elasticUser'] = os.environ.get('NOAA_ELASTIC_USER')
config['elasticPass'] = os.environ.get('NOAA_ELASTIC_PASSWORD')

if config['sslVerify'] is False:
  requests.packages.urllib3.disable_warnings()

files = glob.glob( config['ghcndLocation'] + "/*.csv")

# This is used to verify the source data type, so we can make sure it makes it to the correct type before being dumped to json.
def contains_int(n):
  try:
    a = int(n)
  except ValueError:
    return False
  else:
    return True

def contains_float(n):
  try:
    a = float(n)
  except ValueError:
    return False
  else:
    return True

def insert_data(config, postChunk, docNumber):
  for attempt in range(10):
    print(f"Trying attempt {attempt}")
    try:
      time.sleep(attempt ** 2)
      r = requests.post(config['elasticURL'] + "/_bulk", verify=config['sslVerify'], auth=(config['elasticUser'], config['elasticPass']), data=postChunk, headers=config['elasticHeader'])
      docCount = postChunk.count('\n')/2
      logging.info(f"docs: {docCount}, docNumber: {docNumber} code: {r.status_code}, attempt: {attempt}")

      if not r.status_code == 200:
        raise Exception(f"insert failed code: {r.status_code}")
        logging.error(f"error: {r.text}")
      else:
        return

    except:
      print(f"try failed {r.status_code}")
      return

# We need to track all fields that return in 10ths of a degree Celcius, so that we can later convert them to C and F.
celciusFields = [
  "TAVG",
  "TMAX",
  "TMIN",
  "MDTN",
  "MDTX",
  "MNPN",
  "MXPN",
  "TOBS"
]

# We need to add key names of S[N,X][0-8][1-7] as the soil type and depth of the measurement is embedded in the key name.
for i in range(0,9):
  for j in range(1,8):
    celciusFields.append("SN"+str(i)+str(j))
    celciusFields.append("SX"+str(i)+str(j))

for file in files:
  if os.path.exists(file + ".old"):
    logging.info(f'{file}.old exists, skipping')
    continue
  logging.info(file)
  csvData = csv.DictReader(open(file).readlines())
  originalData = open(file).read().splitlines()
  uploadDocument = ""
  i = 1

  for entry in csvData:
    jsonEntry = {}
    jsonEntry['Original_Data'] = {}
    jsonEntry['Original_Data']['csv_keys'] = originalData[0]
    jsonEntry['Original_Data']['csv_entry'] = originalData[i]

    for key in entry:
      if key.endswith('ATTRIBUTES'):
        jsonEntry[key] = {}
        tmpAttribs = entry[key].split(',')
        if len(tmpAttribs) >= 1:
          jsonEntry[key]['Measurement_Flag'] = tmpAttribs[0]
        else:
          jsonEntry[key]['Measurement_Flag'] = ""
        if len(tmpAttribs) >= 2:
          jsonEntry[key]['Quality_Flag'] = tmpAttribs[1]
        else:
          jsonEntry[key]['Quality_Flag'] = ""
        if len(tmpAttribs) >= 3:
          jsonEntry[key]['Source_Flag'] = tmpAttribs[2]
        else:
          jsonEntry[key]['Source_Flag'] = ""
      elif contains_int( entry[key].strip() ):
        jsonEntry[key] = int(entry[key].strip())
      elif contains_float(entry[key].strip()):
        jsonEntry[key] = float(entry[key].strip())
      else:
        jsonEntry[key] = entry[key].strip()

      if key in celciusFields and contains_int( entry[key].strip() ):
        temp = int(entry[key].strip())
        jsonEntry[str( key + "_C" )] = float( temp/10 )
        jsonEntry[str( key + "_F" )] = float( ((temp / 10) * 9/5) + 32 )

    jsonEntry['location'] = []
    jsonEntry['location'].append(float(jsonEntry['LONGITUDE']))
    jsonEntry['location'].append(float(jsonEntry['LATITUDE']))
    idString = jsonEntry['STATION'] + jsonEntry['DATE']
    docID = hashlib.sha224(idString.encode('utf-8')).hexdigest()
    indexName = "noaa-data-" + jsonEntry['DATE'].replace('-','.')[0:4]
    uploadDocument+=json.dumps( { "index" : { "_index" : indexName, "_id" : docID } } )
    uploadDocument+="\n"
    uploadDocument+=json.dumps( jsonEntry )
    uploadDocument+="\n"

    i = i+1

  postChunk = ""
  docNumber = 0
  for line in uploadDocument.splitlines():
     docNumber = docNumber+1
     postChunk+=line
     postChunk+="\n"
     if postChunk.count('\n') == 500:
        print("found 500 lines, let's post it")
        insert_data(config, postChunk, docNumber/2)
        postChunk = ""

  if len(postChunk) > 0:
    print(f"found {len(postChunk)} lines, let's post it")
    insert_data(config, postChunk, docNumber/2)
    postChunk = ""

exit()
