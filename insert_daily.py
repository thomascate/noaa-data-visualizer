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
import threading
import queue
from multiprocessing import Process, active_children


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

config['Processes'] = 8

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

def insert_data(config, postChunk, docNumber, fileName):
  for attempt in range(10):
    print(f"Trying attempt {attempt}")
    try:
      time.sleep(attempt ** 2)
      r = requests.post(config['elasticURL'] + "/_bulk", verify=config['sslVerify'], auth=(config['elasticUser'], config['elasticPass']), data=postChunk, headers=config['elasticHeader'])
      docCount = postChunk.count('\n')/2
      logging.info(f"docs: {docCount}, docNumber: {docNumber} code: {r.status_code}, fileName: {fileName}, attempt: {attempt}")

      if not r.status_code == 200:
        raise Exception(f"insert failed code: {r.status_code}")
        logging.error(f"error: {r.text}")
      else:
        return

    except:
      print(f"try failed {r.status_code}")
      return

def celcius_fields():
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

  return(celciusFields)

def mm_fields():
  # These fields are defined in mm, defining here in case we want to save a copy in inches
  mmFields = [
    "PRCP",
    "SNOW",
    "SNWD",
    "EVAP",
    "MDEV",
    "MDPR",
    "MDSF",
    "THIC",
    "WESF"
  ]

  return(mmFields)

def cm_fields():
  # These fields are defined in cm, defining here in case we want to save a copy in inches
  cmFields = [
    "FRGB",
    "FRGT",
    "FRTH",
    "FRTH",
    "GAHT"
  ]

  return(cmFields)

def generate_document(fileName):

  celciusFields = celcius_fields()
  uploadDocument = ""

  if os.path.exists(fileName + ".done"):
    logging.info(f'{fileName}.done exists, skipping')
    return(uploadDocument)    

  logging.info(fileName)
  csvData = csv.DictReader(open(fileName).readlines())
  originalData = open(fileName).read().splitlines()
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

    # We want docID to be consistent, which means we can run this against duplicated data without getting duplicated entries in ES
    docID = hashlib.sha224(idString.encode('utf-8')).hexdigest()
    indexName = "noaa-data-" + jsonEntry['DATE'].replace('-','.')[0:4]

    # This is building a newline seperated _bulk upload document out of the entries
    uploadDocument+=json.dumps( { "index" : { "_index" : indexName, "_id" : docID } } )
    uploadDocument+="\n"
    uploadDocument+=json.dumps( jsonEntry )
    uploadDocument+="\n"

    i = i+1

  return(uploadDocument)

def upload_file(fileName):

  logging.info(f"function: upload_file, fileName: {fileName}, pid: {os.getpid()}")
  try:
    print(f"get upload document for {fileName}")
    uploadDocument = generate_document(fileName)

    if uploadDocument == "":
     return 

    postChunk = ""
    docNumber = 0

    for line in uploadDocument.splitlines():
      docNumber = docNumber+1
      postChunk+=line
      postChunk+="\n"
      docCount = postChunk.count('\n')

      if docCount == 500:
        print(f"found {docCount} lines, let's post it")
        insert_data(config, postChunk, docNumber/2, fileName)
        postChunk = ""

    if len(postChunk) > 0:
      print(f"found {docCount} lines, let's post it")
      insert_data(config, postChunk, docNumber/2, fileName)
      postChunk = ""

    open(fileName + ".done", 'a').close()
   #   attributes = {}
   #   attributes['path'] = fileName
   #   attributes['name'] = os.path.basename(fileName)
   #   attributes['size'] = os.path.getsize(fileName)
   #   print(attributes)

  except Exception as e:
    print(f"error on {fileName} {str(e)}")


files = glob.glob( config['ghcndLocation'] + "/*.csv")

fileQueue = queue.Queue()
activeProcesses = []

for file in files:
  fileQueue.put(file)

print(fileQueue.qsize())

while True:
  if fileQueue.empty() == False:
#    print(active_children())
    if len(active_children()) < config['Processes']:
      fileName = fileQueue.get()
      logging.info(f"active_children: {len(active_children())}, fileName: {fileName}, files_left: {fileQueue.qsize()}")
      p = Process(target=upload_file, args=(fileName,))
      p.start()
  time.sleep(0.01)

p.join()
#while len(active_children) > 0:
#  sleep



#for i in range(config['Processes']):
#  p = Process(target=upload_file, args=())
#  activeProcesses.append(p)
#  p.start()

#for activeProcess in activeProcesses:
#  activeProcess.join()

exit()
