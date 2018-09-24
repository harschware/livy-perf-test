#!/usr/bin/env python3

import sys
import requests, json
from os import environ
import textwrap
import time
import threading
import pprint
is_py2 = sys.version[0] == '2'
if is_py2:
    import Queue as queue
else:
    import queue as queue

host = "http://localhost:8420/api"
START_URL = "/v1/spark/shell/start"
TRANSFORM_URL = "/v1/spark/shell/transform"
headers = {}
data = {}

WAIT=.050

pp = pprint.PrettyPrinter(indent=4)

def add_input(input_queue):
    while True:
        input_queue.put(sys.stdin.read(1))


session = requests.Session()
session.auth = ('dladmin', 'thinkbig')



def startNewSession():
   r = session.post( host + START_URL, data=json.dumps(data), headers=headers )
   r.raise_for_status()
   # print(r.status_code)

def transformJson( script ): 
   return {"policies":[],"pageSpec":{"firstRow":0,"numRows":64,"firstCol":0,"numCols":1000},"doProfile":False,"doValidate":True,"script":script,"datasources":[],"catalogDatasets":[]}; 

def timeTransform( code ):
  start_time = time.time()

  r = session.post( host + TRANSFORM_URL, json=transformJson(code), headers=headers )
  r.raise_for_status
  j = r.json()
  # pp.pprint(j)

  count = 1
  status = str(j["status"])
  table = str(j["table"])

  while status != "SUCCESS":
    time.sleep( WAIT )
    count = count + 1
    r = session.get(host + TRANSFORM_URL + "/" + table, headers=headers)
    r.raise_for_status()
    j = r.json()
#    pp.pprint(j)
    status = str(j["status"])

  print( "Status checks: {}, Wait between checks: {:.2f}ms, Wall clock: {:.3f}s".format( str(count), WAIT * 1000, time.time() - start_time) )
#  pp.pprint(j)
  return

############################## MAIN #################################

startNewSession()
with open('initcode.txt', 'r') as myfile:
  initcode = myfile.read()

timeTransform( initcode )

print("Startup and Initial Query Complete" )

# Set us up to check for input
input_queue = queue.Queue()
input_thread = threading.Thread(target=add_input, args=(input_queue,))
input_thread.daemon = True
input_thread.start()

# Get the code ready
with open('perfcode.txt', 'r') as myfile:
  perfcode = myfile.read()

# Loop on statements
qid = 1
while True:
   timeTransform( perfcode )
   qid += 1
   print("Query # %s complete" % ( qid ) )

   # Check for user input, pause for 10 seconds if non-digit key entered, otherwise wait digit * 10 seconds
   if not input_queue.empty():
      userin = input_queue.get()
      if userin.isdigit():
        pause = float(userin)*10.0
      else:
        pause= 10.0
      print( "Pausing for " + str(pause) + " seconds..." )
      time.sleep(pause) 
