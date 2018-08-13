#!/usr/bin/env python

import sys
import requests, json
from os import environ
import textwrap
import time
import threading
import Queue

sid = None
if len(sys.argv) > 1:
   sid = sys.argv[1]

host = "http://localhost:8998"
headers = {}

WAIT=.050

def add_input(input_queue):
    while True:
        input_queue.put(sys.stdin.read(1))


def timeResponse( stid, j):
  start_time = time.time()
  count = 1
  progress = str(j["progress"])
  while progress != "1.0":
    time.sleep( WAIT )
    count = count + 1
    r = requests.get(statements_url + "/" + stid, headers=headers)
    r.raise_for_status
    j = r.json()
    progress = str(j["progress"])

  print( "Status checks: {}, Wait between checks: {:.2f}ms, Wall clock: {:.3f}s".format( str(count), WAIT * 1000, time.time() - start_time) )
  return str(j["id"])

def startNewSession( sid=None ):
   if sid is None:
     sid = environ.get("SESSION_ID")
 
   waiting = 0
   if sid is None:
      print( "No Session Defined" )
      data = {"kind": "pyspark", "conf" : { "spark.driver.memory" : "512m", "spark.executor.memory": "5120m"} }
      r = requests.post( host + "/sessions", data=json.dumps(data), headers=headers )
      WAIT_FOR_START=10
      print( "Sleeping " + str(WAIT_FOR_START) + "s while waiting for session to start" )
      waiting = WAIT_FOR_START
      time.sleep(WAIT_FOR_START)
   else:
      print( "Running tests against pre-existing session = %s" % ( sid ) )
      r = requests.get(host + "/sessions/" + str(sid), headers=headers)

   if r.status_code < 200 or r.status_code > 201:
      r.raise_for_status()
   else:
      while True:
        j = r.json()
        sid = j["id"]
        state = str(j["state"])
        if state == "idle":
            break
        time.sleep( 1 )
        waiting = waiting + 1
        print( "Waited %s seconds for Session %s to start" % ( waiting, sid ) )
        sessions_url = host + "/sessions/" + str(sid)
        r = requests.get(sessions_url, headers=headers)
        r.raise_for_status

   print( "export SESSION_ID=" + str(sid) ) 
   return sid

sid = startNewSession(sid)
sessions_url = host + "/sessions/" + str(sid)

statements_url = sessions_url + '/statements'
with open('initcode.txt', 'r') as myfile:
  initcode = myfile.read()
data = { 'code': textwrap.dedent(initcode) }
r = requests.post(statements_url, data=json.dumps(data), headers=headers)
j = r.json()
stid = str(j["id"]);

qid = timeResponse( stid, j )
print "Query # %s complete" % ( qid )

# Set us up to check for input
input_queue = Queue.Queue()
input_thread = threading.Thread(target=add_input, args=(input_queue,))
input_thread.daemon = True
input_thread.start()

# Get the code ready
with open('perfcode.txt', 'r') as myfile:
  code2 = myfile.read()
data = { 'code': textwrap.dedent(code2) }

# Loop on statements
while True:
   r = requests.post(statements_url, data=json.dumps(data), headers=headers)
   r.raise_for_status
   j = r.json()
   stid = str(j["id"]);

   qid = timeResponse( stid, j )
   print "Query # %s complete" % ( qid )
   # Check for user input, pause for 10 seconds if key entered
   if not input_queue.empty():
      userin = input_queue.get()
      if userin.isdigit():
        pause = float(userin)*10.0
      else:
        pause= 10.0
      print( "Pausing for " + str(pause) + " seconds..." )
      time.sleep(pause) 
