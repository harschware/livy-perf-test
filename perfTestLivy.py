#!/usr/bin/python

import sys;
import requests, json;
from os import environ
import textwrap;
import time;

sid = None
if len(sys.argv) > 1:
   sid = sys.argv[1]


host = "http://localhost:8998"
headers = {}

WAIT=.050

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
      data = {"kind": "spark", "conf" : { "spark.driver.memory" : "512m", "spark.executor.memory": "512m"} }
      r = requests.post( host + "/sessions", data=json.dumps(data), headers=headers )
      print( "Sleeping 15s while waiting for session to start" )
      waiting = 15
      time.sleep(15)
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

   return sid

sid = startNewSession(sid)
sessions_url = host + "/sessions/" + str(sid)

statements_url = sessions_url + '/statements'
with open('init.scala', 'r') as myfile:
  initcode = myfile.read()
data = { 'code': textwrap.dedent(initcode) }
r = requests.post(statements_url, data=json.dumps(data), headers=headers)
j = r.json()
stid = str(j["id"]);

qid = timeResponse( stid, j )
print "Query # %s complete" % ( qid )

with open('code2.scala', 'r') as myfile:
  code2 = myfile.read()
while True:
   data = { 'code': textwrap.dedent(code2) }
   r = requests.post(statements_url, data=json.dumps(data), headers=headers)
   r.raise_for_status
   j = r.json()
   stid = str(j["id"]);

   qid = timeResponse( stid, j )
   print "Query # %s complete" % ( qid )
