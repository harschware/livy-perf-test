#!/usr/bin/env python3

import sys, pprint
import requests, json
from os import environ
import os
import textwrap
import time
import threading
from optparse import OptionParser
is_py2 = sys.version[0] == '2'
if is_py2:
    import Queue as queue
else:
    import queue as queue


parser = OptionParser()
parser.add_option("-k", "--kind", dest="kind",
                  help="Code Kind (Required)", metavar="FILE")
parser.add_option("-o", "--output", dest="output",
                  help="Name of log file", metavar="FILE")
(options, args) = parser.parse_args()

if not options.kind:   #
    print("!!! Required Argument Missing !!!\n")
    parser.print_help()
    sys.exit()

if options.output:
    sys.stdout.flush()
    sys.stdout = open(options.output, 'w')

sid = None
if len(args) > 1:
   sid = args[1]

host = "http://localhost:8998"
headers = {}

pp = pprint.PrettyPrinter(indent=4)

CODE_KIND = os.getenv("CODE_KIND","spark")

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
  pp.pprint(j)
  return str(j["id"])

def startNewSession( sid=None ):
   if sid is None:
     sid = environ.get("SESSION_ID")
 
   waiting = 0
   if sid is None:
      print( "No Session Defined")
      data = {"kind": "shared", "conf" : { "spark.driver.memory" : "768m"} }
      r = requests.post( host + "/sessions", data=json.dumps(data), headers=headers )
      WAIT_FOR_START=10
      msg = "Sleeping " + str(WAIT_FOR_START) + "s while waiting for session to start"
      print( msg )
      waiting = WAIT_FOR_START
      time.sleep(WAIT_FOR_START)
   else:
      msg = "Running tests against pre-existing session = %s" % ( sid )
      print( msg )
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
        msg = "Waited %s seconds for Session %s to start" % ( waiting, sid )
        print( msg )
        sessions_url = host + "/sessions/" + str(sid)
        r = requests.get(sessions_url, headers=headers)
        r.raise_for_status

   msg = "export SESSION_ID=" + str(sid)
   print( msg )
   return sid

sid = startNewSession(sid)
sessions_url = host + "/sessions/" + str(sid)

statements_url = sessions_url + '/statements'
with open('initcode.txt', 'r') as myfile:
  initcode = myfile.read()
data = { 'kind' :  CODE_KIND, 'code': textwrap.dedent(initcode) }
r = requests.post(statements_url, data=json.dumps(data), headers=headers)
j = r.json()
stid = str(j["id"]);

qid = timeResponse( stid, j )
msg = "Query # %s complete" % ( qid )
print( msg )

# Set us up to check for input
input_queue = queue.Queue()
input_thread = threading.Thread(target=add_input, args=(input_queue,))
input_thread.daemon = True
input_thread.start()

# Get the code ready
with open('perfcode.txt', 'r') as myfile:
  code2 = myfile.read()
data = { 'kind' : CODE_KIND, 'code': textwrap.dedent(code2) }

# Loop on statements
i = 0
while not i:
   r = requests.post(statements_url, data=json.dumps(data), headers=headers)
   r.raise_for_status
   j = r.json()
   stid = str(j["id"]);

   qid = timeResponse( stid, j )
   msg = "Query # %s complete" % ( qid )
   print( msg )
   # Check for user input, pause for 10 seconds if key entered
   if not input_queue.empty():
      userin = input_queue.get()
      if userin.isdigit():
        pause = float(userin)*10.0
      else:
        pause= 10.0
      msg = "Pausing for " + str(pause) + " seconds..."
      print( msg )
      time.sleep(pause)
