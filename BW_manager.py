#!/usr/bin/env python

#Bandwidth manager
import socket
import sys
import threading
import subprocess
import redis
import time
from datetime import datetime


unrecognized=400            #how much BW assign to unrecognized locations
emerged= dict()             #to remember which locations have already been emerged
                            #if you detect in the garbage collector that some locations are empty, then remove them from dict.
locinfo={}

try:
    ports = open(sys.argv[1], 'r+')   
    #read the ports file into mem
    for line in ports:
        data=line.split(" ")
        locinfo[str(data[0])]=[str(data[1]), data[2], data[3]]

    #on special command refresh the file again
except:
    print 'Couldn\'t open ports file for reading'
    
#if not running yet, start redis?

#make host&port,db parameters?
try:
    BW = redis.Redis(host='localhost', port=6379, db=0)         #subscribed - to listen
    WR = redis.Redis(host='localhost', port=6379, db=0)         #free one - to write and publish
except:
    print "Could not connect to localhost:6379, is redis running?"
    print sys.exc_info()

try:
    BW['init']='connected'
    WR['init']='connected'
except:
    print "Error estabishing connection to redis:"
    sys.exc_info()[0], sys.exc_info()[1]


class emerge(threading.Thread):

    def __init__ ( self, redis, writer ):
        self.BW=redis
        self.WR=writer
        threading.Thread.__init__ ( self )
    
    def run(self):
        try:
            BW=self.BW
            BW.subscribe(sys.argv[2])
            for msg in BW.listen():
                if msg['type'] == 'subscribe' : continue
                if msg['data'] == 'DIE' : sys.exit(255)
                print "Got message:", msg['data']
                
                tokens=msg['data'].rsplit('.',1)
                tokens.reverse()
                location=tokens[0]
                print "Got location:", location
                try:
                    num=emerged[location]
                    print 'Already emerged location', location, num ,'times'
                    emerged[location]+=1
                except:
                    #print "doing stuff to emerge that location"
                    
                    try:
                        router=locinfo[location][0]
                        port  =locinfo[location][1]
                        speed =locinfo[location][2]
                        
                        #just a quick temp fix
                        freeUp  =10000
                        freeDown=10000
                        
                        #requires 2.7?
                        #command='ssh root@noc "snmpget -v2c -chyvesro '+router+' FOUNDRY-SN-SWITCH-GROUP-MIB::snSwIfStatsOutBitsPerSec.'+port+" | sed 's/.*: \([0-9]\+\)/\\1/'"
                        #print command
                        #shell=subprocess.check_output(['sh',command],stderr=subprocess.STDOUT)
                        #print shell
                        #usedUp=shell/1000000 +1
                        #and the same for used down
                        
                        
                    except:
                        print "Couldn't retrieve location info for", location
                        #print sys.exc_info()
                        freeUp  =unrecognized
                        freeDown=unrecognized
                        
                    emerged[location]=1
                    message= "*emerge " + str(freeUp) + "/" + str(freeDown)
                    print datetime.now(), message, "for", location
                    #TODO: instead of msg[data] use proper format (e.g. location? )
                    WR[msg['data']+".upFree"]=freeUp
                    WR[msg['data']+".downFree"]=freeDown
                    WR.publish(msg['data']+".all.channel",message)
                     
                
        except:
            print 'caught exception: ', sys.exc_info()[0], sys.exc_info()[1]

#bandwidth emerge thread:
try:
    emerge(BW, WR).start()
except:
    print 'caught exception: ', sys.exc_info()
#control(BW).start()    - this is "controller" thread - via this you can change some parameters, like urecognized BW, reload ports etc. 

#key='test'
#print BW[key]
