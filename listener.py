#!/usr/bin/env python
#the peer's distributed tracker part, manages transfers and connection speeds

#TODO: reuse redis pipes where possible
#TODO: make sure you don't forget to enable large files (>64GB) in rtorrent

import os, sys, traceback, threading, socket, time, random, subprocess, signal, logging
import socket as socketmodule               #some sockets inside connectToPeer just refuse to get created as normal sockets, yet as socketmodule.socket it works just fine [and before, it worked even with sockets, no problems]
import redis, xmlrpc2scgi 
from xmlrpc2scgi import RTorrentXMLRPCClient
from datetime import datetime
import logging.handlers

#the WorkDir is actually a sessionDir, since this script gets started there
WorkDir = os.getcwd()

#DOESN'T WORK, because xmlrpc cannot transfer such a large number
#global max_file_size            #this is a hack for rtorrent: normally it doesn't support files >64GB, but if you specify a bigger max size via d.set_max_file_size, then it works
#max_file_size = 137438953472   #4398046511104 = 4TB - it if is really 4TB is to be seen, IMO it overflows... safe value should be 137438953472, which is 128GB (i guess 255 GB should be also safe)


global recvSendBuf      #TODO: make a loops which read multiple times and join reads instead of relying on larger buffer [but still, i am sending messages myself of a known few bytes size] 
recvSendBuf = 16384     #this is the buffer size for sending & receiving

global reconnSec
reconnSec = 1           #how many seconds to wait before trying to reconnect to redis

global transfers        
transfers = 0           #how many transfers are currently going on

global timeout
timeout = 60.0          #how many seconds keep seeding after the file is downloaded

global refresh
refresh = 10.0          #each refresh seconds check out if the transfer is finished - for init seeder

global keepalive        #after how many seconds will speed info keys expire in redis; if it's expired, you know that the system is dead/disconnected
keepalive = 30.0        #beware that keepalive has to be higher than monitoringInterval, which states how often they will be refreshed; good value is 3x higher

global monitoringInterval   
monitoringInterval = 10 #each monitoringInterval seconds retrieve and upload new statistics...

global throttled
throttled = dict()      #list of peers already added to throttle groups

#TODO:    check if new rtorrent 0.8.7 can handle multiple concurrent xmlrpc commands...
global rLock            #lock for avoiding two threads calling xmlrpc command simultaneously
rLock = threading.Lock()

global grpConnLock      #lock for add_to_group mutex over throttled[peer] variable
grpConnLock = threading.Lock()


#some defaults; they get overwritten at start when reading settings
PORT = 10002            # Port on which this script listens
rtPORT = 10001          # Port on which rtorrent runs/does transfers
rdPORT = 6379           # Port on which redis runs
MGMT = WorkDir+"/MGMT.socket"  # Path to the management unix socket where the rtorrListener listens and processes commands...
SCGI = 'scgi://'+WorkDir+'/SCGI.socket' # path to SCGI socket, which is in the rtorrent session directory

#BWmain = 'p2p-bootserver.internal'      #the main BW manager; this one should be replicated and have hot standby
#BWMAIN would be used for dynamic BW and R management
 
myWeight = 0            #for dynamic BW mgmt    
maxSPD = 700            #700mbit per peer maximum
DCdefault = 2000        #2gbit per DC


#myUP/myDOWN is my cumulative up/down speed per all the transfers...
global upDownLock
upDownLock = threading.Lock()
global myUP
global myDOWN
global lastUP
global lastDOWN
myUP = myDOWN = lastUP = lastDOWN = 0

global TR           #all the transfer's specific and shared data
TR = {}             #TR[HASH][variable] is the transfer specific and shared (among more transfer managing threads) variable

global TRlock
TRlock = threading.Lock()

#MAIN starts here already:

#connect to rtorrent SCGI socket
try:
    rtorr = RTorrentXMLRPCClient(SCGI)
except:
    print datetime.now(), 'MAIN: Couldn\'t connect to rtorrent on', SCGI,':', sys.exc_info()[0], sys.exc_info()[1]
    sys.exit(255)



#try to create logfiles
try:
    connLog = open(WorkDir+'/connect.log', 'a+')
    grpLog  = open(WorkDir+'/groups.log', 'a+')
    annLog  = open(WorkDir+'/announce.log', 'a+')
except:
    print datetime.now(), "MAIN: Couldn't create/open files for logging"
    sys.exit(255)



#TODO?: change this into reading a configfile
#Read commandline settings
try:
    myIP   = sys.argv[1]
    myHOST = sys.argv[2]
    myCHNL = sys.argv[3]
    myRack = sys.argv[4]
    myDC   = sys.argv[5]
    rtPORT = int(sys.argv[6])
    PORT   = int(sys.argv[7])
    Rmain  = sys.argv[8]
    rdPORT = int(sys.argv[9])
    try:
        logHOST= sys.argv[10]
        logPORT= sys.argv[11]
    except:
        logHOST= ''

    Rhost = Rmain
    #BWmain = Rmain        #that's for dyn bw & r mgmt
except:
    print datetime.now(), 'MAIN: parameters are (comma separated): <myIP> <myHOST> <myChannel> <myRack> <myDC> <rtorrent port> <overlay port> <main bandwidth manager ip/hostname> <redis port> [<log_HOST> <log_PORT>]'
    print datetime.now(), 'MAIN: parameters given:', sys.argv[1:]
    print datetime.now(), 'MAIN: exception ', sys.exc_info()[0], sys.exc_info()[1]
    sys.stdout.flush()
    sys.exit(255)

#create main listener socket
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((myIP, PORT))
    s.listen(5)       #the more the better, since I should expects some hoardes in the transfer start
except:
    #TODO: >>stderr?
    print datetime.now(), 'MAIN: Could not open socket on port', PORT, 'for listening: ', sys.exc_info()[0],sys.exc_info()[1] 
    sys.stdout.flush()
    sys.exit(255)

global log
#initialize logging:
log = logging.getLogger("hyves.p2p.f")
tlog = logging.getLogger("hyves.p2p.t")

if logHOST == '':
    print datetime.now(), 'MAIN: using local logging'
    sys.stdout.flush()    
    handler = logging.handlers.SysLogHandler("/dev/log", logging.handlers.SysLogHandler.LOG_DAEMON)
    thandler = logging.handlers.SysLogHandler("/dev/log", logging.handlers.SysLogHandler.LOG_DAEMON)
else:
    print datetime.now(), 'MAIN: using remote logging'
    sys.stdout.flush()
    handler = logging.handlers.SysLogHandler((str(logHOST),int(logPORT)),logging.handlers.SysLogHandler.LOG_DAEMON)
    thandler = logging.handlers.SysLogHandler((str(logHOST),int(logPORT)),logging.handlers.SysLogHandler.LOG_DAEMON)

format = logging.Formatter("%(name)-9s %(levelname)-8s %(funcName)-16s %(message)s")
tformat = logging.Formatter("%(name)-9s %(levelname)-8s %(threadName)-16s %(message)s")

handler.setFormatter(format)
thandler.setFormatter(tformat)
handler.setLevel(logging.DEBUG)
thandler.setLevel(logging.DEBUG)
log.addHandler(handler)
tlog.addHandler(thandler)
log.setLevel(logging.DEBUG)
tlog.setLevel(logging.DEBUG)
log.info("%s MAIN: starting..." % datetime.now() )


def mutex(function, parameters, lock):
    ''' Mutual exclusion on lock: run the function(parameters) and return result'''
    global log

    result = False
    lock.acquire()

    try:
        #log.debug("%s About to call %s with parameters %s" % (datetime.now(), function , parameters) )
        result = function(parameters)
    except:
        log.error("%s Failed calling %s, parameters %s, error: %s %s" % (datetime.now(), function, parameters, sys.exc_info()[0], sys.exc_info()[1] ))
        sys.stdout.flush()
        raise
    finally:
        lock.release()
        
    return result

def RTex(function, parameters):         #just a wrapper; unfortunatelly doesn't work for d.add_peer, because of some xmlrpc2scgi.py trouble (but at least if I call it directly, it works)    
    return mutex(function, parameters, rLock)


#TODO: v 0.8.7 is supposed to support multithreaded xmlrpc handling, lock may no more be needed; 
#still, this needs to be tested well, if the rtorrent implementation doesn't have any trouble with race conditions etc. (e.g. 2 concurrent xmlrpc calls to modify the same throttle etc.)

def TRkeys():
    '''
        Returns list of currently running transfer's HASHes
    '''
    
    global TRlock
    
    TRlock.acquire()
    try:
        keys = TR.keys()
    except:
        keys = None
    finally:       
        TRlock.release()
        return keys

def addToGroup(group, peer):
    ''' Adds peer to given group, returns true on success (even if the peer has been added to this group already) or false otherwise. '''
    global log
    
    result = False
    if peer == myIP :
        log.info("%s Not adding self to any group..." % datetime.now()) 
        return True

    grpConnLock.acquire()

    try:
        num = throttled[peer]
        log.info("%s Already added %s to group %s times" % ( datetime.now(), peer, num ) )
        throttled[peer] += 1
        result = True
    except:
        try:
            err = RTex(rtorr.throttle_grp, (group, peer) )
            throttled[peer] = 1
            if err != 0:
                log.error("%s Non-zero (%s) return code when adding peer %s to group %s" % (datetime.now(), err, peer, group)) 
                result = False
            else:
                log.info("%s Adding %s to group %s" % (datetime.now(), peer, group))   
                result = True
        except:
            log.error("%s Error adding peer %s to group %s : %s %s" % (datetime.now(), peer, group, sys.exc_info()[0], sys.exc_info()[1]))
            result = False
    finally:    
        grpConnLock.release()
        
    return result

def addPeer(HASH, peer):
    ''' Adds throttled peer to rtorrent download HASH (this means rtorrent will connect to that peer for the given download), returns true on success, false otherwise. '''
    global log
    
    result = False
    if peer == myIP :
        log.info("%s Not adding self to any transfers..." % datetime.now())
        return True
    
    grpConnLock.acquire()
    
    try:
        num = throttled[peer]
        try:
            dest = peer+':'+str(rtPORT)
            rLock.acquire()
            try:
                err = rtorr.d.add_peer(HASH, dest)
            except:
                log.error("%s Error adding peer %s to transfer %s : %s %s" % (datetime.now(), peer, HASH, sys.exc_info()[0], sys.exc_info()[1]))
                err = 255
            finally:
                rLock.release()
            
            #err=RTex(rtorr.d.add_peer, (HASH, dest) )    #This way it just doesn't work - it never correctly passes the peer (it passes '' instead)
            if err !=0:
                log.error("%s Non-zero (%s) return code when adding peer %s to transfer %s" % (datetime.now(), err, peer, HASH)) 
                result = False
            else:   
                log.info("%s Adding %s to transfer %s" % (datetime.now(), peer, HASH))
                result = True
        except:
            log.error("%s Error adding peer %s to transfer %s : %s %s" % (datetime.now(), peer, HASH, sys.exc_info()[0], sys.exc_info()[1]))
            result = False
    
    except:
        log.error("%s %s not added to any group yet!, not adding to transfer %s" % (datetime.now(), peer, HASH))
        result = False

    finally:
        grpConnLock.release()
        
    return result

class connectToPeer(threading.Thread):
    ''' Let the peer know that we have added him to our throttle groups, thus as soon as he adds us, he can connect to the rtorrent directly... (see addPeer function)'''
    
    def __init__ (self, HASH, peer):
        self.HASH = HASH
        self.peer = peer
        threading.Thread.__init__( self )
        self.setName("connectToPeer")

    def run(self):
        peer = self.peer
        HASH = self.HASH
        local= threading.local()
        global tlog
        
        print "i am alive!"
        sys.stdout.flush()
        
        if peer == myIP :
            tlog.info("%s Not connecting to self..." % datetime.now()) 
            return True
        
        success = False
        
        try:
            local.s = socketmodule.socket(socketmodule.AF_INET, socketmodule.SOCK_STREAM)
        except:
            tlog.error("%s Couldn't create socket, thus unable to connect to %s : %s %s" % (datetime.now(), peer, sys.exc_info()[0], sys.exc_info()[1]))
            return False
        
        try:
            local.s.connect((peer, PORT))     #PORT is usually 10002; defined in the config file
        except:
            tlog.error("%s Couldn't connect to %s:%s : %s %s" % (datetime.now(), peer, PORT, sys.exc_info()[0], sys.exc_info()[1]) )
            return False 

        request = ['CONNECT', myIP, myRack, myDC, HASH]    
        tlog.info("%s Sending request: %s to %s" % (datetime.now(), str(" ".join(request)), peer ))
        local.s.send(" ".join(request))
        
        data = local.s.recv(16384)
        local.s.close()
        log.info("%s Received: %r from %s" % (datetime.now(), data, peer) )
        
        reply = data.split(' ')
        if reply[0] == "200" :  success = True
        else:   log.warning("%s Problem when connecting to %s : %r" % (datetime.now(), peer, data))      
            
        return success

class announceTransfer(threading.Thread):
    ''' Inform the peer that he should join the given transfer '''
    
    def __init__ (self, peer, HASH, destPath, torrentPath, md5Path, speed, priority, localTorrentPath):
        self.HASH = HASH
        self.peer = peer
        self.destPath = destPath
        self.torrentPath = torrentPath
        self.md5Path = md5Path
        self.speed = speed
        self.prio = priority
        self.ltp = localTorrentPath
        threading.Thread.__init__( self )
        self.setName("announceTransfer")

    def run(self):
        peer = self.peer
        local= threading.local()
        global tlog
        
        #COPY THE FILES FIRST
        getTorr = subprocess.Popen(["scp", "-oStrictHostKeyChecking=no",  self.ltp, str(self.peer)+":/etc/p2p/" ], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
        code = getTorr.wait()
        if code != 0:
            tlog.error("%s Couldn't get torrent file to %s, scp return code = %s" % (datetime.now(), self.peer, code))
            return                  #no torrent file = nothing to download! = no announce....
        tlog.info("%s Got %s to %s" % (datetime.now(), self.ltp, peer))
        
        getMd5 = subprocess.Popen(["scp", "-oStrictHostKeyChecking=no", self.md5Path , str(self.peer)+":/etc/p2p/" ], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
        code = getMd5.wait()
        if code != 0:
            tlog.error("%s Couldn't get md5 file to %s, scp return code = %s" % (datetime.now(), self.peer, code))
            return                  #no md5 file = corrupt download - which in reality might not be corrupt, thus creating a misleading and confusing situation
        tlog.info("%s Got %s to %s" % (datetime.now(), self.md5Path, peer))
        
        #THEN ANNOUNCE:
        if str(peer) == str(myHOST) :
            tlog.info("%s Not announcing to self..." % datetime.now()) 
            return True
        
        success = False
        
        try:
            local.s = socketmodule.socket(socketmodule.AF_INET, socketmodule.SOCK_STREAM)
        except:
            tlog.error("%s Couldn't create socket, thus unable to connect to %s : %s %s" % (datetime.now(), peer, sys.exc_info()[0], sys.exc_info()[1]))
            return False
        
        try:
            local.s.connect((peer, PORT))     #PORT is usually 10002; defined in the config file
        except:
            tlog.error("%s Couldn't connect to %s:%s : %s %s" % (datetime.now(), peer, PORT, sys.exc_info()[0], sys.exc_info()[1]) )
            return False 

        request = ['TRANSFER', self.HASH, myIP, self.destPath, self.torrentPath, self.md5Path, str(self.speed), str(self.prio)]
        tlog.info("%s Sending request: %s to %s" % (datetime.now(), str(" ".join(request)), peer ))
        local.s.send(" ".join(request))
        
        data = local.s.recv(16384)
        local.s.close()
        tlog.info("%s Received: %r from %s" % (datetime.now(), data, peer) )
        
        reply = data.split(' ')
        if reply[0] == "200" :
                success = True
        else:   tlog.warning("%s Problem when connecting to %s : %r" % (datetime.now(), peer, data))

        return success

def post(text, lock, msgQueue):
    ''' Post a redis message containing text to the head of msgQueue protected by the lock... '''
    global log
    
    try:
        msg = {'pattern': None, 'type': 'message', 'channel': 'like_we_do_care', 'data': text} 
        lock.acquire()
        msgQueue.insert(0, msg)    #push the msg. to the shared queue's head
        lock.notify()
    except:
        log.error("%s Exception when trying to post %s : %s %s" % (datetime.now(), text, sys.exc_info()[0], sys.exc_info()[1] ))
    finally:
        lock.release()
    
def yielder(shared_variable, condition_variable):
    ''' The yielder is yielding the messages from the shared_variable protected by condition_variable. The yielder is a typical consumer... '''
  
    while 1:
        condition_variable.acquire()
        if len(shared_variable) == 0 : condition_variable.wait()        #if there's nothing to read, wait until it's delivered/produced ...
        local_variable = shared_variable.pop(0)                         #there will always be st. to get, otherwise we wouldn't be notified
        condition_variable.release()
        yield local_variable

class subscriber(threading.Thread):
    ''' Subscribes to a channel on host, receives messages and puts them to a shared_variable protected by the condition_variable... '''
    
    def __init__(self, host, channel, shared_variable, condition_variable):
        self.hst = host
        self.chn = channel
        self.shv = shared_variable
        self.cdv = condition_variable
        self.DIE = False                    #if I should quit
        threading.Thread.__init__(self)
        self.setName("subscriber")
        
    
    def run(self):
        ready = self.cdv
        R = redis.Redis(host = self.hst, port = rdPORT, db=0)
        #print datetime.now(), "subscriber thread:", self.chn
        #sys.stdout.flush()
        global tlog
        
        tlog.info("%s Subscriber" % datetime.now())
        
        while 1:
            if self.DIE : return   
            try:
                #R['init']=self.getName()    #don't - if you're still subscribed on reconnect, you'll get error
                R.subscribe(self.chn)
            except:
                print datetime.now(), "subscriber: Error establishing connection to", str(self.hst)+":"+str(rdPORT), "and subscribing to", self.chn
                print sys.exc_info()[0],":",sys.exc_info()[1]
                time.sleep(reconnSec)
                print datetime.now(), "subscriber: Reconnecting..."
                sys.stdout.flush()
                continue
            
            try:
                print datetime.now(), "subscriber: Connected to", str(self.hst)+":"+str(rdPORT)+", about to listen on", self.chn
                sys.stdout.flush()
                for msg in R.listen():
                    #print datetime.now(), 'subscriber', self.getName(), 'got msg', msg
                    #sys.stdout.flush()
                    
                    if self.DIE : return    #death can come anytime
                    ready.acquire()
                    self.shv.append(msg)    #push the msg. to the shared queue
                    ready.notify()
                    ready.release()
            except:                         #this is to keep trying reconnect
                print datetime.now(), "subscriber: Error when listening at", str(self.hst)+":"+str(rdPORT), "on", self.chn
                print sys.exc_info()[0], ":", sys.exc_info()[1]
                sys.stdout.flush()
            finally:
                try:
                    ready.release() #since I might still be holding it, but maybe not - it depends which command raised the exception
                except:
                    pass
                
            if self.DIE : return
            print datetime.now(), "subscriber: Reconnecting..."
            time.sleep(reconnSec)       #this is probably a connection error, so just wait a bit and then try again....
                
        #should be never reached:
        #print datetime.now(), 'listener', self.getName(), 'quit'
        #sys.stdout.flush()

                

class rtorrListener(threading.Thread):
    ''' This thread is listening on MGMT.socket and processes commands both from rtorrent (e.g. transfer finished) and the user - submitted via sockecho.py '''
        
    def __init__(self, socketPath):
        self.sPath = socketPath
        threading.Thread.__init__(self)
        self.setName("rtorrListener")
        
    def run(self):
        global TR
        
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            os.remove(self.sPath)
        except OSError:
            pass    #it's OK - it will fail if the file doesn't exist yet
            
        try:
            s.bind(self.sPath)
        except:
            print datetime.now(), "rListen: Couldn\'t bind the path for rtorrent communication socket:", self.sPath
            sys.exit(255)       #this is really serious, so the whole application should exit
        
        s.listen(1)
        print datetime.now(), "rListen: Listening on", self.sPath
        sys.stdout.flush()

        while 1:
            conn, addr = s.accept()
        
            #print datetime.now(), "rListen: Got connection from", addr
            data = conn.recv(16384)
            #TODO: is 1024 enough?
            #TODO: if not data: #TROUBLE
            
            print datetime.now(), "rListen: received:", data
            sys.stdout.flush()
            
            parse = data.split(' ')
            
#           print datetime.now(), "rListen: parsed", parse
#           print datetime.now(), "rListen: HASH =", HASH
#           sys.stdout.flush()
            
            
            try:
                if parse[0] == 'HASH' :                       #this is message from rtorrent (via finished.py)
                    HASH = parse[1]
                    try:
                        a = TRkeys().index(HASH)
                    except:
                        print datetime.now(), "rListen: not doing transfer", HASH 
                        sys.stdout.flush()
                    
                    if parse[2] == "OK": 
                        print datetime.now(), "rListen: posting FINISHED download..."
                        sys.stdout.flush()
                        post('*FINISHED download', TR[HASH]['mgr'].ready, TR[HASH]['mgr'].messages)
                        
                    if parse[2] == "CORRUPT": 
                        print datetime.now(), "rListen: posting CORRUPTED download..."
                        sys.stdout.flush()
                        post('*CORRUPTED download', TR[HASH]['mgr'].ready, TR[HASH]['mgr'].messages)

                    if parse[2] == "UNKNOWN":
                        print datetime.now(), "rListen: posting CORRUPTED download..."
                        sys.stdout.flush()
                        post('*CORRUPTED download', TR[HASH]['mgr'].ready, TR[HASH]['mgr'].messages)
                    
                    continue
                    
                if parse[0] == 'INIT' :                     #this message from user (via sockecho.py) to start transfer
                        #TODO: check if message format is OK
                        HASH = parse[1]
                        Rhost = parse[2]    #=self? but also can specify some other dedicated host....
                        destPath = parse[3]
                        torrentPath = parse[4]
                        md5Path = parse[5]
                        speed = int(parse[6])
                        destRemote = parse[7]
                        try:
                            priority = parse[8]
                        except:
                            priority = 2
 
                        #if not(busy):
                        conn.send("200 OK, starting the transfer...") 
                        transferWrapper(HASH, Rhost, torrentPath, destPath, md5Path, True, destRemote, speed, priority).start()
                        #else: 
                        #    conn.send("400 Already doing a transfer...")
                        continue
                            
                if parse[0] == 'TRANSFER' :                 #this message is to join a transfer
                        #TODO: check if message format is OK
                        HASH = parse[1]
                        Rhost = parse[2]
                        destPath = parse[3]
                        torrentPath = parse[4]
                        md5Path = parse[5]
                        speed = int(parse[6])
                        try:
                            priority = parse[7]
                        except:
                            priority = 2 
 
                        #if not(busy):
                        conn.send("200 OK, joining the transfer...")
                        transferWrapper(HASH, Rhost, torrentPath, destPath, md5Path, False, '', speed, priority).start()
                        #else:
                            #conn.send("400 Already doing a transfer...")
                        continue
                
                if str(parse[0]).upper() == 'PID' :                      #kind-of PING
                        conn.send(repr(os.getpid()))
                        continue
                    
                if str(parse[0]).upper() == 'STOPALL' :
                        try:
                            a=0
                            keys = TRkeys()
                            for HASH in keys:
                                post('*FINISHED transfer', TR[HASH]['mgr'].ready, TR[HASH]['mgr'].messages)
                                a += 1
                            conn.send("200 OK, stopping "+str(a)+" transfers.")
                        except:
                            print datetime.now(), "rListen: not doing transfer", HASH
                            conn.send("200 OK, wasn't doing any transfer already...")
                            sys.stdout.flush()
                        
                        continue
                   
                conn.send("400 Unknown command: "+repr(parse[0])+" in "+repr(data)) 
                            
            except:
                print datetime.now(), "rListen: error processing", data
                print datetime.now(), "rListen: exception:", sys.exc_info()
                sys.stdout.flush()
                
            #TODO: add more mgmt commands here
                
            sys.stdout.flush()        
            conn.close()


class connManager(threading.Thread ):
    ''' This thread is listening on the main script PORT for the incoming requests (or information) about connecting to peers:
        The incoming connection comes from the peer who has already added us to his throttle groups and is letting us know that 
        as soon as we add him to our throttles, we can tell rtorrent to add that peer to the given download. '''

    def __init__(self, socket, address ):
        self.conn = socket
        self.address = address
        threading.Thread.__init__(self)
        self.setName("connManager")

    def run(self):
        #global busy
        #print datetime.now(), 'Received connection:', self.address
        
        #TODO: is 1024 enough? how does it really read? what if the message is fragmented? I don't expect more than 1024 bytes, but can I expect it arrives intact?
        data = self.conn.recv(16384)
        print datetime.now(),"connMan: received", repr(data)
        
        #    while recv != '':
        #        recv = self.socket.recv(1024)
        #        self.data += recv
        #        if not self.data: break               #EOF
        #        print self.data

        try:
            parse = data.split(' ')     #parse
            #print datetime.now(),"connMan: received", parse
            sys.stdout.flush()
            
            if parse[0] == 'CONNECT':
                if len(parse) < 5:               #it should be: <hostname/IP> <rack> <DC> <HASH>
                    self.conn.send ( "500 couldn't parse your CONNECT request" )
                    print >>connLog, datetime.now(), 'Failed parsing:', repr(parse), '.'
                else:            
                    peer = parse[1]
                    HASH = parse[4]
                    success = True
    #               print 'My peer\'s rack is', parse[2]
    #               print 'My peer\'s DC   is', parse[3]
    #               print 'My peer\'s HASH is', parse[4]
    
                    if parse[2] == myRack \
                     and parse[3] == myDC:   group='myRack'
                    elif parse[3] == myDC:   group='myDC'
                    else:                    group='outside'
    
                    #print datetime.now(), 'I would add this peer to group', group
                    success = addToGroup(group, peer)
    
                    #if successfully added to group, then connect:
                    if success: success = addPeer(HASH, peer)
                        
                    if success: self.conn.send ( '200 OK' )
                    else:       self.conn.send ( "400 Couldn't complete connection attempt")            
            
            elif parse[0] == 'TRANSFER' :                 #this message is to join a transfer
                    if len(parse) < 7:
                        self.conn.send ( "500 couldn't parse your TRANSFER request" )
                        print datetime.now(),"connMan: couldn't parse TRANSFER request:", parse
                        sys.stdout.flush()
                        
                    else:
                        HASH = parse[1]
                        Rhost = parse[2]
                        destPath = parse[3]
                        torrentPath = parse[4]
                        md5Path = parse[5]
                        speed = int(parse[6]) 
                        try:
                            priority = parse[7]
                        except:
                            priority = 2
 
                        #if not(busy):
                        self.conn.send("200 OK, joining the transfer...")
                        transferWrapper(HASH, Rhost, torrentPath, destPath, md5Path, False, '', speed, priority).start()
                        #else:
                        #    self.conn.send("400 Already doing a transfer...")
                
            elif str(parse[0]).upper() == 'PID' :                      #kind-of PING
                        self.conn.send(repr(os.getpid()))

            elif str(parse[0]).upper() == 'STOPALL' :
                        try:
                            a=0
                            keys = TRkeys()
                            for HASH in keys:
                                post('*FINISHED transfer', TR[HASH]['mgr'].ready, TR[HASH]['mgr'].messages)
                                a += 1
                            self.conn.send("200 OK, stopping "+str(a)+" transfers.")
                        except:
                            print datetime.now(), "rListen: not doing transfer", HASH
                            self.conn.send("200 OK, wasn't doing any transfer already...")
                            sys.stdout.flush()
                        
                            
            else: self.conn.send("500 Unknown command: "+repr(parse[0])+" in "+repr(data))
             
                            
        except:
                print datetime.now(), "rListen: error processing", data
                print datetime.now(), "rListen: exception:", sys.exc_info()
                sys.stdout.flush()
        finally:
            #that's it, request processed...
            self.conn.close()
            #print datetime.now(), 'Closed connection:', self.address [ 0 ]

def updateSpeeds():
    ''' Computes cumulative myUP and myDOWN (for throttles) and updates outside throttles if they have changed '''
    global TR
    global upDownLock
    global myUP
    global myDOWN
    global lastUP
    global lastDOWN
    global maxSPD
    
    sumOutUP = 0
    sumOutDOWN=0
    sumDCUP  = 0
    sumDCDOWN= 0
    
    print datetime.now(), "updSPD: about to compute some stuff..."
    sys.stdout.flush()
    
    upDownLock.acquire()
    try:
        #print datetime.now(), "updSPD: got lock... about to get keys"  
        #sys.stdout.flush()
        keys = TRkeys()
        print datetime.now(), "updSPD: found", len(keys), "keys"  
        sys.stdout.flush()
        
        try:
            for a in keys:
                
                #every computation in try except - because some just starting transfers don't have those keys yet
                try:    sumOutUP += TR[a]['outUP']
                except: pass
                try:    sumOutDOWN += TR[a]['outDOWN']
                except: pass
                try:    sumDCUP += TR[a]['dcUP']
                except: pass    
                try:    sumDCDOWN += TR[a]['dcDOWN']
                except: pass
        except:
            print datetime.now(), "updSPD: trouble computing sums:", sys.exc_info()[0], sys.exc_info()[1]
            print repr(TR)   
            sys.stdout.flush()
            return
            
    
        print datetime.now(), "updSPD: computed sums outUP, outDOWN:", sumOutUP, sumOutDOWN  
        sys.stdout.flush()
    
        if sumDCUP > maxSPD : sumDCUP = maxSPD
        if sumDCDOWN > maxSPD : sumDCDOWN = maxSPD
        
        #I can't have higher up/download from outside group than from DC, since it actually goes via the same link
        if sumOutUP > sumDCUP : sumOutUP = sumDCUP
        if sumOutDOWN > sumDCDOWN : sumOutDOWN = sumDCDOWN
        
        myUP = sumDCUP
        myDOWN = sumDCDOWN
        
        print datetime.now(), "updSPD: Set myUP/myDOWN rates:", str(myUP)+"/"+str(myDOWN)  
        sys.stdout.flush()
        
        update = False
        if sumOutUP != lastUP or sumOutDOWN != lastDOWN : 
            update = True
            lastUP = sumOutUP
            lastDOWN = sumOutDOWN
        
        
        #print "updateSpeeds: computed some stuff..."
        #sys.stdout.flush()
                    
        if update:
            th_down=str(sumOutDOWN)+'k'
            th_up=str(sumOutUP)+'k'
            try:
                print datetime.now(), "updSPD: about to update rtorrent"  
                sys.stdout.flush()
                err1 = RTex(rtorr.throttle_down, ('outside',th_down))
                err2 = RTex(rtorr.throttle_up, ('outside',th_up))
                print datetime.now(), "updSPD: Set outside throttle up/down rates:", str(th_up)+"/"+str(th_down)  
                sys.stdout.flush()
            except:
                err1 = 0
                err2 = 0
                print datetime.now(), "updSPD: Error setting outside throttle up/down rates:", sys.exc_info()
                sys.stdout.flush()
            if err1 != 0 or err2 != 0:
                print datetime.now(), "updSPD: Error setting outside throttle up/down rates, codes=", str(err1), str(err2)
                sys.stdout.flush()
    except:
        print datetime.now(), "updSPD: trouble computing speeds:", sys.exc_info()[0], sys.exc_info()[1]   
        sys.stdout.flush()
    finally:
        upDownLock.release()
        print datetime.now(), "updSPD: released lock"  
        sys.stdout.flush()


class speedManager(threading.Thread):
    
    def __init__(self, redisHost, HASH):
        self.DIE = False 
        self.HASH = HASH
        self.rHost = redisHost
        threading.Thread.__init__(self)
        self.setName("speedManager")
    
    def run(self):
        global TR
        HASH = self.HASH
        rHost= self.rHost
        speed = TR[HASH]['speed']
        
        #global role
        #global defaultSPD
        #global myUP
        #global myDOWN
        
        print datetime.now(), 'spdMan: starting...'
        sys.stdout.flush()
        
        
        
        #for now only failsafe mode and for only one transfer

        while not(self.DIE):        
            try:
                BW = redis.Redis(host=rHost, port=rdPORT, db=0)    #to listen
                RD = redis.Redis(host=rHost, port=rdPORT, db=0)    #to read
            except:
                print datetime.now(), "spdMan: Could not connect to", str(rHost)+":"+str(rdPORT), ", is redis running?"
                print sys.exc_info()
                time.sleep(reconnSec)
                print datetime.now(), "spdMan: Reconnecting..."
                sys.stdout.flush()
                continue
    
            #it actually tries to connect only here:
            try:
                BW['init']='connected'
                RD['init']='connected'
            except:
                print datetime.now(), "spdMan: Error establishing connection to redis on", str(rHost)+":"+str(rdPORT)
                print sys.exc_info()[0], sys.exc_info()[1]
                time.sleep(reconnSec)
                print datetime.now(), "spdMan: Reconnecting..."
                sys.stdout.flush()
                continue
            
            #TODO: listen on all the channels where you're R  - when you become R there, launch another thread that manages that thing...
            print datetime.now(), 'spdMan: about to subscribe to', HASH+'.'+myDC+'.'+myRack+'.all.channel'
            sys.stdout.flush()
            BW.subscribe(HASH+'.'+myDC+'.'+myRack+'.all.channel')
            
            print datetime.now(), 'spdMan: Waiting for overlay/init managers to join the overlay...'
            TR[HASH]['speedEvent'].wait()
            print datetime.now(), 'spdMan: finished waiting... and finishing due to embedded spdman'
            return
            
            try:
                for msg in BW.listen():
                    #print datetime.now(), 'spdMan: self.DIE=', self.DIE
                    #sys.stdout.flush() 
                    #if msg['type'] == 'subscribe' : continue
                    if self.DIE : break
        
        
        
        
                    update=False
                    #if I am an R, then update speeds
                    print datetime.now(), "spdMan: role is", TR[HASH]['role'], "and message is", msg['data']
                    
                    if TR[HASH]['role'] >= 2:     
                        
                        uploaders = RD.scard(HASH+'.'+myDC+'.'+myRack+'.uploaders')
                        downloaders=RD.scard(HASH+'.'+myDC+'.'+myRack+'.downloaders')
                        print datetime.now(), 'spdMan: found', uploaders, 'uploaders and', downloaders, 'downloaders in my rack'
                        
                        if uploaders == 0  : uploaders = 1   #&& spit fire!!! as this should never happen
                        if downloaders== 0 : downloaders = 1 #&& spit fire!!! as this should never happen
                        
                        myUP =  speed/ uploaders
                        myDOWN= speed/ downloaders
                        
                        if myUP < 10 : myUP = 10
                        if myDOWN<10 : myDOWN=10
                        
                        myUP = myUP / 10
                        myDOWN=myDOWN/10
                        
                        myDC_UP = 0
                        myDC_DOWN=0
                        
                        update=True
                        
                    #only if I can also go out
                    if TR[HASH]['role'] == 3 :    
                        DCuploaders = RD.scard(HASH+'.'+myDC+'.uploaders')
                        DCdownloaders=RD.scard(HASH+'.'+myDC+'.downloaders')
                        print datetime.now(), 'spdMan: found', DCuploaders, 'uploaders and', DCdownloaders, 'downloaders in my DC'
                        
                        if DCuploaders == 0 :  DCuploaders = 1   #&& spit fire!!! as this should never happen
                        if DCdownloaders==0 : DCdownloaders= 1 #&& spit fire!!! as this should never happen
                        
                        myDC_UP = DCdefault / DCuploaders
                        myDC_DOWN=DCdefault/DCdownloaders
                        
                        if myDC_UP < 10 : myDC_UP = 10
                        if myDC_DOWN<10 : myDC_DOWN=10 
                        
                        myDC_UP = myDC_UP / 10
                        myDC_DOWN=myDC_DOWN/10
                        
                        #because if you want to go out of DC, you have to go out of rack - the groups are inclusive
                        if myDC_UP > myUP : myDC_UP = myUP
                        if myDC_DOWN > myDOWN : myDC_DOWN = myDOWN
                        
                        update=True
      
      
      
        
                    if update:
                        try:
                            TR[HASH]['outUP']  =myDC_UP
                            TR[HASH]['outDOWN']=myDC_DOWN
                            TR[HASH]['dcUP']   =myUP
                            TR[HASH]['dcDOWN'] =myDOWN
                            updateSpeeds()        
                        except KeyError, e:
                            print datetime.now(), "spdMan: Couldn't update the speeds in TR[HASH], transfer stopped? Exception: %s" % e
                            print repr(TR) 
                            sys.stdout.flush()

                        print datetime.now(), 'spdMan: dcUP/dcDOWN ; outUP/outDOWN: [', str(myUP)+"/"+str(myDOWN),';', str(myDC_UP)+"/"+str(myDC_DOWN),"] for", HASH    
                        sys.stdout.flush()
            except:
                print datetime.now(), "spdMan: Error when reading messages from", str(rHost)+":"+str(rdPORT), "on", HASH+'.'+myDC+'.'+myRack+'.all.channel'
                print sys.exc_info()[0], sys.exc_info()[1]
                time.sleep(reconnSec)
                print datetime.now(), "spdMan: Reconnecting..."
                sys.stdout.flush()
                continue            
            
        print datetime.now(), 'spdMan: DONE...'
        sys.stdout.flush()
                              

class initOverlayManager(threading.Thread):
    ''' initOverlayManager is the first seeder, he runs local redis to store shared transfer related data. The init peer doesn't move in the overlay,
        but instead joins the root immediately and stays there until the transfer is finished. Then it just does the cleanup and if no other transfers
        are running, shuts the local redis down. The lists with finished/corrupt peers are still available at the p2pmain redis. 
    '''
    def __init__(self, hash, Rhost):
        self.hash = hash
        self.redis = Rhost
        self.ready = threading.Condition()
        self.messages = []
        threading.Thread.__init__(self)
        self.setName("initOverlayManager")

    def run(self):
        HSH = HASH = self.hash
        speed = TR[HASH]['speed']
        
        #global started          #here you remember for all the transfers if you have started the redis
        
        #global role
        #global speedEvent
        #global announceEvent
        #global targets
        
        TR[HASH]['role'] = 1
        
        ready = self.ready          #shared condition variable for all the subscribed threads
        messages = self.messages    #shared queue of the messages retrieved by all the subscribers
        
        rootListener = None
        DC_listener  = None
        rack_listener= None
        
        print datetime.now(), "INIT: This is the INIT overlay manager for transfer", HSH #, "started =", started
        
        
        try:
            RD = redis.Redis(host=myIP, port=rdPORT, db=0)    #to read
            RD.sadd(HASH+'.PENDING', str(myHOST))
            toGoHosts = RD.scard(HASH+'.PENDING')
        except:
            #COMPLETE FAIL - because e.g. I don't have PENDING hosts list...

            print datetime.now(), "INIT: About to restart redis2..."
            sys.stdout.flush()            
            #my own redis isn't somehow running, how can this be possible? Nevertheless, I need to restart it...
            redis2 = subprocess.Popen(["/etc/init.d/redis2", "restart"], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
            code = redis2.wait()
            if code != 0:
                print datetime.now(), "INIT: Couldn't restart redis2, return code =", code
                sys.stdout.flush()
        
                #failed to start, this is major problem
                return
            else:
                #if this fails again even after restart, then screw it all, dtrack is allowed to crash as well...
                RD = redis.Redis(host=myIP, port=rdPORT, db=0)    #to read
                RD.sadd(HASH+'.PENDING', str(myHOST))
                toGoHosts = RD.scard(HASH+'.PENDING')
        
        #the list of peers which want the file, already filled in by the transfer wrapper shell script before initManager is started        
        print datetime.now(), "INIT: Found", toGoHosts, "targets in the", HASH+'.PENDING'
        sys.stdout.flush()
        
        self.redis = myIP
        
        #kill possible old broken/still running transfer:
        keys = RD.keys(HASH+".*.*.all")                 #RACKS only
        #print "GOT KEYS:", keys
        #sys.stdout.flush()
        modkeys = [a+'.channel' for a in keys]
        #print "GOT MODKEYS", modkeys
        #sys.stdout.flush()
        total = 0
        for channel in modkeys:
            publicum = RD.publish(channel, '*FINISHED transfer')   #this can be quite very slow & demanding on the redis server
            total += publicum
            #print "INIT: receivers =", publicum        
        print datetime.now(), "INIT: Total peers receiving FINISH message:", total
        
        #cleanup first:        
        keys = RD.keys(HASH+".*")
        keys +=RD.keys("."+HASH+".*")
        #print "GOT KEYS TO REMOVE:", keys
        #removed = RD.delete(' '.join(keys))
        #print datetime.now(), "INIT: about to remove", keys
        
        #st. like for a in [key for key in keys if key != str(HASH)+".PENDING"] should be faster than the thing below
        #or nopending = lambda key: key != str(HASH)+".PENDING"
        #and then keys = filter( nopending, keys)
        #and this should be also faster than what's used now:
        pipe = RD.pipeline(False)
        for a in keys:
            if a != str(HASH)+".PENDING" : 
                pipe.delete(a)
                #print "INIT: deleting ", a
        result = pipe.execute()
        #print datetime.now(), "INIT: result =", result
        
        #subscribe
        rack_listener = subscriber(self.redis, HSH+'.'+myDC+"."+myRack+'.all.channel', messages, ready)
        rack_listener.start()
        
        try:
            
            #atomically join rack group - I am starting as a seeder
            pipe = RD.pipeline(True)
            pipe.sadd(HSH+'.'+myDC+'.'+myRack+'.S', myIP)
            pipe.sadd(HSH+'.'+myDC+'.'+myRack+'.all', myIP)
            pipe.smembers(HSH+'.'+myDC+'.'+myRack+'.N')
            results = pipe.execute()
    
            #add the group peers to throttle groups and connect to them [but there shouldn't be any yet!]
            for peer in results.pop():
                    if addToGroup('myRack', peer): connectToPeer(HASH, peer).start()
                    #if not success : print "*couldn't connect to", peer
    
            #let the speed manager run
            TR[HASH]['speedEvent'].set()
    
            #become R
            diff = [ HSH+'.'+myDC+'.N' , HSH+'.'+myDC+'.'+myRack+'.N' ]     #I should be the first, but just in case....
            DC_listener = subscriber(self.redis, HSH+'.'+myDC+'.all.channel', messages, ready)
            DC_listener.start()
    
            #atomically join DC group
            pipe = RD.pipeline(True)
            pipe.sadd(HSH+'.'+myDC+'.S', myIP)
            pipe.sadd(HSH+'.'+myDC+'.all', myIP)
            pipe.sadd(HSH+'.'+myDC+'.'+myRack+'.uploaders', myIP)
            pipe.sdiff(diff)
            results = pipe.execute()
            
            TR[HASH]['role'] = 2
            RD.publish(HSH+'.'+myDC+'.'+myRack+'.all.channel','*promoted: '+myIP)   #I should be the only one, but just in case...
            
            for peer in results.pop():
                if addToGroup('myDC', peer): connectToPeer(HASH, peer).start()
                #if not success : print "*couldn't connect to", peer
            print datetime.now(), "INIT: promoted to rack uploader and joined DC group", myDC
            sys.stdout.flush()
         
            diff = [ HSH+'.'+'.N' , HSH+'.'+myDC+'.N' ]
            rootListener = subscriber(self.redis, HSH+'.all.channel', messages, ready)
            rootListener.start()
    
            #atomically join DC group
            pipe = RD.pipeline(True)
            pipe.sadd(HSH+'.S', myIP)
            pipe.sadd(HSH+'.all', myIP)
            pipe.sadd(HSH+'.'+myDC+'.uploaders', myIP)
            pipe.sdiff(diff)
            results = pipe.execute()
            
            TR[HASH]['role'] = 3
            RD.publish(HSH+'.'+myDC+'.all.channel','*promoted: '+myIP)
            RD.publish(HSH+'.'+myDC+'.'+myRack+'.all.channel','*promoted: '+myIP)   #this is for current kind-of crippled BW mgmt
            
            for peer in results.pop():
                if addToGroup('outside', peer): connectToPeer(HASH, peer).start()
                else: print datetime.now(), "INIT: *couldn't connect to", peer
                
            print datetime.now(), "INIT: promoted to DC uploader and joined root group; now waiting for the transfer to finish..."
            #lastEvent = time.time()
            
            RD.smove(HASH+'.PENDING', HASH+'.FINISHED', str(myHOST))
            #start transfer on all the peers in .PENDING
            TR[HASH]['targets'] = RD.smembers(HASH+'.PENDING')
            TR[HASH]['announceEvent'].set()
    
            #this is to prevent stalling on no messages...
            global refresh
            live = threading.Timer(refresh, post, ['*SELF refresh', ready, messages] )
            live.start()
            
            for msg in yielder(messages, ready):        # <==> while not finished...
                #print datetime.now(), "INIT: recv message: ", msg
                if msg['type'] == 'message' : print datetime.now(), "INIT: recv message:", msg['data']
                
                doneHosts = RD.scard(HASH+'.FINISHED')
                corruptHosts = RD.scard(HASH+'.CORRUPT')
                if (doneHosts + corruptHosts) == toGoHosts : break                   #that's it, done
                if msg['data'] == '*FINISHED transfer' : break      #manual stop - usefull for restarts
                
                if msg['data'] == '*SELF refresh' :
                    print datetime.now(), "INIT: refresh"
                    #just to keep it spinning
                    #check if our own redis is alive
                    live = threading.Timer(refresh, post, ['*SELF refresh', ready, messages] )
                    live.start()
                
                update=False
                    
                #if I am an R, then update speeds
                if TR[HASH]['role'] >= 2:     
                    
                    uploaders = RD.scard(HASH+'.'+myDC+'.'+myRack+'.uploaders')
                    downloaders=RD.scard(HASH+'.'+myDC+'.'+myRack+'.downloaders')
                    print datetime.now(), 'spdMan: found', uploaders, 'uploaders and', downloaders, 'downloaders in my rack'
                    
                    if uploaders == 0  : uploaders = 1   #&& spit fire!!! as this should never happen
                    if downloaders== 0 : downloaders = 1 #&& spit fire!!! as this should never happen
                    
                    myUP =  speed/ uploaders
                    myDOWN= speed/ downloaders
                    
                    if myUP < 10 : myUP = 10
                    if myDOWN<10 : myDOWN=10
                    
                    myUP = myUP / 10
                    myDOWN=myDOWN/10
                    
                    myDC_UP = 0
                    myDC_DOWN=0
                    
                    update=True
                    
                #only if I can also go out
                if TR[HASH]['role'] == 3 :    
                    DCuploaders = RD.scard(HASH+'.'+myDC+'.uploaders')
                    DCdownloaders=RD.scard(HASH+'.'+myDC+'.downloaders')
                    print datetime.now(), 'spdMan: found', DCuploaders, 'uploaders and', DCdownloaders, 'downloaders in my DC'
                    
                    if DCuploaders == 0 :  DCuploaders = 1   #&& spit fire!!! as this should never happen
                    if DCdownloaders==0 : DCdownloaders= 1 #&& spit fire!!! as this should never happen
                    
                    myDC_UP = DCdefault / DCuploaders
                    myDC_DOWN=DCdefault/DCdownloaders
                    
                    if myDC_UP < 10 : myDC_UP = 10
                    if myDC_DOWN<10 : myDC_DOWN=10 
                    
                    myDC_UP = myDC_UP / 10
                    myDC_DOWN=myDC_DOWN/10
                    
                    #because if you want to go out of DC, you have to go out of rack - the groups are inclusive
                    if myDC_UP > myUP : myDC_UP = myUP
                    if myDC_DOWN > myDOWN : myDC_DOWN = myDOWN
                    
                    update=True
    
                if update:
                    try:
                        TR[HASH]['outUP']  =myDC_UP
                        TR[HASH]['outDOWN']=myDC_DOWN
                        TR[HASH]['dcUP']   =myUP
                        TR[HASH]['dcDOWN'] =myDOWN
                        updateSpeeds()        
                    except KeyError, e:
                        print datetime.now(), "spdMan: Couldn't update the speeds in TR[HASH], transfer stopped? Exception: %s" % e
                        print repr(TR) 
                        sys.stdout.flush()

                    print datetime.now(), 'spdMan: dcUP/dcDOWN ; outUP/outDOWN: [', str(myUP)+"/"+str(myDOWN),';', str(myDC_UP)+"/"+str(myDC_DOWN),"] for", HASH    
                    sys.stdout.flush()
                
                
                sys.stdout.flush()
                    
            print datetime.now(), "INIT: about to do cleanup..."
    
            #there really shouldn't be anyone anywhere still downloading by now, so you can do this safely
            #if you have started redis and you are not running any other transfers, you can stop it now...
    
            
            #everyone, it's DONE:
            keys = RD.keys(HASH+".*.*.all")                 #RACKS only
            #print "GOT KEYS:", keys
            #sys.stdout.flush()
            modkeys = [a+'.channel' for a in keys]
            #print "GOT MODKEYS", modkeys
            sys.stdout.flush()
            total = 0
            for channel in modkeys:
                publicum = RD.publish(channel, '*FINISHED transfer')   #this can be quite very slow & demanding on the redis server
                total += publicum
                #print "INIT: receivers =", publicum
                                                            #moreover, there's actually no need to do any cleanup 
                                                            #      e.g. RD.publish(modkeys, '*FINISHED dropALL')
                                                            #TODO: if you are the last transfer, dont wipe, just drop the whole DB (and send dropALL instead)
            
            print datetime.now(), "INIT: Total peers receiving FINISH message:", total
        
            RD.rename(HASH+".FINISHED","."+HASH+".FINISHED")
            
        except:
            print datetime.now(), "INIT: Got exception handling redis, rHost =", str(self.redis)+":"+str(rdPORT), ", transfer might need restarting..."
            print sys.exc_info()[0], sys.exc_info()[1]
            sys.stdout.flush()
            
        #save the results for later analysis
        #if there are no corrupt hosts, this will fail, but so what
        try:
            RD.rename(HASH+".CORRUPT","."+HASH+".CORRUPT")
        except:
            pass
        
        try:
            #TODO: is this necessary? there shouldn't be anyone listening now...
            RD.publish(HASH+'.all.channel', "*quit: "+myIP)
            RD.publish(HASH+'.'+myDC+'.all.channel', "*quit: "+myIP)
            RD.publish(HASH+'.'+myDC+'.'+myRack+'.all.channel', "*quit: "+myIP)
            
            #cleanup
            keys = RD.keys(HASH+".*")
            #print "GOT KEYS TO REMOVE:", keys
            #removed = RD.delete(' '.join(keys))
            #print datetime.now(), "INIT: about to remove", keys
            pipe = RD.pipeline(False)
            for a in keys: 
                pipe.delete(a)
                #print "INIT: deleting ", a
                
            result = pipe.execute()
        except:
            pass
        
        #STOP the subscribers
        rootListener.DIE  = True
        DC_listener.DIE   = True
        rack_listener.DIE = True
        
        #that's it, all done
        print datetime.now(), "INIT: ***FINISHED***", HASH
        sys.stdout.flush()
        return

class overlayManager(threading.Thread):
    def __init__ (self, hash, Rhost, seeder):
        self.hash = hash
        self.redis = Rhost
        self.ready = threading.Condition()
        self.messages = []
        self.seed = seeder
        self.reconnect = False
        threading.Thread.__init__(self)
        self.setName("overlayManager")

    def run(self):
        global TR
        HSH = HASH = self.hash
        ratio= 0.95          #if you're above this percentage done, you can't become R - this is to prevent some turbulence in the end
        global rLock         #rtorrent xmlrpc lock
        speed = TR[HASH]['speed']
        
        #global speedEvent
        #global role          #shared with BW manager
        
        highgroup = ""       #the highest group I am member of - for the purpose of ID computation
        myID = 0             #default for the 1st peer
        oldID = myID
        peersNO = 1          #there's always at least me in any group i'm member of... 
        oldNO = peersNO     

        seeder = self.seed          #if I am seeder (= have the file already)        
        ready  = self.ready         #shared condition variable for all the subscribed threads
        messages = self.messages    #shared queue of the messages retrieved by all the subscribers
        
        
        #so that I can check if I have listener threads already running in case of reconnect...
        rack_listener= None
        DC_listener  = None
        rootListener = None
        
        print datetime.now(), "ovrMan: This is the overlay manager for transfer", HSH
        success = False
        
        TR[HASH]['role'] = 1             #you start in your rack and might make it further in the overlay
        finished = 0
        connected = False
        while not(connected):
            try:
                RD = redis.Redis(host=self.redis, port=rdPORT, db=0)    #to read
                toGoHosts= RD.scard(HASH+'.PENDING')
                connected= True
            except:
                connected= False
                print datetime.now(), "ovrMan: Could not connect to", str(self.redis)+":"+str(rdPORT), ", is redis running?"
                print sys.exc_info()[0], sys.exc_info()[1]
                time.sleep(reconnSec)
                print datetime.now(), "ovrMan: Reconnecting..."
                sys.stdout.flush()
                continue
        
        #subscribe
        if rack_listener == None: 
            rack_listener = subscriber(self.redis, HSH+'.'+myDC+"."+myRack+'.all.channel', messages, ready)
            rack_listener.start()
            highgroup = HSH+'.'+myDC+"."+myRack+".N"
        
        success = False
        while not(success):
            try:
                if seeder:  
                    RD.smove(HASH+".PENDING", HASH+".FINISHED", myHOST)
                    finished = 2
                else:
                    RD.sadd(HASH+'.PENDING', myHOST)
                
                #atomically join rack group
                pipe = RD.pipeline(True)
                if seeder:  pipe.sadd(HSH+'.'+myDC+'.'+myRack+'.S', myIP)
                else:       pipe.sadd(HSH+'.'+myDC+'.'+myRack+'.N', myIP)
                pipe.sadd(HSH+'.'+myDC+'.'+myRack+'.all', myIP)
                if seeder:  pipe.smembers(HSH+'.'+myDC+'.'+myRack+'.N')
                else:       pipe.smembers(HSH+'.'+myDC+'.'+myRack+'.all')
                results = pipe.execute()
                peers = results.pop()
                success = True
                
                #print datetime.now(), "ovrMan: got results1: ", repr(results)
                #sys.stdout.flush()
            except:
                success = False
                print datetime.now(), "ovrMan: Disconnected from", str(self.redis)+":"+str(rdPORT), ", is redis running?"
                print sys.exc_info()[0], sys.exc_info()[1]
                time.sleep(reconnSec)
                print datetime.now(), "ovrMan: Reconnecting..."
                sys.stdout.flush()
                continue
        
        #let the speed manager run
        TR[HASH]['speedEvent'].set()
        
        #add the group peers to throttle groups and connect to them
        print datetime.now(), "ovrMan: Found these peers in my rack:", peers
        for peer in peers:
                if addToGroup('myRack', peer):  connectToPeer(HASH, peer).start()
                #if not success : print "*couldn't connect to", peer
        
        RD.publish(HSH+'.'+myDC+'.'+myRack+'.all.channel','*joined: '+myIP)
        #wait until speed manager is ready
        
        
        if finished == 0:       #only become normal peer if you're not yet finished
            #print datetime.now(), "ovrMan: about to read messages..."
            #sys.stdout.flush()  
            for msg in yielder(messages, ready):        # <==> while not finished...
                #if msg['type'] == 'message' :
                #print datetime.now(), "ovrMan: recv message:", msg['data']
                
                update=False
                if msg['type'] == 'subscribe' and self.reconnect :
                    if TR[HASH]['role'] >= 0:   #reconnect to group peers
                        peers = RD.smembers(HSH+'.'+myDC+'.'+myRack+'.all')
                        for peer in peers:
                            if addToGroup('myRack', peer): connectToPeer(HASH, peer).start()
                        print datetime.now(), "ovrMan: re-connected to peers in my rack", myRack
                    if TR[HASH]['role'] >= 1:   #reconnect to DC peers
                        peers = RD.smembers(HSH+'.'+myDC+'.all')
                        for peer in peers:
                            if addToGroup('myDC', peer): connectToPeer(HASH, peer).start()
                        print datetime.now(), "ovrMan: re-connected to peers in my DC", myDC  
                    if TR[HASH]['role'] >= 2:   #reconnect to root peers
                        peers = RD.smembers(HSH+'.all')
                        for peer in peers:
                            if addToGroup('output', peer): connectToPeer(HASH, peer).start()
                        print datetime.now(), "ovrMan: re-connected to peers in the root"
                
                if msg['data'] == '*FINISHED download' :            #this message gets sent to us by ourselves, triggered by completetion of download in rtorrListener
                    print datetime.now(), "ovrMan: Finished download:", HASH
                    finished = 2
                    sys.stdout.flush()
                    break
                if msg['data'] == '*CORRUPTED download' :            #this message gets sent to us by ourselves, triggered by completetion of download in rtorrListener
                    print datetime.now(), "ovrMan: Corrupted download:", HASH
                    finished = 0
                    sys.stdout.flush()
                    break
                if msg['data'] == '*FINISHED transfer' :            #this message gets posted by the init seeder when the complete transfer is done
                    print datetime.now(), "ovrMan: Finished complete transfer", HASH        #and this can be quite some trouble, since i might not be finished by then
                    finished = 1
                    sys.stdout.flush()
                    break
                
                print datetime.now(), "ovrMan: my role is:", repr(TR[HASH]['role'])
                sys.stdout.flush()
                
                try:
                    if TR[HASH]['role'] == 1 :
                        #check out if your're actually not re-joining and are an R already
                        if RD.sismember(HSH+'.'+myDC+'.N', myIP) == 0:
                            time.sleep( random.random() / 3 )
                            curRep = RD.scard(HSH+'.'+myDC+'.'+myRack+'.downloaders')
                            if curRep == '' : curRep = 0
                            if curRep < 2 :
                                try:
                                    completed = RTex(rtorr.d.get_bytes_done, HASH)
                                    file_size = RTex(rtorr.d.get_size_bytes, HASH)
                                except:                        
                                    print datetime.now(), "ovrMan: Error getting info from rtorrent:", sys.exc_info()
                                
                                fraction = completed / file_size
                                print datetime.now(), "ovrMan: my download complete percentage is", fraction
                                if fraction < ratio or curRep == 0 and ratio != 1:
                                    #become R
                                    
                                    diff = [ HSH+'.'+myDC+'.all' , HSH+'.'+myDC+'.'+myRack+'.all' ]
                                    if DC_listener == None:
                                        DC_listener = subscriber(self.redis, HSH+'.'+myDC+'.all.channel', messages, ready)
                                        DC_listener.start()
                    
                                    #atomically join DC group
                                    pipe = RD.pipeline(True)
                                    pipe.sadd(HSH+'.'+myDC+'.N', myIP)
                                    pipe.sadd(HSH+'.'+myDC+'.all', myIP)
                                    pipe.sadd(HSH+'.'+myDC+'.'+myRack+'.downloaders', myIP)
                                    pipe.sadd(HSH+'.'+myDC+'.'+myRack+'.uploaders', myIP)
                                    pipe.sdiff(diff)
                                    results = pipe.execute()
                                    
                                    TR[HASH]['role'] = 2
                                    highgroup = HSH+'.'+myDC+".N"
                                    
                                    RD.publish(HSH+'.'+myDC+'.'+myRack+'.all.channel','*promoted: '+myIP)
                                    peers = results.pop()
                                    print datetime.now(), "ovrMan: Found these peers in my DC:", peers
                                    for peer in peers:
                                        if addToGroup('myDC', peer): connectToPeer(HASH, peer).start()
                                        #if not success : print "*couldn't connect to", peer
                                    
                                    print datetime.now(), "ovrMan: promoted to rack R with ratio", fraction, "and joined DC group", myDC
                                else:
                                    print datetime.now(), "ovrMan: NOT promoted to rack R because of ratio", fraction
                        else:
                            #reconnect
                            diff = [ HSH+'.'+myDC+'.all' , HSH+'.'+myDC+'.'+myRack+'.all' ]
                            if DC_listener == None:
                                DC_listener = subscriber(self.redis, HSH+'.'+myDC+'.all.channel', messages, ready)
                                DC_listener.start()
                            peers = RD.sdiff(diff)                                
                            TR[HASH]['role'] = 2
                            highgroup = HSH+'.'+myDC+".N"
                            #RD.publish(HSH+'.'+myDC+'.'+myRack+'.all.channel','*promoted: '+myIP)
                            print datetime.now(), "ovrMan: Found these peers in my DC:", peers
                            for peer in peers:
                                if addToGroup('myDC', peer): connectToPeer(HASH, peer).start()
                            print datetime.now(), "ovrMan: re-joined DC", myDC
                            
                    if TR[HASH]['role'] == 2 :
                        if RD.sismember(HSH+'.N', myIP) == 0:
                            time.sleep( random.random() / 3 )
                            curRep = RD.scard(HSH+'.'+myDC+'.downloaders')
                            if curRep == '' : curRep = 0
                            if curRep < 5 :
                                try:
                                    completed = RTex( rtorr.d.get_bytes_done, HASH)
                                    file_size = RTex( rtorr.d.get_size_bytes, HASH)
                                except:
                                    print datetime.now(), "ovrMan: Error getting info from rtorrent:", sys.exc_info()
                                
                                fraction = completed / file_size
                                print datetime.now(), "ovrMan: my download complete percentage is", fraction
                                if fraction < ratio or curRep == 0 and ratio != 1 :
            
                                    #become R
                                    diff = [ HSH+'.all' , HSH+'.'+myDC+'.all' ]
                                    if rootListener == None:
                                        rootListener = subscriber(self.redis, HSH+'.all.channel', messages, ready)
                                        rootListener.start()
                    
                                    #atomically join DC group
                                    pipe = RD.pipeline(True)
                                    pipe.sadd(HSH+'.N', myIP)
                                    pipe.sadd(HSH+'.all', myIP)
                                    pipe.sadd(HSH+'.'+myDC+'.downloaders', myIP)
                                    pipe.sadd(HSH+'.'+myDC+'.uploaders', myIP)
                                    pipe.sdiff(diff)
                                    results = pipe.execute()
                                    
                                    TR[HASH]['role'] = 3
                                    highgroup = HSH+".N"
                                    
                                    RD.publish(HSH+'.'+myDC+'.all.channel','*promoted: '+myIP)
                                    RD.publish(HSH+'.'+myDC+'.'+myRack+'.all.channel','*promoted: '+myIP)   #this is for current kind-of crippled BW mgmt
                                    peers = results.pop()
                                    print datetime.now(), "ovrMan: Found these peers in the root:", peers
                                    for peer in peers:
                                        if addToGroup('outside', peer): connectToPeer(HASH, peer).start()
                                        #if not success : print "*couldn't connect to", peer
                                        
                                    print datetime.now(), "ovrMan: promoted to DC R with ratio", fraction, "and joined root group"
                                else:
                                    print datetime.now(), "ovrMan: NOT promoted to DC R because of ratio", fraction
                        else:
                            diff = [ HSH+'.all' , HSH+'.'+myDC+'.all' ]
                            if rootListener == None:
                                rootListener = subscriber(self.redis, HSH+'.all.channel', messages, ready)
                                rootListener.start()
                            peers = RD.sdiff(diff)                                
                            TR[HASH]['role'] = 3
                            highgroup = HSH+".N"
                            #RD.publish(HSH+'.'+myDC+'.all.channel','*promoted: '+myIP)
                            #RD.publish(HSH+'.'+myDC+'.'+myRack+'.all.channel','*promoted: '+myIP)   #this is for current kind-of crippled BW mgmt
                            print datetime.now(), "ovrMan: Found these peers in the root:", peers
                            for peer in peers:
                                if addToGroup('outside', peer): connectToPeer(HASH, peer).start()
                            print datetime.now(), "ovrMan: re-joined root"
                        
                    if TR[HASH]['role'] >= 2:     
                    
                        uploaders = RD.scard(HASH+'.'+myDC+'.'+myRack+'.uploaders')
                        downloaders=RD.scard(HASH+'.'+myDC+'.'+myRack+'.downloaders')
                        print datetime.now(), 'spdMan: found', uploaders, 'uploaders and', downloaders, 'downloaders in my rack'
                        
                        if uploaders == 0  : uploaders = 1   #&& spit fire!!! as this should never happen
                        if downloaders== 0 : downloaders = 1 #&& spit fire!!! as this should never happen
                        
                        myUP =  speed/ uploaders
                        myDOWN= speed/ downloaders
                        
                        if myUP < 10 : myUP = 10
                        if myDOWN<10 : myDOWN=10
                        
                        myUP = myUP / 10
                        myDOWN=myDOWN/10
                        
                        myDC_UP = 0
                        myDC_DOWN=0
                        
                        update=True
                    
                    if TR[HASH]['role'] == 3 :    
                        DCuploaders = RD.scard(HASH+'.'+myDC+'.uploaders')
                        DCdownloaders=RD.scard(HASH+'.'+myDC+'.downloaders')
                        print datetime.now(), 'spdMan: found', DCuploaders, 'uploaders and', DCdownloaders, 'downloaders in my DC'
                        
                        if DCuploaders == 0 :  DCuploaders = 1   #&& spit fire!!! as this should never happen
                        if DCdownloaders==0 : DCdownloaders= 1 #&& spit fire!!! as this should never happen
                        
                        myDC_UP = DCdefault / DCuploaders
                        myDC_DOWN=DCdefault/DCdownloaders
                        
                        if myDC_UP < 10 : myDC_UP = 10
                        if myDC_DOWN<10 : myDC_DOWN=10 
                        
                        myDC_UP = myDC_UP / 10
                        myDC_DOWN=myDC_DOWN/10
                        
                        #because if you want to go out of DC, you have to go out of rack - the groups are inclusive
                        if myDC_UP > myUP : myDC_UP = myUP
                        if myDC_DOWN > myDOWN : myDC_DOWN = myDOWN
                        
                        update=True
                    
                    if update:
                        try:
                            TR[HASH]['outUP']  =myDC_UP
                            TR[HASH]['outDOWN']=myDC_DOWN
                            TR[HASH]['dcUP']   =myUP
                            TR[HASH]['dcDOWN'] =myDOWN
                            updateSpeeds()        
                        except KeyError, e:
                            print datetime.now(), "spdMan: Couldn't update the speeds in TR[HASH], transfer stopped? Exception: %s" % e
                            print repr(TR) 
                            sys.stdout.flush()

                        print datetime.now(), 'spdMan: dcUP/dcDOWN ; outUP/outDOWN: [', str(myUP)+"/"+str(myDOWN),';', str(myDC_UP)+"/"+str(myDC_DOWN),"] for", HASH    
                        sys.stdout.flush()
                        
                except:
                    print datetime.now(), "ovrMan: Got exception inside yielder loop, rHost =", str(self.redis)+":"+str(rdPORT), ", is redis running?"
                    print sys.exc_info()[0], sys.exc_info()[1]
                    time.sleep(reconnSec)
                    print datetime.now(), "ovrMan: Attempting reconnect on next message..."
                    sys.stdout.flush()
                    continue
                
                
                #update_ID
                try:
                    members = list( RD.smembers(highgroup) )
                    peersNO = RD.scard(highgroup)
                    
                    #print datetime.now(), "ovrMan: peersNO =", peersNO, "; members =", members
                    #sys.stdout.flush()
                    
                    try:
                        myID = members.index(myIP)                            
                    except:
                        #this should never happen...
                        print datetime.now(), "ovrMan: couldn't find myIP among highgroup", highgroup, "members"
                        sys.stdout.flush()
                    
                    if myID != oldID or peersNO != oldNO :
                        oldID=myID
                        oldNO=peersNO
                        
                        if peersNO != 0 :
                            rLock.acquire()    
                            try:
                                #TODO: fix rtorrent to not break down when using this:
                                err = rtorr.d.update_ID(HSH, str(myID)+"/"+str(peersNO) )
                                #err = 0
                            except:
                                print datetime.now(), "ovrMan: Error updating myID/peersNO to", str(myID)+"/"+str(peersNO), "for", HSH, sys.exc_info()
                                sys.stdout.flush()
                                print rGetMessage(HASH)
                                err = 255
                            finally:
                                rLock.release()
                            if err != 0:
                                print datetime.now(), "tovrMan: Error", err, "when trying to set myID/peersNO to", str(myID)+"/"+str(peersNO), "for", HSH, sys.exc_info()
                            else:                                     
                                print datetime.now(), "ovrMan: updated myID/peersNO to ", str(myID)+"/"+str(peersNO)
                                sys.stdout.flush()
                except:
                    print datetime.now(), "ovrMan: Got exception when trying to set myID/peerNO, rHost =", str(self.redis)+":"+str(rdPORT), ", is redis running?"
                    print sys.exc_info()[0], sys.exc_info()[1]
                    time.sleep(reconnSec)
                    print datetime.now(), "ovrMan: Attempting to reconnect and set myID/peerNO again on next message..."
                    sys.stdout.flush()
                    continue

            
            print datetime.now(), "ovrMan: finished =", finished    
            sys.stdout.flush()
            
            
            try:
                if finished == 2: RD.smove(HASH+".PENDING", HASH+".FINISHED", myHOST)
                
                Trole = TR[HASH]['role']
                publishRack = True  #for the current ver. of BW mgmt
                if Trole == 3 :
                    RD.srem(HASH+'.'+myDC+'.downloaders', myIP)
                    publishDC = True
                    if finished == 2 :
                        RD.smove(HASH+'.N', HASH+'.S', myIP)
                        RD.publish(HASH+'.all.channel', "*resign: "+myIP)
                    Trole = 2
        
                if Trole == 2 :
                    RD.srem(HASH+'.'+myDC+'.'+myRack+'.downloaders', myIP)
                    publishRack = True
                    if finished == 2 :
                        RD.smove(HASH+'.'+myDC+'.N', HASH+'.'+myDC+'.S', myIP)
                        publishDC = True
        
                if finished == 2 :
                    RD.smove(HASH+'.'+myDC+'.'+myRack+'.N', HASH+'.'+myDC+'.'+myRack+'.S', myIP)
        
                if publishDC : RD.publish(HASH+'.'+myDC+'.all.channel', "*resign: "+myIP)
                if publishRack : RD.publish(HASH+'.'+myDC+'.'+myRack+'.all.channel', "*resign: "+myIP)
                
            except:
                print datetime.now(), "ovrMan: Got exception after yielder loop, resignation possibly incomplete, rHost =", str(self.redis)+":"+str(rdPORT), ", is redis running?"
                print sys.exc_info()[0], sys.exc_info()[1]
                sys.stdout.flush()
    
    
        #The seeder part...
        if finished == 2:
            #still listen on channels, wait till the end of transfer is announced, or e.g. 60s passed
            global timeout
            quit = threading.Timer(timeout, post, ['*FINISHED transfer', ready, messages] )
            quit.start()
    
            print datetime.now(), "ovrMan: seeding for either", timeout, "sec or until FINISHED transfer message arrives..."
            sys.stdout.flush()
            
            
            for msg in yielder(messages, ready):        # <==> while not finished...
                if msg['type'] == 'message' : print datetime.now(), "ovrMan: recv post download message:", msg['data'] ; sys.stdout.flush() 
                if msg['data'] == '*FINISHED transfer' :
                    print datetime.now(), "ovrMan: Finished complete transfer", HASH
                    sys.stdout.flush()
                    break
                try:
                    if TR[HASH]['role'] == 1 :
                        time.sleep( random.random() / 3 )
                        curRep = RD.scard(HSH+'.'+myDC+'.'+myRack+'.uploaders')
                        if curRep == '' : curRep = 0
                        if curRep <= 2 :
                            #become R
                                diff = [ HSH+'.'+myDC+'.N' , HSH+'.'+myDC+'.'+myRack+'.N' ]
                                DC_listener = subscriber(self.redis, HSH+'.'+myDC+'.all.channel', messages, ready)
                                DC_listener.start()
                
                                #atomically join DC group
                                pipe = RD.pipeline(True)
                                pipe.sadd(HSH+'.'+myDC+'.S', myIP)
                                pipe.sadd(HSH+'.'+myDC+'.all', myIP)
                                pipe.sadd(HSH+'.'+myDC+'.'+myRack+'.uploaders', myIP)
                                pipe.sdiff(diff)
                                results = pipe.execute()
                                
                                TR[HASH]['role'] = 2                        
                                RD.publish(HSH+'.'+myDC+'.'+myRack+'.all.channel','*promoted: '+myIP)
                                
                                for peer in results.pop():
                                    if addToGroup('myDC', peer): connectToPeer(HASH, peer).start()
                                    #if not success : print "*couldn't connect to", peer
                                
                                print datetime.now(), "ovrMan: promoted to rack uploader and joined DC group", myDC
                                
                    if TR[HASH]['role'] == 2 :
                        time.sleep( random.random() / 3 )       
                        curRep = RD.scard(HSH+'.'+myDC+'.uploaders')
                        if curRep == '' : curRep = 0
                        if curRep <= 10 :
                                #become R
                                diff = [ HSH+'.'+'.N' , HSH+'.'+myDC+'.N' ]
                                rootListener = subscriber(self.redis, HSH+'.all.channel', messages, ready)
                                rootListener.start()
                
                                #atomically join DC group
                                pipe = RD.pipeline(True)
                                pipe.sadd(HSH+'.S', myIP)
                                pipe.sadd(HSH+'.all', myIP)
                                pipe.sadd(HSH+'.'+myDC+'.uploaders', myIP)
                                pipe.sdiff(diff)
                                results = pipe.execute()
                                
                                TR[HASH]['role'] = 3
                                
                                RD.publish(HSH+'.'+myDC+'.all.channel','*promoted: '+myIP)
                                RD.publish(HSH+'.'+myDC+'.'+myRack+'.all.channel','*promoted: '+myIP)   #this is for current kind-of crippled BW mgmt
                                
                                for peer in results.pop():
                                    if addToGroup('outside', peer): connectToPeer(HASH, peer).start()
                                    #if not success : print "*couldn't connect to", peer
                                    
                                print datetime.now(), "ovrMan: promoted to DC uploader and joined root group"
                                
                    update=False
                    
                    #if I am an R, then update speeds
                    if TR[HASH]['role'] >= 2:     
                        
                        uploaders = RD.scard(HASH+'.'+myDC+'.'+myRack+'.uploaders')
                        downloaders=RD.scard(HASH+'.'+myDC+'.'+myRack+'.downloaders')
                        print datetime.now(), 'spdMan: found', uploaders, 'uploaders and', downloaders, 'downloaders in my rack'
                        
                        if uploaders == 0  : uploaders = 1   #&& spit fire!!! as this should never happen
                        if downloaders== 0 : downloaders = 1 #&& spit fire!!! as this should never happen
                        
                        myUP =  speed/ uploaders
                        myDOWN= speed/ downloaders
                        
                        if myUP < 10 : myUP = 10
                        if myDOWN<10 : myDOWN=10
                        
                        myUP = myUP / 10
                        myDOWN=myDOWN/10
                        
                        myDC_UP = 0
                        myDC_DOWN=0
                        
                        update=True
                        
                    #only if I can also go out
                    if TR[HASH]['role'] == 3 :    
                        DCuploaders = RD.scard(HASH+'.'+myDC+'.uploaders')
                        DCdownloaders=RD.scard(HASH+'.'+myDC+'.downloaders')
                        print datetime.now(), 'spdMan: found', DCuploaders, 'uploaders and', DCdownloaders, 'downloaders in my DC'
                        
                        if DCuploaders == 0 :  DCuploaders = 1   #&& spit fire!!! as this should never happen
                        if DCdownloaders==0 : DCdownloaders= 1 #&& spit fire!!! as this should never happen
                        
                        myDC_UP = DCdefault / DCuploaders
                        myDC_DOWN=DCdefault/DCdownloaders
                        
                        if myDC_UP < 10 : myDC_UP = 10
                        if myDC_DOWN<10 : myDC_DOWN=10 
                        
                        myDC_UP = myDC_UP / 10
                        myDC_DOWN=myDC_DOWN/10
                        
                        #because if you want to go out of DC, you have to go out of rack - the groups are inclusive
                        if myDC_UP > myUP : myDC_UP = myUP
                        if myDC_DOWN > myDOWN : myDC_DOWN = myDOWN
                        
                        update=True
        
                    if update:
                        try:
                            TR[HASH]['outUP']  =myDC_UP
                            TR[HASH]['outDOWN']=myDC_DOWN
                            TR[HASH]['dcUP']   =myUP
                            TR[HASH]['dcDOWN'] =myDOWN
                            updateSpeeds()        
                        except KeyError, e:
                            print datetime.now(), "spdMan: Couldn't update the speeds in TR[HASH], transfer stopped? Exception: %s" % e
                            print repr(TR) 
                            sys.stdout.flush()

                        print datetime.now(), 'spdMan: dcUP/dcDOWN ; outUP/outDOWN: [', str(myUP)+"/"+str(myDOWN),';', str(myDC_UP)+"/"+str(myDC_DOWN),"] for", HASH    
                        sys.stdout.flush()
                        
                except:
                    print datetime.now(), "ovrMan: Got exception inside seeder yielder loop, rHost =", str(self.redis)+":"+str(rdPORT), ", is redis running?"
                    print sys.exc_info()[0], sys.exc_info()[1]
                    time.sleep(reconnSec)
                    print datetime.now(), "ovrMan: Attempting reconnect on next message..."
                    sys.stdout.flush()
                    continue

        print datetime.now(), "ovrMan: about to do cleanup..."
        
        #cleanup...
        try:
            if TR[HASH]['role'] == 3 :
                pipe = RD.pipeline(True)
                if finished != 2:   
                        pipe.srem(HSH+'.'+myDC+'.downloaders', myIP)
                        pipe.srem(HSH+'.N', myIP)
                else:   pipe.srem(HSH+'.S', myIP)
                pipe.srem(HSH+'.all', myIP)
                pipe.srem(HSH+'.'+myDC+'.uploaders', myIP)
                results = pipe.execute()
                TR[HASH]['role'] = 2
                rootListener.DIE = True
                RD.publish(HASH+'.all.channel', "*quit: "+myIP)
                #print results
                #print threading.enumerate()
                sys.stdout.flush()
                
            if TR[HASH]['role'] == 2 :
                pipe = RD.pipeline(True)
                if finished != 2:   
                        pipe.srem(HSH+'.'+myDC+'.'+myRack+'.downloaders', myIP)
                        pipe.srem(HSH+'.'+myDC+'.N', myIP)
                else:   pipe.srem(HSH+'.'+myDC+'.S', myIP)
                pipe.srem(HSH+'.'+myDC+'.all', myIP)
                pipe.srem(HSH+'.'+myDC+'.'+myRack+'.uploaders', myIP)
                results = pipe.execute()
                TR[HASH]['role'] = 1
                DC_listener.DIE = True
                RD.publish(HASH+'.'+myDC+'.all.channel', "*quit: "+myIP)
                #print results
                #print threading.enumerate()
                #sys.stdout.flush()
                
            pipe = RD.pipeline(True)
            if finished != 2:   pipe.srem(HSH+'.'+myDC+'.'+myRack+'.N', myIP)
            else:               pipe.srem(HSH+'.'+myDC+'.'+myRack+'.S', myIP)
            pipe.srem(HSH+'.'+myDC+'.'+myRack+'.all', myIP)
            results = pipe.execute()
            
            rack_listener.DIE = True
            RD.publish(HASH+'.'+myDC+'.'+myRack+'.all.channel', "*quit: "+myIP)
    
            if finished == 1 : RD.smove(HASH+".PENDING", HASH+".FINISHED", myHOST)
            if finished == 0 : RD.smove(HASH+".PENDING", HASH+".CORRUPT", myHOST)
        except:
            print datetime.now(), "ovrMan: Got exception in the final cleanup, which is thus possibly incomplete, rHost =", str(self.redis)+":"+str(rdPORT), ", is redis running?"
            print sys.exc_info()[0], sys.exc_info()[1]
            sys.stdout.flush()
            
        print datetime.now(), "ovrMan: ***FINISHED*** ", HASH
        sys.stdout.flush()
        return

def listener():
    ''' Just a connManager "wrapper" : listens on the main PORT and for every incoming connection spawns connManager thread which then handles the requests... '''
    
    print datetime.now(), "listener: About to listen on", PORT
    while True:
        socket, address = s.accept()
        connManager(socket, address).start()

def rGetMessage(HASH):
    ''' Get the rtorrent message for given HASH '''
    
    global rLock
    
    rLock.acquire()
    try:
        err = rtorr.d.get_message(HASH)
    except:
        print datetime.now(), "rGetMessage: Error reading message from rTorrent, is transfer started?", HASH, sys.exc_info()
        sys.stdout.flush()
        err = 255       #TODO: consider sending some different object as err - e.g. an exception?
    finally:
        rLock.release()
        
    return err

class announceWaiter(threading.Thread):
    ''' This thread waits until it can announce the start of the transfer to all the peers...
    '''
    
    def __init__(self, HASH, destPath, torrentPath, md5Path, speed, priority, localTorrentPath):
        self.HASH = HASH
        self.dest = destPath
        self.torr = torrentPath
        self.md5 = md5Path
        self.spd = speed
        self.prio = priority
        self.ltp = localTorrentPath 
        threading.Thread.__init__(self)
        self.setName("announceWaiter")

    def run(self):
        global TR
        
        #global targets
        #global announceEvent
        
        print datetime.now(), "annWait: Waiting..."
        sys.stdout.flush() 
        TR[self.HASH]['announceEvent'].wait()
        if TR[self.HASH]['annDie']:
            TR[self.HASH]['annDie'] = False 
            return
        
        print datetime.now(), "annWait: Announcing..."
        sys.stdout.flush()
        
        for a in TR[self.HASH]['targets']:
            announceTransfer(a, self.HASH, self.dest, self.torr, self.md5, self.spd, self.prio, self.ltp).start()        


class transferWrapper(threading.Thread):
    ''' Initializes the transfer: retrieves torrent file, loads it to rtorrent, checks if all is OK, starts monitoring & speed manager threads
        and then overlay manager thread. Waits until the transfer is done and then does the cleanup: closes and removes the transfer from rtorrent,
        "stops" (orders them to DIE) monitoring & spdMan threads, removes temporary files.    '''
    
    def __init__(self, HASH, Rhost, torrentPath, destPath, md5Path, init, destRemote, speed, priority = 1, restart = False):
        self.HASH = HASH
        self.torr = torrentPath
        self.md5 = md5Path
        self.path = destPath
        self.init = init
        self.Rhost = Rhost
        self.md5 = md5Path
        self.remPath = destRemote
        self.speed = speed
        self.prio = priority
        self.rst = restart
        threading.Thread.__init__(self)
        self.setName("transferWrapper")

    def run(self):
        global TR
        global TRlock
        global rLock
        global monitoringInterval
        global max_file_size
        
        #global busy
        global transfers
        
        HASH = self.HASH
        init = self.init
        seeder = False
        Rhost = self.Rhost
        path = self.path
        md5 = False
        failure = False
        err = 255
        
        try:
            hashes = TRkeys()
            a = hashes.index(HASH)
            working = True
        except:
            working = False
            TR[HASH]= {}
            TR[HASH]['restart'] = False
            
                        
        if working == True:
            try:
                a = TR[HASH]['restart']
                if a:
                    print datetime.now(), "trWrap: ALREADY RESTARTING", HASH, "!!! wait a bit and then try again, ONE RESTART AT THE TIME!!!"
                    sys.stdout.flush()
                    return
            except:
                TR[HASH]['restart'] = True
            
            print datetime.now(), "trWrap: Restarting transfer", HASH, "with new parameters..."
            sys.stdout.flush()
            TR[HASH]['restart'] = True
            
            #if the manager is running already, just post the *FINISHED transfer message & wait till it ends the tr.
            #and if it's not running, then setting 'restart' to True should be enough
            try:
                a = TR[HASH]['mgr']
                post('*FINISHED transfer', TR[HASH]['mgr'].ready, TR[HASH]['mgr'].messages)
            except:
                pass
                
            #wait till it stops (that is, when the TR[HASH] key is deleted)
            stopped = False
            while not stopped:
                time.sleep(random.random() / 3)
                try:
                    x = TR[HASH]
                except:
                    stopped = True
          
            #and now we can go on...
            print datetime.now(), "trWrap: Transfer", HASH, "stopped, now starting again..."
            TR[HASH]= {}
            TR[HASH]['restart'] = False
            sys.stdout.flush()
        
    
        TR[HASH]['working'] = True
        start = time.time()
        
        if not self.rst:
            
            if init == False:
                
                #THE PYTHON WAY to comment out a block of code:
                something = False
                if something:
                    #I suppose I am in the sessionDir
                    #TODO: make sure you don't deadlock/freeze here
                    getTorr = subprocess.Popen(["scp", "-oStrictHostKeyChecking=no",  self.torr, "."], stdout=open('./scp.out', 'w'), stderr=open('./scp.err', 'w'))
                    code = getTorr.wait()
                    if code != 0:
                        print datetime.now(), "trWrap: Couldn 't get torrent file, scp return code =", code
                        sys.stdout.flush()
                        TRlock.acquire()
                        try:
                            del TR[HASH]
                        except:
                            print datetime.now(), "trWrap: Error doing del TR[HASH]: ", sys.exc_info()[0], sys.exc_info()[1]
                            sys.stdout.flush()
                        finally:
                            print datetime.now(), "trWrap: deleted TR["+str(HASH)+"]"
                            sys.stdout.flush()
                            TRlock.release()
                            return                  #no torrent file = nothing to download!
                    
                    print datetime.now(), "trWrap: got .torrent file"
                    sys.stdout.flush()
                    
                    getMd5 = subprocess.Popen(["scp", "-oStrictHostKeyChecking=no", self.md5, "."], stdout=open('./md5.out', 'w'), stderr=open('./md5.err', 'w'))
                    code = getMd5.wait()
                    md5 = True
                    if code != 0:
                        print datetime.now(), "trWrap: Couldn't get md5 file, scp return code =", code
                        sys.stdout.flush()
                        md5 = False
                        #no return, but we'll fail md5 check
                    
                    print datetime.now(), "trWrap: got .md5 file"
                    sys.stdout.flush()
                
                
                #TODO: test if rtorrent wouldn't create it himself anyway
                if os.path.exists(path) == False:
                    mkdir = subprocess.Popen(["mkdir", "-p", path], stdout=open('./mkdir.out', 'w'), stderr=open('./mkdir.err', 'w'))
                    code = mkdir.wait()
                    if code != 0:
                        print datetime.now(), "trWrap: Couldn't mkdir", path, "; return code =", code
                        sys.stdout.flush()
                        failure = True
                
                print datetime.now(), "trWrap: created dest. directory"
                sys.stdout.flush()
                    
                tmp = self.torr.split('/')
                tmp.reverse()
                torrent = WorkDir+"/"+tmp[0]
                print datetime.now(), "trWrap: torrent =", torrent
                sys.stdout.flush()
            
            if init == True : 
                torrent = self.torr
                TR[HASH]['targets'] = None
                TR[HASH]['announceEvent']= threading.Event()
                TR[HASH]['announceEvent'].clear()
                TR[HASH]['annDie'] = False
                announceWaiter(HASH, self.remPath, str(myHOST)+':/'+self.torr+".torrent", str(myHOST)+':/'+self.md5, self.speed, self.prio, self.torr+".torrent").start()
                torrent = self.torr+"_FAST.torrent"
            
            print datetime.now(), "trWrap: starting transfer", torrent, "to", path, "and am I init?", init
            sys.stdout.flush()
            
            #OPEN:
            try:
                err = RTex(rtorr.load, torrent)
            except:
                print datetime.now(), "trWrap: Error loading/opening", torrent, ":", sys.exc_info()[1]
                sys.stdout.flush()
                #busy = False
                failure = True
                #return
            if err != 0:
                print datetime.now(), "trWrap: Error", err, "when trying to open", torrent
                #busy = False
                failure = True
                #return
            #print rGetMessage(HASH)
            print datetime.now(), "trWrap: loaded", torrent
            sys.stdout.flush()
            
            
            #because of bug/implementation in/of xmlrpc2scgi.py, there's no way to use methods which have two . in their name + 2 parameters via wrapper...
            #it has to be called directly only...
            
            #SET PATH:
            rLock.acquire()
            try:
                err = rtorr.d.set_directory_base(HASH, path)        #d.set_directory_base
            except:
                print datetime.now(), "trWrap: Error setting path", path, ":", sys.exc_info()[1]
                sys.stdout.flush()
                #busy = False
                failure = True
                #return
            finally:
                rLock.release()
            if err != 0:
                print datetime.now(), "trWrap: Error", err, "when trying to set path", path, "for", torrent
                print rGetMessage(HASH)
                #busy = False
                failure = True
                #return
            #print rGetMessage(HASH)
            print datetime.now(), "trWrap: path set to", path
            sys.stdout.flush()
            
            #SET max_file_size
    #        try:
    #            err = RTex( rtorr.d.set_max_file_size, (HASH, max_file_size) )
    #        except:
    #            print datetime.now(), "trWrap: Error setting max_file_size", path, sys.exc_info()
    #            sys.stdout.flush()
    #            print rGetMessage(HASH)
    #            err = 255
    #        if err != 0:
    #            print datetime.now(), "trWrap: Error", err, "when trying to set max_file_size to", max_file_size, "for", torrent
    #            return
            #print rGetMessage(HASH)
    #        print datetime.now(), "trWrap: max_file_size set to", max_file_size
    #        sys.stdout.flush()
        else:
            print datetime.now(), "trWrap: re-starting already present transfer", HASH
            sys.stdout.flush() 
            torrent = "Re-started "+str(HASH)

        
        #START:
        try:
            err = RTex(rtorr.d.start, HASH)
        except:
            print datetime.now(), "trWrap: Error starting", torrent, ":", sys.exc_info()[1]
            sys.stdout.flush()
            print rGetMessage(HASH)
            #busy = False
            failure = True
            #return
        if err != 0:
            print datetime.now(), "trWrap: Error", err, "when trying to start", torrent
            sys.stdout.flush()
            #busy = False
            failure = True
            #return
        #print rGetMessage(HASH)
        print datetime.now(), "trWrap: started", HASH
        sys.stdout.flush()
         
        #WAIT for hashcheck:
        now = time.time()
        while 1:
            try:
                err = RTex(rtorr.d.is_hash_checked, HASH)
            except:
                print datetime.now(), "trWrap: Error waiting for HASH check on", torrent, ":", sys.exc_info()[1]
                sys.stdout.flush()
                print rGetMessage(HASH)
                #busy = False
                failure = True
                break
                #return
            if err == 1: break
            if err > 1:
                print datetime.now(), "trWrap: Error", err, "when waiting for HASH check on", torrent
                #busy = False
                failure = True
                #return
            if TR[HASH]['restart']: break
            
            #wait a bit, then check again if DONE
            time.sleep(random.random() / 3)
            
        done = time.time()
        print datetime.now(), "trWrap: The hash check for", torrent, "took", str(done - now), "seconds"
        #print rGetMessage(HASH)
        sys.stdout.flush()
        
        #CHECK if failed hashing    [e.g. when the path is wrong!!!]
        try:
            err = RTex(rtorr.d.get_hashing_failed, HASH)
        except:
            print datetime.now(), "trWrap: Error checking failed hashing", torrent, ":", sys.exc_info()[1]
            sys.stdout.flush()
            print rGetMessage(HASH)
            #busy = False
            failure = True
            #return
        if err == 1:
            print datetime.now(), "trWrap: Failed hashing", torrent
            #busy = False
            failure = True
            #return
        elif err >1:
            print datetime.now(), "trWrap: Error", err, "when checking failed hashing", torrent
            #busy = False
            failure = True
            #return
        #print rGetMessage(HASH)
        print datetime.now(), "trWrap: HASH check succeeded", torrent
        sys.stdout.flush()
        
        #CHECK if the complete file is present = if I am a seeder
        try:
            err = RTex(rtorr.d.get_complete, HASH)
        except:
            print datetime.now(), "trWrap: Error when d.get_complete", torrent, ":", sys.exc_info()[1]
            sys.stdout.flush()
            print rGetMessage(HASH)
            #busy = False
            failure = True
            #return
        if err == 1:    seeder = True
        elif err >1: 
            print datetime.now(), "trWrap: Error", err, "when trying d.get_complete", torrent
            #busy = False
            failure = True 
            #return
        #print rGetMessage(HASH)
        print datetime.now(), "trWrap: Have complete", torrent, "?", seeder
        sys.stdout.flush()
         
        if init == True and seeder == False:
            print datetime.now(), "trWrap: I am INIT seeder, but I don't have complete file!"
            sys.stdout.flush()
            #TODO: sys.exit(255)? return? FAIL?
        
        #SET priority
        rLock.acquire()
        try:
            err = rtorr.d.set_priority(HASH, self.prio)
            err = rtorr.d.update_priorities(HASH)
        except:
            print datetime.now(), "trWrap: Error setting priority for", path, "to", self.prio, ":", sys.exc_info()[0], sys.exc_info()[1]
            sys.stdout.flush()
            #busy = False
            #failure = True
            #return
        finally:
            rLock.release()
        if err != 0:
            print datetime.now(), "trWrap: Error", err, "when trying to set priority", self.prio, "for", torrent
            print rGetMessage(HASH)
            #busy = False
            #failure = True
            #return
        
        try:        
            if TR[HASH]['restart'] and init:
                    TR[HASH]['annDie'] = True
                    TR[HASH]['announceEvent'].set()
                    #wait until announcer really dies
                    while not TR[HASH]['annDie'] : time.sleep(random.Random()/3 )                
        except:
            print datetime.now(), "trWrap: Couldn't get TR[HASH]['restart']"
            print datetime.now(), "trWrap: TR=", repr(TR)
            
            #THIS IS SERIOUS FLAW, how can I all of a sudden loose my own variable?
            failure = True
            #return
            
        
        if not( TR[HASH]['restart'] ) and not(failure):     
            #START the speeds manager
            #busy = True
            print datetime.now(), "trWrap: Starting speeds manager..."
            sys.stdout.flush()
            
            TR[HASH]['speedEvent']= threading.Event()
            TR[HASH]['speedEvent'].clear()
            TR[HASH]['speed']= self.speed
            spd = speedManager(Rhost, HASH)
            spd.start()
            
            #START the statistics thread
            print datetime.now(), "trWrap: about to start trStats for", HASH 
            sys.stdout.flush()
            
            stats = transferStatistics(Rhost, myHOST, HASH, monitoringInterval)     
            stats.start()
            
            #TODO: use locks here!
            transfers += 1
            
            #START the appropriate overlay manager
            global mgr
            if init == True:  
                TR[HASH]['mgr'] = initOverlayManager(HASH, Rhost)
                TR[HASH]['mgr'].start()
            else :  
                TR[HASH]['mgr'] = overlayManager(HASH, Rhost, seeder)
                TR[HASH]['mgr'].start()
            
            #WAIT until it's done
            print datetime.now(), "trWrap: waiting for transfer manager to finish...", HASH 
            sys.stdout.flush()
            TR[HASH]['mgr'].join()        
            transfers -= 1
    
            print datetime.now(), "trWrap: trMan finished, of", HASH 
            sys.stdout.flush()
            #STOP the statistics and speed manager threads
            stats.DIE = True
            spd.DIE = True
            
            print datetime.now(), "trWrap: trStats and spdMan ordeted to die for", HASH 
            sys.stdout.flush()
            
            #update doneB in statistics...
            try:
                doneB = RTex(rtorr.d.get_bytes_done, HASH)
                R = redis.Redis(host = self.Rhost, port = rdPORT, db=0)
                R.set(HASH+".doneB."+myHOST, doneB)
            except:
                print datetime.now(), "trWrap: failed to update final doneB for", HASH, ", Exception:", sys.exc_info()[0], sys.exc_info()[1] 
                sys.stdout.flush()
                
            #print rGetMessage(HASH)
            print datetime.now(), "trWrap: transfer DONE for", torrent
            sys.stdout.flush()
        

        #STOP:
        try:
            err = RTex(rtorr.d.close, HASH)
        except:
            print datetime.now(), "trWrap: Error closing", torrent, ":", sys.exc_info()[1]
            sys.stdout.flush()
            print rGetMessage(HASH)
            #busy = False
            failure = True
            #return
        if err != 0:
            print datetime.now(), "trWrap: Error", err, "when trying to close", torrent
            #busy = False
            failure = True
            #return
        #print rGetMessage(HASH)
        print datetime.now(), "trWrap: closed", torrent
        sys.stdout.flush()
    
        #DELETE:            #remove the torrent from rtorrent downloads list, delete the torrent file
        try:
            err = RTex(rtorr.d.erase, HASH)
        except:
            print datetime.now(), "trWrap: Error closing", torrent, ":", sys.exc_info()[1]
            sys.stdout.flush()
            print rGetMessage(HASH)
            #busy = False
            failure = True
            #return
        if err != 0:
            print datetime.now(), "trWrap: Error", err, "when trying to close", torrent
            #busy = False
            failure = True
            #return
        print datetime.now(), "trWrap: erased", torrent
        sys.stdout.flush()
        
        #emit final FINISH message [maybe with some statistics?]
        finish = time.time()
        duration = finish - start
        print datetime.now(), "trWrap: ***FINISHED in", duration, "sec ***" #ETC. some more stuff  
        sys.stdout.flush()
        
        print datetime.now(), "trWrap: md5 path is", self.md5
        sys.stdout.flush()
        
        tmp = self.md5.split('/')
        tmp.reverse()
        md5path = WorkDir+"/"+tmp[0]
        try: os.remove(md5path)
        except:
            print datetime.now(), "trWrap: Error removing md5 file", self.md5, ":", sys.exc_info()
        
        TRlock.acquire()
        try:
            del TR[HASH]
        except:
            print datetime.now(), "trWrap: Error doing del TR[HASH]: ", sys.exc_info()[0], sys.exc_info()[1]
            sys.stdout.flush()
        finally:
            print datetime.now(), "trWrap: deleted TR["+str(HASH)+"]"
            sys.stdout.flush()
            TRlock.release()
        
        if failure:
            try:
                os.remove(self.torr)
                print datetime.now(), "trWrap: deleted", self.torr
            except:
                print datetime.now(), "trWrap: Couldn't delete", self.torr
                pass
        
        updateSpeeds()
        #for a in TR[HASH]:             #... doesn't work, because "RuntimeError: dictionary changed size during iteration"
            #del TR[HASH][a]            # so i'll leave it just to the GC to deal with
            
        return
 
 
class transferListener(threading.Thread):
    ''' This thread is subscribed on the redis channel and is listening for the orders to do transfers... '''
    
    def __init__(self, Rhost, channel):
        self.Rhost = Rhost
        self.channel = channel        
        self.DIE = False
        threading.Thread.__init__(self)
        self.setName("transferListener")

    def run(self):      
        R = redis.Redis(host=self.Rhost, port=rdPORT, db=0)
        
        print datetime.now(), "trListener: Starting, message source", str(self.Rhost)+":"+str(rdPORT)+", channel", self.channel
        sys.stdout.flush()
        
        while True:
            try:
                R.subscribe(self.channel)
            except:
                print datetime.now(), "trListener: Error establishing connection to", str(self.Rhost)+":"+str(rdPORT), "and subscribing to", self.channel
                print sys.exc_info()
                time.sleep(reconnSec)
                print datetime.now(), "trListener: Reconnecting..."
                sys.stdout.flush()
                continue
                
            try:
                print datetime.now(), "trListener: Connected to", str(self.Rhost)+":"+str(rdPORT)
                sys.stdout.flush()
                for msg in R.listen():
                    #print datetime.now(), 'trListener: got msg', msg
                    #sys.stdout.flush()
                    try:
                        if msg['type'] == "subscribe" : continue
                        print datetime.now(), 'trListener: got msg', msg['data']
                        
                        #process the message here, start the appropriate wrappers, threads etc.
                        tmp = msg['data']
                        parse = tmp.split(' ')
                        
                        #print datetime.now(), "trListener: parsed", parse
                        #sys.stdout.flush()
                        
                        if parse[0] == 'TRANSFER' :
                            #TODO: check if message format is OK
                            HASH = parse[1]
                            Rhost = parse[2]
                            destPath = parse[3]
                            torrentPath = parse[4]
                            md5Path = parse[5]
                            speed = int(parse[6]) 
                            try:
                                priority = parse[7]
                            except:
                                priority = 2
     
                            transferWrapper(HASH, Rhost, torrentPath, destPath, md5Path, False, '', speed, priority).start()
                            
                            continue
                        
                        elif str(parse[0]).upper() == 'STOPALL' :
                            try:
                                keys = TRkeys()
                                for HASH in keys:
                                    post('*FINISHED transfer', TR[HASH]['mgr'].ready, TR[HASH]['mgr'].messages)
                            except:
                                print datetime.now(), "rListen: not doing transfer", HASH
                                sys.stdout.flush()
                        
                            continue
                    except:
                        print datetime.now(), "trListener: error processing", msg['data']
                        print datetime.now(), "trListener: exception:", sys.exc_info()
                        sys.stdout.flush()
                            
            except:
                print datetime.now(), "trListener: Error while reading messages from", str(self.Rhost)+":"+str(rdPORT), "on", self.channel
                print sys.exc_info()
                print datetime.now(), "trListener: Reconnecting..."
                sys.stdout.flush()
                time.sleep(reconnSec)       #this is probably a connection error, so just wait a bit and then try again....
    
class transferStatistics(threading.Thread):
    ''' This thread lives along with the transfer and "prints out" (in reality "uploads" to redis) some statistics.
        If the speed is 0, it will print the last message from rtorrent, which can be helpful in determining why 
        the download is stuck - e.g. out of disk space or similar. Any tracker messages are irrelevant, as this script is
        in fact a tracker and all other trackers in rtorrent (normal, DHT, local peer discovery) are disabled. 
    '''
    #TODO: templates support
    
    def __init__(self, Rhost, myIP, HASH, interval):
        self.hsh = HASH
        self.IP = myIP
        self.Rhost = Rhost
        self.int = interval
        self.DIE = False 
        threading.Thread.__init__(self)
        self.setName("transferStatistics")

    def run(self):
        global keepalive
        R = redis.Redis(host = self.Rhost, port = rdPORT, db=0)
        HASH = self.hsh
        interval = self.int
        myIP = myHOST
        
        print datetime.now(), "trStats: starting for", HASH 
        sys.stdout.flush()

        try:
            R['init']=self.getName()
        except:
            print datetime.now(), "trStats: Error establishing redis connection to", self.Rhost
            print sys.exc_info()
        
        transfers = False
        
        pipe = R.pipeline(False)    #the main pipe to submit statistics...
        start = time.time()
        while not(self.DIE):
            
            sleeptime = interval - time.time() + start
            if sleeptime <= 0:
                print datetime.now(), "trStats: Failed to retrieve all statistics within the interval of", interval, "sec"
                sys.stdout.flush()
            else:
                time.sleep(sleeptime)
            
            if self.DIE: continue
            
            try:
                connPeers = RTex(rtorr.d.get_peers_connected, HASH)
                doneB = RTex(rtorr.d.get_bytes_done, HASH)
                
                myDCT = RTex(rtorr.get_throttle_down_max, 'myDC')
                myDC  = RTex(rtorr.get_throttle_down_rate, 'myDC')
                myDCTU= RTex(rtorr.get_throttle_up_max, 'myDC')
                myDCU = RTex(rtorr.get_throttle_up_rate, 'myDC')
                
                myRackT = RTex(rtorr.get_throttle_down_max, 'myRack')
                myRack  = RTex(rtorr.get_throttle_down_rate, 'myRack')
                myRackTU= RTex(rtorr.get_throttle_up_max, 'myRack')
                myRackU = RTex(rtorr.get_throttle_up_rate, 'myRack')
                
                outsideT =RTex(rtorr.get_throttle_down_max, 'outside')
                outside  =RTex(rtorr.get_throttle_down_rate, 'outside')
                outsideTU=RTex(rtorr.get_throttle_up_max, 'outside')
                outsideU =RTex(rtorr.get_throttle_up_rate, 'outside')
                
                speeds=myDC + myRack + outside + myDCU + myRackU + outsideU
                
            except:
                print datetime.now(), "trStats: Error retrieving data from rtorrent: ", sys.exc_info()[0],":",sys.exc_info()[1]
                sys.stdout.flush()
                speeds = 0
            
            if speeds == 0: 
                transfers = False
                if connPeers == 0:      #if i'm stuck & have no peers, I should reconnect
                    TR[HASH]['mgr'].reconnect = True
            else: transfers = True
            
            if transfers == True:
                print datetime.now(), "trStats:", doneB, "B done for", HASH
                print datetime.now(), "trStats: (Rack/DC/out) DOWN spd:", myRack, '/', myDC, '/', outside, 'DOWN Thr: ', myRackT, '/' , myDCT, '/', outsideT
                print datetime.now(), "trStats: (Rack/DC/out) UP   spd:", myRackU,'/', myDCU,'/', outsideU,'UP   Thr: ', myRackTU,'/' , myDCTU, '/',outsideTU
    
                #upload some statistics
                try:
                    pipe.set(HASH+".speeds."+myIP, speeds)
                    pipe.set(HASH+".doneB."+myIP, doneB)
                    pipe.expire(HASH+".speeds."+myIP, int(keepalive))
                    rslt = pipe.execute()
                    #print datetime.now(), "trStats: got update result:", rslt
                    #sys.stdout.flush()
                    if rslt != [True, True, True]: raise Exception('redis results not all true: '+repr(rslt))
                except:
                    print datetime.now(), "trStats: Error pushing new data to", self.Rhost
                    print sys.exc_info()[0],":",sys.exc_info()[1]
                    sys.stdout.flush()
            else:
                try:
                    #TODO: don't post rtorrent message if it is tracker related
                    pipe.set(HASH+".speeds."+myIP, rGetMessage(HASH))
                    pipe.expire(HASH+".speeds."+myIP, int(keepalive))
                    rslt = pipe.execute()
                    #print datetime.now(), "trStats: got update result:", rslt
                    #sys.stdout.flush()
                    if rslt != [True, True]: raise Exception('redis results not all true: '+repr(rslt))
                except:
                    print datetime.now(), "trStats: Error pushing new data to", self.Rhost
                    print datetime.now(), "trStats: OR problems wirh getting message from rtorrent for", HASH
                    print sys.exc_info()[0],":",sys.exc_info()[1]
                    sys.stdout.flush()

            start = time.time()     #starting time of the next loop
        
        R.expire(HASH+".doneB."+myIP, int(keepalive))
        print datetime.now(), "trStats: STOP monitoring", HASH 
        sys.stdout.flush()

def throttles():
    global transfers
    global myUP
    global myDOWN
    curOutUpLast = 0
    curOutDownLast = 0
    
    while True:
        if transfers > 0:
            #get current speeds:            
            try:
                curOutUp   = RTex(rtorr.get_throttle_up_rate,   'outside')
                curOutDown = RTex(rtorr.get_throttle_down_rate, 'outside')
            except:
                curOutUp   = curOutUpLast
                curOutDown = curOutDownLast
                print datetime.now(), "throttles: Error getting current outside throttle up/down rates:", sys.exc_info()
                sys.stdout.flush()
            
            Urate=(myUP   * 1024 * 1024 - curOutUp)   / 1024
            Drate=(myDOWN * 1024 * 1024 - curOutDown) / 1024 
            
            if Urate < 1000 : Urate=1000
            if Drate < 1000 : Drate=1000

            print datetime.now(), "throttles: about to set up/down to", str(Urate * 1024)+"/"+str(Drate * 1024) , "[ max", str(myUP * 1024 * 1024)+"/"+str(myDOWN * 1024 * 1024), ", current outside", str(curOutUp)+"/"+str(curOutDown), "]"
            sys.stdout.flush()

            curOutUpLast = curOutUp
            curOutDownLast=curOutDown
            
            try:
                err1 = RTex(rtorr.throttle_up,   ('myDC', str(Urate)))
                err2 = RTex(rtorr.throttle_down, ('myDC', str(Drate)))
            except:
                err1 = 0
                err2 = 0
                print datetime.now(), "throttles: Error setting current outside throttle up/down rates:", sys.exc_info()
                sys.stdout.flush()
            if err1 != 0 or err2 != 0:
                print datetime.now(), "throttles: Error setting current outside throttle up/down rate, codes=", str(err1), str(err2)
                sys.stdout.flush()

        else:
            pass
        time.sleep(reconnSec)

def feedBackPipe():
    global transfers
    global looptime
    
    print datetime.now(), "fbkPipe: Trying to open", str(WorkDir)+'/feedback.pipe'
    sys.stdout.flush()
    
    open = False
    while not open:
        try:
            #fbkPipe = open(WorkDir+'/feedback.pipe', 'w')
            os.remove(str(WorkDir)+'/feedback.pipe')
        except:
            print datetime.now(), "fbkPipe: Failed to delete", str(WorkDir)+'/feedback.pipe', sys.exc_info()
            sys.stdout.flush()
            #maybe it didn't exist already?
        
        try:
            #fbkPipe = open(WorkDir+'/feedback.pipe', 'w')
            os.mkfifo(str(WorkDir)+'/feedback.pipe')
        except:
            print datetime.now(), "fbkPipe: Failed to mkfifo", str(WorkDir)+'/feedback.pipe', sys.exc_info()
            sys.stdout.flush()
            #maybe it exists already?
            
        
        try:
            fbkPipe = os.open(str(WorkDir)+'/feedback.pipe', os.O_WRONLY)
            open = True
        except:
            print datetime.now(), "fbkPipe: Failed to open", str(WorkDir)+'/feedback.pipe', sys.exc_info()
            sys.stdout.flush()
            return
            #time.sleep(reconnSec)
            
    
    print datetime.now(), "fbkPipe: Opened", str(WorkDir)+'/feedback.pipe'
    sys.stdout.flush()
    
    while True:
        if transfers > 0 :
            print datetime.now(), "fbkPipe: writing pulse..."
            sys.stdout.flush()
            written = os.write(fbkPipe, 'pulse')
            print datetime.now(), "fbkPipe: wrote", written, "Bytes"
            sys.stdout.flush()
            #print >>fbkPipe, "pulse"
            #fbkPipe.flush()
        else:
            pass
        
        time.sleep(looptime)

def initRestart():
    ''' This tries to re-start all the transfers that are present in the rtorrent upon start.
        It is a best-effort only function, if there is any trouble along the way, the restart is aborted.
    '''
    connected = False
    
    restarted=list()
    
    print datetime.now(), "initRst: about to do get present torrents..."
    
    while not connected:
        try:
            R = redis.Redis(host=Rmain, port=rdPORT, db=0)
            R['init']='test'
            connected = True
        except:
            print datetime.now(), "initRst: trouble connecting to", str(Rmain)+":"+str(rdPORT), ", reconnecting..."
            sys.stdout.flush()
            time.sleep(reconnSec)
    
    try:
        views = rtorr.view_list()
        if views == '': 
            print datetime.now(), "initRst: got empty views list!"
            sys.stdout.flush()
            return
        
        #print datetime.now(), "initPurge: views =", views    
        #sys.stdout.flush()
        
        for view in views:
            hashes = rtorr.d.multicall(view, "d.get_hash=")
            #print datetime.now(), "initPurge: got hashes for view =", view, ":", hashes    
            #sys.stdout.flush()
            for hsh in hashes:
                a = hsh.pop()
                try:
                    restarted.index(a)
                except:
                    restarted.append(a)
                    #print "a =", a
                    print datetime.now(), "initRst: restarting", a
                    sys.stdout.flush()
                    try:     
                        Rhost = R[str(a)+'.INIT']
                        speed = R[str(a)+'.SPEED']
                        priority = R[str(a)+'.PRIORITY']           
                        transferWrapper(a, Rhost, '', '', '', False, '', speed, priority, True).start()
                    except:
                        print datetime.now(), "initRst: trouble getting enough information to restart", a
                        sys.stdout.flush()
    
    except:
        print datetime.now(), "initRst: exception when restarting:", sys.exc_info()
        sys.stdout.flush()
        return
    
#    rLock.release()
 
    return   


def initPurge():
    ''' This cleans up all running/open torrents from rtorrent, if there are any. 
        This function is only called at the start, when there's no information about 
        those transfers, thus there are possible bandwidth leaks... (those transfers run unmanaged). 
    '''
    
    print datetime.now(), "initPurge: about to do cleanup..."

#in fact, I don't need locks here, since i'm the only one running...
#    rLock.acquire()
    
    try:
            
        views = rtorr.view_list()
        if views == '': 
            print datetime.now(), "initPurge: got empty views list!"
            sys.stdout.flush()
            return
        
        #print datetime.now(), "initPurge: views =", views    
        #sys.stdout.flush()
        
        for view in views:
            hashes = rtorr.d.multicall(view, "d.get_hash=")
            #print datetime.now(), "initPurge: got hashes for view =", view, ":", hashes    
            #sys.stdout.flush()
            for hsh in hashes:
                a = hsh.pop()
                #print "a =", a
                print datetime.now(), "initPurge: purging", a
                sys.stdout.flush()
                
                err = rtorr.d.close(a)
                if err != 0 : 
                    print datetime.now(), "initPurge: Error closing", a, ":", err 
                    sys.stdout.flush()
                    print rGetMessage(a)
                #else:
                    #print datetime.now(), "initPurge: closed", a 
                    #sys.stdout.flush()
                    
                err = rtorr.d.erase(a)
                if err != 0 : 
                    print datetime.now(), "initPurge: Error erasing", a, ":", err 
                    sys.stdout.flush()
                    print rGetMessage(a)
                #else:
                    #print datetime.now(), "initPurge: deleted", a 
                    #sys.stdout.flush()
    
    except:
        print datetime.now(), "initPurge: exception when purging:", sys.exc_info()
        sys.stdout.flush()
        return
    
#    rLock.release()
 
    return   

def cleanup(signum, frame):
    print datetime.now(), "MAIN: Got signal", signum, ", doing cleanup..."
    sys.stdout.flush()
    try:
        os.remove(MGMT)
    except:
        sys.exit("Couldn't remove "+str(MGMT))     #1 = dirty, no cleanup
    
    sys.exit(0)
    print datetime.now(), "MAIN: YOU SHOULD NEVER SEE THIS!"
    sys.stdout.flush()
    return




#TODO:
# be able to start with transfers already running: service restart will restart only dtrack
# for the transfers running already, try to re-join the overlay? look for your own information...

#TODO: finish initRestart testing


#MAIN:
try:
    signal.signal(signal.SIGTERM, cleanup)
    
    #first close possibly all active downloads - since we have no information about them and they can cause trouble...
    initPurge()
    #initRestart()
   
    print datetime.now(), "MAIN: Starting threads..."
    sys.stdout.flush()
    
    listen = threading.Thread(None, listener)
    listen.start()
    thrttls = threading.Thread(None, throttles)
    thrttls.start()
    
    #NO need for feedbackpipe since the manage_local_rate.py will be pythonized = a thread here in this application
    #feedPipe = threading.Thread(None, feedBackPipe)
    #feedPipe.start()
    
    rtorrListener(MGMT).start()
    transferListener(Rhost, "Transfers.channel").start()
    transferListener(Rhost, str(myCHNL)).start()
    
    while True:         #this thing is here just that the main keeps on running and can thus trap signals
        time.sleep(1)
        
except SystemExit:
    raise    
except:
    print datetime.now(), 'MAIN: caught exception: ' #, sys.exc_info()[0], sys.exc_info()[1]
    traceback.print_exc(10, file=sys.stdout)