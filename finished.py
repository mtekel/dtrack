#!/usr/bin/env python
import socket, sys, subprocess

HASH = sys.argv[2]
md5 = subprocess.Popen(["md5sum", "-c", HASH+".md5"], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
code = md5.wait()

message = "HASH "+HASH+" "
if   code == 0: message +="OK"
elif code == 1: message +="CORRUPT"
else: message += "UNKOWN"

s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sys.argv[1])
s.send(message)
s.close()