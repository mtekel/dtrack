#!/usr/bin/env python
#"echo" your commandline to the argv[1] socket & get reply
import socket, sys

message = ' '.join(sys.argv[2:])

s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sys.argv[1])
s.send(message)
data = s.recv(1024)
s.close()
print str(data)