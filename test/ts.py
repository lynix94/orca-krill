import os, sys, time
import redis

r = redis.StrictRedis(host='127.0.0.1', port=6379)



#
# stream
#

r.delete('stream')

for i in range (1000):
	print r.xadd('stream', {'a':0, 'b':1})

while True:
	print len(r.xrange('stream', ))


sys.exit(0)



