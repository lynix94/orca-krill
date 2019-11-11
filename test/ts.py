import os, sys, time
import redis

r = redis.StrictRedis(host='127.0.0.1', port=6379)

#
# ts
#
r.delete('stream')
r.delete('ts:stream')


for i in range (100):
	print r.xadd('ts:stream', {'a':i, 'b':i})

print r.xlen('ts:stream')
print r.xrange('ts:stream')




