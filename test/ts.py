import os, sys, time
import redis
import socket;

r = redis.StrictRedis(host='127.0.0.1', port=6379)

#
# ts
#
r.delete('ts:cpu.user')
r.delete('ts:cpu.sys')
r.delete('ts:cpu.idle')


for i in range (100):
	print r.xadd('ts:cpu.user', {'a':i*3}, id='%d'%(i*1000))

for i in range (90):
	print r.xadd('ts:cpu.sys', {'a':i*2}, id='%d'%(i*1000))

for i in range (10, 100):
	print r.xadd('ts:cpu.idle', {'a':i}, id='%d'%(i*1000))

print r.xrange('ts:cpu.user')


s = socket.socket()
s.connect(('127.0.0.1', 6379))
s.sendall('tsrange 0 1000000 ts:cpu.user ts:cpu.sys ts:cpu.idle\r\n')
time.sleep(0.1);
ret = s.recv(4096);
print ret;


r.delete('ts:cpu.user')
r.delete('ts:cpu.sys')
r.delete('ts:cpu.idle')

buff = ''
for i in range (200):
	buff += 'ts:cpu.user %f %d ' % (i+0.5, i)
	buff += 'ts:cpu.sys %f %d ' % (i+0.5, i*2)
	buff += 'ts:cpu.idle %f %d ' % (i+0.5, i*3)

s.sendall('tsadd %s\r\n' % buff)
ret = s.recv(4096);
print ret;


s.sendall('tsrange 0 1000000 ts:cpu.user ts:cpu.sys ts:cpu.idle\r\n')
time.sleep(0.1);
ret = s.recv(4096);
print ret;









