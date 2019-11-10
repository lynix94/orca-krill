import os, sys, time, threading
import redis

test_host = '127.0.0.1'
test_port = 6379

r = redis.StrictRedis(host=test_host, port=test_port)







#
# K/V
#
r.delete('key')
assert r.set('key', 'value') == True

assert r.get('key') == 'value'

assert r.type('key') == 'string'
assert r.exists('key') == 1
assert r.exists('key2') == 0

for i in range(1000):
	r.set('key-%d' % i, 'value-%d' % i)
	assert r.get('key-%d' % i) == 'value-%d' % i




#
# list
#

raised = False
try:
	r.lpush('key', 1) 
except redis.exceptions.ResponseError:
	raised = True

assert raised == True

r.delete('list')
assert r.lpush('list', 6) == 1
assert r.lpush('list', 5, 4) == 3
assert r.lpush('list', '3', '2', '1') == 6
assert r.lpush('list', 0) == 7
assert r.rpush('list', 7) == 8
assert r.rpush('list', 8, 9, 10) == 11 # 0 1 2 3 4 5 6 7 8 9 10

assert r.llen('list') == 11

for i in range(11):
	assert int(r.lindex('list', i)) == i

assert r.lpop('list') == '0'
assert r.lpop('list') == '1'
assert r.lpop('list') == '2'
assert r.lpop('list') == '3'
assert r.rpop('list') == '10'
assert r.rpop('list') == '9'
assert r.rpop('list') == '8'

assert r.lrange('list', 0, -1) == ['4', '5', '6', '7'];
assert r.lrange('list', 1, -2) == ['5', '6'];
assert r.lrange('list', 2, 3) == ['6', '7'];

assert r.lset('list', 0, 'a') == True
assert r.lset('list', 1, 'b') == True
assert r.lrange('list', 0, -1) == ['a', 'b', '6', '7'];


# blpop, brpop
r.delete('list')
assert r.rpush('list', 1) == 1
assert r.rpush('list', 2) == 2
assert r.rpush('list', 3) == 3
assert r.blpop('list')[1] == '1'
assert r.brpop('list')[1] == '3'
assert r.brpop('list')[1] == '2'

start = time.time()
assert r.brpop('list', 1) == None
end = time.time()
assert (end - start) >= 1 and (end - start) < 1.1


def lpush_thread(host, port, key, sleep):
	time.sleep(sleep)
	r = redis.StrictRedis(host=host, port=port)
	ret = r.lpush('list', 10)

t = threading.Thread(target=lpush_thread, args=(test_host, test_port, 'stream', 0.3))
t.start()

start = time.time()
ret = r.brpop('list', 10)
assert ret[1] == '10'
end = time.time()
assert (end - start) >= 0.3 and (end - start) < 0.4

t.join()




#
# set
#
r.delete('set1')
r.delete('set2')
r.delete('set3')
for i in range(10):
	assert r.sadd('set1', 'set_%d' % i) == 1

assert len(r.smembers('set1')) == 10
assert r.scard('set1') == 10

for i in range(6, 16):
	assert r.sadd('set2', 'set_%d' % i) == 1

assert len(r.sunion('set1', 'set2')) == 16
assert len(r.sinter('set1', 'set2')) == 4
assert len(r.sdiff('set1', 'set2')) == 6

assert r.sunionstore('set3', 'set1', 'set2') == 16
assert r.scard('set3') == 16

assert r.sinterstore('set3', 'set1', 'set2') == 4
assert r.scard('set3') == 4

assert r.sdiffstore('set3', 'set1', 'set2') == 6
assert r.scard('set3') == 6

assert r.sismember('set1', 'set_1') == True
assert r.sismember('set1', 'set_15') == False
assert r.sismember('set2', 'set_1') == False
assert r.sismember('set2', 'set_15') == True


assert r.sismember('set1', 'set_0') == True
assert r.sismember('set2', 'set_0') == False
assert r.smove('set1', 'set2', 'set_0') == True
assert r.sismember('set1', 'set_0') == False
assert r.sismember('set2', 'set_0') == True


#
# zset
#

r.delete('zset')
for i in range(10):
	assert r.zadd('zset', {i:"subkey-%d" % i}) == 1

ret = r.zrange('zset', 0, -1)
assert len(ret) == 10
assert ret[0] == 'subkey-0'
assert ret[9] == 'subkey-9'

ret = r.zrange('zset', 0, -1, withscores=True)
assert len(ret) == 10
assert ret[0] == ('subkey-0', 0.0)

ret = r.zrange('zset', 0, -1, True)
assert len(ret) == 10
assert ret[0] == ('subkey-9')

ret = r.zrangebyscore('zset', 3, 7)
assert len(ret) == 5
assert ret[0] == 'subkey-3'
assert ret[4] == 'subkey-7'

assert r.zcard('zset') == 10


#
# hash
#

r.delete('hash')
assert r.hget('hash', 'key') == None
assert r.hset('hash', 'key', 'value') == 1
assert r.hget('hash', 'key') == 'value'
assert r.hset('hash', 'key', 'value2') == 0
assert r.hget('hash', 'key') == 'value2'
assert r.hdel('hash', 'key') == 1
assert r.hdel('hash', 'key') == 0
assert r.hget('hash', 'key') == None
assert r.hlen('hash') == 0
assert r.hset('hash', 'key', 'value3')
assert r.hlen('hash') == 1
assert r.hset('hash', 'key2', 'value2') == 1
assert r.hlen('hash') == 2

assert r.hkeys('hash') == ['key', 'key2'];
assert r.hvals('hash') == ['value3', 'value2'];
assert r.hgetall('hash') == {'key2':'value2', 'key':'value3'};
assert r.hexists('hash', 'key') == True;
assert r.hexists('hash', 'nokey') == False;



#
# stream
#

r.delete('stream')
r.delete('stream2')

id_list = []
for i in range (10):
	ret = r.xadd('stream', {'a':0, 'b':1})
	id_list.append(ret)

assert len(r.xrange('stream')) == 10
assert len(r.xrevrange('stream')) == 10

assert r.xlen('stream') == 10

assert len(r.xrange('stream', id_list[3], id_list[7])) == 5
assert len(r.xrevrange('stream', id_list[7], id_list[3])) == 5

assert r.xdel('stream', *id_list[0:3]) == 3
assert r.xlen('stream') == 7

assert r.xtrim('stream', 5) == 2
assert r.xlen('stream') == 5

assert r.xread({'stream':'$'}) == []

start = time.time()
assert r.xread({'stream':'$'}, block=300) == []
end = time.time()
assert (end - start) >= 0.3 and (end - start) < 0.4



def xadd_thread(host, port, key, sleep):
	time.sleep(sleep)
	r = redis.StrictRedis(host=host, port=port)
	ret = r.xadd('stream', {'a':1000, 'b':1000})

t = threading.Thread(target=xadd_thread, args=(test_host, test_port, 'stream', 0.3))
t.start()

start = time.time()
ret = r.xread({'stream':'$'}, block=1000*60)
assert ret[0][0] == 'stream'
assert ret[0][1][0][1]['a'] == '1000'
end = time.time()
assert (end - start) >= 0.3 and (end - start) < 0.4

t.join()





#
# stream - group
#

r.delete('stream')
r.delete('stream2')

print r.xadd('stream', {'a':0, 'b':1})
print r.xadd('stream2', {'a':0, 'b':1})

r.xgroup_destroy('stream', 'group-a');
assert r.xgroup_create('stream', 'group-a') == True;
assert r.xgroup_destroy('stream', 'group-a') == True;
assert r.xgroup_create('stream', 'group-a') == True;

print r.xinfo_stream('stream');
print r.xinfo_groups('stream');
print r.xinfo_consumers('stream', 'group-a');
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})


#
# ts
#
r.delete('stream')
r.delete('ts:stream')


for i in range (20):
	print r.xadd('ts:stream', {'a':i, 'b':i})

print r.xlen('ts:stream')
print r.xrange('ts:stream')




print 'done'

