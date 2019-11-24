
print 'basic redis examples'

import os, sys, time, threading
import redis

test_host = '127.0.0.1'
test_port = 6379

r = redis.StrictRedis(host=test_host, port=test_port)



print 'simple K/V example. it is compatible with redis'
print r.set('key', 'value')
print r.get('key')

print r.type('key')
print r.exists('key')
print r.delete('key')
print r.exists('key')



print 'list example. basic operation and brpop, blpop commands are possible'

r.delete('list')
print r.lpush('list', 6)
print r.lpush('list', 5, 4)
print r.lpush('list', '3', '2', '1')
print r.lpush('list', 0)
print r.rpush('list', 7)
print r.rpush('list', 8, 9, 10)

print r.llen('list')

print r.lpop('list')
print r.lpop('list')
print r.lpop('list')
print r.lpop('list')
print r.rpop('list')
print r.rpop('list')
print r.rpop('list')

print r.lrange('list', 0, -1)
print r.lrange('list', 1, -2)
print r.lrange('list', 2, 3)

print r.lset('list', 0, 'a')
print r.lset('list', 1, 'b')
print r.lrange('list', 0, -1)


# blpop, brpop
r.delete('list')
print r.rpush('list', 1)
print r.rpush('list', 2)
print r.rpush('list', 3)
print r.blpop('list')[1]
print r.brpop('list')[1]
print r.brpop('list')[1]

start = time.time()
print r.brpop('list', 1)
end = time.time()
print end - start


def lpush_thread(host, port, key, sleep):
	time.sleep(sleep)
	r = redis.StrictRedis(host=host, port=port)
	ret = r.lpush('list', 10)

t = threading.Thread(target=lpush_thread, args=(test_host, test_port, 'stream', 0.3))
t.start()

start = time.time()
ret = r.brpop('list', 10)
print ret[1]
end = time.time()
print end - start

t.join()



print 'set examples'

r.delete('set1')
r.delete('set2')
r.delete('set3')
for i in range(10):
	print r.sadd('set1', 'set_%d' % i)

print len(r.smembers('set1'))
print r.scard('set1')

for i in range(6, 16):
	print r.sadd('set2', 'set_%d' % i)

print len(r.sunion('set1', 'set2'))
print len(r.sinter('set1', 'set2'))
print len(r.sdiff('set1', 'set2'))

print r.sunionstore('set3', 'set1', 'set2')
print r.scard('set3')

print r.sinterstore('set3', 'set1', 'set2')
print r.scard('set3')

print r.sdiffstore('set3', 'set1', 'set2')
print r.scard('set3')

print r.sismember('set1', 'set_1')
print r.sismember('set1', 'set_15')
print r.sismember('set2', 'set_1')
print r.sismember('set2', 'set_15')


print r.sismember('set1', 'set_0')
print r.sismember('set2', 'set_0')
print r.smove('set1', 'set2', 'set_0')
print r.sismember('set1', 'set_0')
print r.sismember('set2', 'set_0')


print 'zset examples'

r.delete('zset')
for i in range(10):
	print r.zadd('zset', {"subkey-%d" % i:i})

ret = r.zrange('zset', 0, -1)
print len(ret)
print ret[0]
print ret[9]

ret = r.zrange('zset', 0, -1, withscores=True)
print len(ret)
print ret[0]

ret = r.zrange('zset', 0, -1, True)
print len(ret)
print ret[0]

ret = r.zrangebyscore('zset', 3, 7)
print len(ret)
print ret[0]
print ret[4]

print r.zcard('zset')


print 'hash examples'

r.delete('hash')
print r.hget('hash', 'key')
print r.hset('hash', 'key', 'value')
print r.hget('hash', 'key')
print r.hset('hash', 'key', 'value2')
print r.hget('hash', 'key')
print r.hdel('hash', 'key')
print r.hdel('hash', 'key')
print r.hget('hash', 'key')
print r.hlen('hash')
print r.hset('hash', 'key', 'value3')
print r.hlen('hash')
print r.hset('hash', 'key2', 'value2')
print r.hlen('hash')

print r.hkeys('hash')
print r.hvals('hash')
print r.hgetall('hash')
print r.hexists('hash', 'key')
print r.hexists('hash', 'nokey')




print 'stream examples'

r.delete('stream')
r.delete('stream2')

id_list = []
for i in range (10):
	ret = r.xadd('stream', {'a':0, 'b':1})
	id_list.append(ret)

print len(r.xrange('stream'))
print len(r.xrevrange('stream'))

print r.xlen('stream')

print len(r.xrange('stream', id_list[3], id_list[7]))
print len(r.xrevrange('stream', id_list[7], id_list[3]))

print r.xdel('stream', *id_list[0:3])
print r.xlen('stream')

print r.xtrim('stream', 5)
print r.xlen('stream')

print r.xread({'stream':'$'})

start = time.time()
print r.xread({'stream':'$'}, block=300)
end = time.time()
print end - start



def xadd_thread(host, port, key, sleep):
	time.sleep(sleep)
	r = redis.StrictRedis(host=host, port=port)
	ret = r.xadd('stream', {'a':1000, 'b':1000})

t = threading.Thread(target=xadd_thread, args=(test_host, test_port, 'stream', 0.3))
t.start()

start = time.time()
ret = r.xread({'stream':'$'}, block=1000*60)
print ret
end = time.time()
print end - start

t.join()





print 'stream group read'

r.delete('stream')
r.delete('stream2')

print r.xadd('stream', {'a':0, 'b':1})
print r.xadd('stream2', {'a':0, 'b':1})

r.xgroup_destroy('stream', 'group-a');
print r.xgroup_create('stream', 'group-a')
print r.xgroup_destroy('stream', 'group-a')
print r.xgroup_create('stream', 'group-a')

print r.xinfo_stream('stream');
print r.xinfo_groups('stream');
print r.xinfo_consumers('stream', 'group-a');
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})
print r.xreadgroup('group-a', 'consumer-a', {'stream':'>'})


