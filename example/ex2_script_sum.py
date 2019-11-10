import os, sys, time, threading
import redis

r = redis.StrictRedis(host='127.0.0.1', port=6379)


for i in range(1000):
	r.set('key_%d' % i, i)

simple_sum = '''
	using krill;

	key_list = argv[0];
	argv_list = argv[1];

	sum = 0;
	for key in key_list {
		# you can access native data directly
		# this eval is inside of krill.data_mutex
		v  = krill.data[key];
		if v == nil {
			continue;
		}

		sum += v.integer();
	}

	return sum;
'''

ret = r.eval(simple_sum, 3, 'key_1', 'key_2', 'key_3')
print ret; # 1 + 2 + 3 = 6


