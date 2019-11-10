import os, sys, time, threading
import redis

r = redis.StrictRedis(host='127.0.0.1', port=6379)

print 'this is krill non-blocking script test'

for i in range(1000):
	r.set('key_%d' % i, i)


print_per_sec = '''
using krill;
using time;
using datetime;

#print: argv;

while true {
	krill.data_mutex { # only this code block is blocked
		min = type.limits.int64_max;
		max = type.limits.int64_min;
		count = sum = 0;

		for k, v in krill.data {
			#print: v; # you can print this. this will be printed at server side
			if !k.starts_with('key_') {
				continue;
			}

			v = v.integer(0); # 0 is default value if convert fails. (without it, exception occurs)
			sum += v;
			if v > max {
				max = v;
			}

			if v < min {
				min = v;
			}

			count += 1;
		}

		avg = sum.float() / count;

		l = krill.data['result'];
		if l == nil {
			l = [];
			krill.data['result'] = l;
		}

		value = [string(datetime.now()), sum, avg, min, max];

		# low level approach. push native list & signal to key
		l.push_back(value);
		krill.signal_block('result');

		# you can use krill api which is same above. It contains signal
		#krill.list.lpush('result', value);
	}

	time.sleep(1);
}
'''

print r.getset('krill:script:set:print_per_sec', print_per_sec)
tid =  r.getset('krill:script:run:print_per_sec', 'hello')

print r.getset('krill:script:runnings:*', '')

for i in range(3):
	print r.blpop('result') # blpop can be executed even script is still running

# you can stop script by this
print r.getset('krill:script:stop:%s' % tid, '')
print r.getset('krill:script:runnings:*', '')


