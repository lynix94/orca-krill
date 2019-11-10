import os, sys, time, threading
import redis

r = redis.StrictRedis(host='127.0.0.1', port=6379)


for i in range(1000):
	r.set('key_%d' % i, i)


total_sum_avg_min_max = '''
	using krill;
	using type;

	min = type.limits.int64_max;
	max = type.limits.int64_min;
	count = 0;
	sum = 0;
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

	return [sum, avg, min, max];
''';


ret = r.eval(total_sum_avg_min_max, 0);
print 'SUM, AVG, MIN, MAX of data: ', ret; 



