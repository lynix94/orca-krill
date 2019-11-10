import os, sys, time, threading
import redis

r = redis.StrictRedis(host='127.0.0.1', port=6379)

print '#### krill script test...'

test_script = '''
using krill;
using time;

for i in range(3) {
	krill.data_mutex {
		print: 'get mutex....%d' % i;
	}

	time.sleep(1);
}
'''

test_script_hang = '''
using krill;
using time;

krill.data_mutex {
	while true {
		print: 'script is running';
		time.sleep(1);
	}
}
'''



assert r.getset('krill:script:set:test', test_script) == 'OK'
assert len(r.getset('krill:script:get:test', '')) > 10
th =  r.getset('krill:script:run:test', 'param')
assert len(r.getset('krill:script:runnings:test2', '')) == 0
assert len(r.getset('krill:script:runnings:test', '')) > 10
assert len(r.getset('krill:script:runnings:*', '')) > 10
assert r.getset('krill:script:join:%s' % th, '') == 'OK'

assert len(r.getset('krill:script:runnings:test', '')) == 0
assert len(r.getset('krill:script:runnings:*', '')) == 0

assert r.getset('krill:script:set:test_hang', test_script_hang) == 'OK'
assert len(r.getset('krill:script:get:test_hang', '')) > 10
th =  r.getset('krill:script:run:test_hang', 'param')

assert r.getset('krill:script:stop:%s' % th, '') == 'OK'

print '#### eval test...'

eval_script = '''
	print: argv;
	key_list = argv[0];
	argv_list = argv[1];
	return argv[0] + argv[1];
'''

ret = r.eval(eval_script, 3, 'key1', 'key2', 'key3', 'argv1', 'argv2')
assert ret == ['key1', 'key2', 'key3', 'argv1', 'argv2'];
sha1 = r.script_load(eval_script);
ret = r.evalsha(sha1, 2, 'key1', 'key2', 'argv1')
assert ret == ['key1', 'key2', 'argv1'];
assert r.script_exists(sha1) == [True]
assert r.script_kill() == True
assert r.script_flush() == True
assert r.script_exists(sha1) == [False]


eval_hang_script = '''
	using time;

	while true {
		time.sleep(1);
	}
	return 0;
'''

def stopper(r):
	time.sleep(3);
	r.script_kill();

th = threading.Thread(target = stopper, args = (r,));
th.start();

ret = r.eval(eval_hang_script, 3, 'key1', 'key2', 'key3', 'argv1', 'argv2')


