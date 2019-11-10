import os, sys, time, threading
import redis

r = redis.StrictRedis(host='127.0.0.1', port=6379)


# communicate with script. you can use socket inside of script (wow)
# but I recommand blocking list
echo_script = '''
using krill;
using time;

while true {
	krill.data_mutex {
		l = krill.data['ask'];
		if l == nil {
			continue;
		}

		c = nil;
		if l.size() > 0 {
			msg = l[0];
			l.pop_front();
			if msg == 'quit' {
				krill.list.lpush('answer', 'bye');
				return;
			}

			krill.list.lpush('answer', msg);
		}
		else {
			c = krill.script_wait_key('ask'); # register me to wait 'ask' key
		}
	}

	if c != nil {
		select
		{
		case c -> ret:
			print: 'get channel';
			
		case time.timer(1) -> ret:
			print: 'timeout';
		}
	}
}
'''

print r.getset('krill:script:set:echo', echo_script)
tid =  r.getset('krill:script:run:echo', '')

print r.getset('krill:script:runnings:*', '')

for i in range(5):
	r.rpush('ask', 'hello, (%d)' % i);
	print r.blpop('answer');

# you can stop script by this
r.rpush('ask', 'quit');
print r.getset('krill:script:join:%s' % tid,  '')
print r.getset('krill:script:runnings:*', '')

