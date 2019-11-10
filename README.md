
# orca krill

The orca krill is redis api level compatible memory storage which is made by pure orca programming language.
The orca programming language is the new oop language and you can see more information from this.

https://github.com/lynix94/orca-lang


The krill is made by pure orca so memory efficiency and speed are not goal of this project.

But, The purposes of krill are belows especially No 4.


1. Krill can be mixed with orca.

It's made by orca language, So you can access data with orca native method.
Script of krill is also made by orca not lua. That script can be executed as thread at the krill server and can access data directly (with mutex)

TODO: ex2 예제

2. Support non-blocking script

Redis script get a lock during execution. So other requests are blocked. So redis provide script kill to avoid script hanging. And redis cript is designed to run at once.

The krill also support redis level script. eval, evalsha and script command are same with redis except using orca language as script.

But, The krill provide non-blocking script. This script can lock and unlock the data inside of it. So script can lock before access data and unlock if it's done. As an example, below script show that.

TODO: ex4 예제

Above example calculate sum, average, min, max of values whose name are starts with key_ every 1 sec. Then push that result into result list. (Then client read this result with brpop)
But To use redis pure api, You should use getset command of special key. getset krill:script:set:SCRIPT_NAME SCRIPT_BODY stores non-blocking script to krill server.
And getset krill:script:run:SCRIPT_NAME PARAM execute non-blocking server as thread. This command return TID and you can use this value to stop the script.
And you can execute many script at same time like thread.

Below example is echo server by krill script.



TODO: ex5 예제

The client send message to ask list by lpush. Then echo script which wait ask list by channel wakes up and read message. Then the echo script send reply to answer list by lpush. Then the script read answer by brpop.

You can see hello 1, hello 2, hello 3, hello 4, hello 5 if you execute this example. Script can be communicate with outside. So the client send special message 'quit' and server shutdown graceflly when it read 'quit'


3. The orca program can include krill as library

The orca program can include krill and run krill as a thread. Then this orca program can read krill.data which are stored as k/v, hash, set, list and stream by krill server.
This program can communicate with client with blocking method (brpop or stream)


4. The krill support time-series by level db

The krill support redis stream. And if the key of stream starts with ts:, the krill assumes that as time-series db and move old data to level db. So you can store time-series data without memory limitation. And you can use or analyze time series data by script or program.

The krill is aimed as the storage of time-series monitoring tool like graphite. The next target of orca project is making time series monitoring tool like grafana. The orca programming language is general purpose language but the one of most impressive factor of it is on web programming. You can see that by 10 minutes introduction and The RAD mode orca sonar by below two links.

링크 2개

The orca project will introduce the time-series monitoring web tool as the example of above advantages. The existing monitoring tool provide user defined dash-board but those are made by existing interface so has its limitation. The RAD modification of orca sonar can be used to build user defined dash board or functions without limitation. Let's see this next result someday later and you can see the possiblity of this new language paradigm.


Aboves are the advantages of krill.

Now it provides version 1.x level of redis api except stream. But api will be added later and special factors will be added.

Any kinds of contribution are welcome.

Thanks for reading and you can see running example by below link.

링크 추가
















