 
# orca krill

크릴은 오르카 프로그래밍 언어로 만들어진 메모리 스토리지로 redis 와 프로토콜이 호환 된다.
오르카 프로그래밍 언어는 새로운 객체지향 스크립트 언어로 아래 저장소를 참조하기 바란다.

https://github.com/lynix94/orca-lang

크릴은 순수하게 오르카로만 작성되어 있어 메모리 효율성이나 속도는 고려사항이 아니다. (C로 만들어진 순수 redis 보다 당연이 느리고 메모리를 많이 사용한다)

하지만, 순수 오르카로 또다른 redis 클론을 만든 이유는 아래를 목표로 했기 때문이다.
특히 가장 마지막 4번이 사실 이 프로젝트를 시작한 이유이다.


### 1. 오르카와 완전히, 자연스럽게 연동된다.

오르카 언어로 동작하기 때문에, 안의 자료들을 오르카 네이티브 로 억세스 할 수 있다.
스크립트의 경우도 redis 의 lua 스크립트가 아니라 오르카 언어로 작성하며, 해당 스크립트는 krill 서버에서 별도의 쓰레드로 실행되며 데이터를 바로 억세스 할 수 있다.

https://github.com/lynix94/orca-krill/blob/master/example/ex2_script_sum.py


### 2. non-blocking 스크립트 지원

redis 의 script 는 lock 을 잡고 진행하기 때문에 script 가 실행되는 동안 blocking 된다. 때문에 script 가 늦게 끝나거나 hang 되는 경우를 방지 하기 위해 kill 명령이 있다. 그리고 redis script 는 한번에 하나만 실행하는 것을 전제로 기획되어 있다.

krill 도 redis level 의 script 를 지원한다. eval, evalsha, script 명령을 사용하면 script 의 언어가 오르카라는 점만 다르고 동일하게 동작한다. 

하지만 non-blocking 으로 동작하는 스크립트를 실행시킬 수도 있다. 이 nb 스크립트는 안에서 데이터 조작이 필요한 경우에만 mutex 를 걸었다 풀면서 실행할 수 있는데, 아래의 예와 같이 실행할 수 있다.

https://github.com/lynix94/orca-krill/blob/master/example/ex4_krill_script_nonblocking.py

이 예는 1초마다 주기적으로 돌면서 key_ 로 시작하는 값들의 총합, 평균, min, max 를 result list 에 lpush 로 추가한다. 단, 이렇게 별도의 쓰레드 형태로 도는 script 를 실행하려면 보기의 예와 같이,
getset krill:script:set:SCRIPT_NAME SCRIPT_BODY 으로 저장하고,
getset krill:script:run:SCRIPT_NAME PARAM 으로 실행한다. 이렇게 실행하면 리턴값으로 TID 가 오는데 이것을 사용하여 해당 쓰레드 스크립트를 종료 시킬수 있다.
쓰레드를 실행하는 것처럼 동일한 스크립트를 여러개 실행할 수 도 있다.


아래는 에코서버를 스크립트로 구현한 예이다.

https://github.com/lynix94/orca-krill/blob/master/example/ex5_krill_script_echo.py

client 가 lpush 로 ask list 에 message 를 보내면 채널로 시그널을 기다리던 서버가 깨어나서 message 를 읽고, answer 에 push 하면 client 는 brpop 으로 이를 읽는 예이다.
이를 실행하면 hello 1, hello 2, hello 3, hello 4, hello 5 가 연속으로 출력된다. 스크립트를 데몬 서버처럼 만들 수 있기 때문에 이 스크립트는 quit 을 읽으면 gracefully exit 가 되는 것도 볼 수 있다.





### 3. 그리고 krill 은 라이브러리로 인클루드 할 수 있다.

오르카 프로그램을 작성하면서 krill 을 import 하고  별도 쓰레드로 띄우면 krill.data 를 통해 krill 이 저장하고 있는 k/v, hash, set, list, stream 을 바로 접근할 수 있다. 이 프로그램은 blocking pop 을 이용해 외부와 통신할 수도 있다.


### 4. level db 를 사용한 time-series 저장소를 지원한다.

krill 은 redis stream 을 지원하는데, 만일 stream 의 key 가 ts: 로 시작하면 별도 취급하여 오래된 데이터는 level db 로 이동시킨다. 때문에 메모리 제한없이 time-series 저장소로 사용할 수 있고, 별도의 스크립트나 프로그램에서 이 타임시리즈를 사용, 분석할 수 있다.

krill 은 오르카가 grafana 와 같은 타임시리즈 모니터링, 분석 툴을 제작하기 위한 저장소로 사용하기 위해 시작되었다. 위 목적의 웹 프로그래밍이 직접, 혹은 원격의 데이터를 효율적으로 다루는 것이 목적이다. 
오르카는 범용 프로그래밍이지만 일단 웹 프로그래밍에서 그 고유의 특장점을 보여줄 생각으로 아래의 10분가이드 및 RAD 모드와 같은 새로운 방식의 접근이 가능하다.

https://github.com/lynix94/orca-lang/blob/master/docs/10min_kr.md

https://github.com/lynix94/orca-lang/blob/master/docs/rad_kr.md

필자는 이를 강조하기 위한 유용한 예제로 시계열 모니터링, 분석 툴을 고려했는데, 기존의 모니터링 툴이 정해진 메트릭을 고르고 사전에 정의된 제한된 가공만이 가능한데 비해 오르카의 웹 프로젝트 툴인 sonar 의 RAD 모드를 개선하면 제한없는 사용이 가능하기 때문이다. 이 웹 프로젝트는 추후 릴리즈하며 다시 공유하겠다. ;)


위와 같은 이유가 krill 을 사용하여 얻을 수 있는 장점이다.

현재는 stream 을 제외하고는 redis 1.0 수준의 collection 만 지원하나 필요한 것들은 차츰 늘려갈 계획이고 orca 만의 동작들도 추가될 예정이다.

버그 리포팅 및 코드 추가 등 어느 형태의 기여도 환영한다.

그럼 읽어 주셔서 감사하고, 아래 링크에서 실 동작을 시연하는 모습을 볼수 있다.

https://youtu.be/OSjO0lQlCRk


### 실행하는 방법

오르카가 설치되어 있다면, 이 프로젝트를 내려받고 'orca run.orca [PORT]' 로 실행할 수 있다. default port 는 6379 이다.

또는 아래의 도커 이미지로 실행할 수 있다.

```
docker run -d -p 6379:6379 lynix94/orca-krill:latest
```














