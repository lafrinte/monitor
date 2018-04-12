## Monitor
### 1. Describe

* a monitor tool for specify logs.

* require:

```
pykafka
gevent
pyinotify
pyyaml
```

* python:

```
python3.X
```

* output: the output is a json string like below

```
{
  'fields': {'para1': value1, 'para2': value2}
  'message': 'log'
}
```



* configuration

```
prospectors:
- input_type:
  paths:
    - /User/lafrinte/workstation/code/monitor/logs/monitor*.logs
  fields:
    software: esb
    product: dazhanggui
    ip: 172.17.0.2
    topic: esb-monitor

# use multiline module
# patterns: The pattern should match what you believe to be an indicator that the field is part of a multi-line event.
# negate: default is False. a message not matching the pattern will constitute a match of the multiline filter
  multiline:
    patterns: '^\['
    negate: True       

# use tags module
# forward: if the log contains the sub-string, it will generate a unparalleled string in field.ssc
  tags:
    forward: 'Service Begin'

output:
- kakfa:
  topics: '%{[fields.topic]}'
  bootstrap_server: '172.17.0.2:9092,172.17.0.3:9092'
```


