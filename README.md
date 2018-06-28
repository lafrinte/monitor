## 1. Describe

Beats is similar to Beats(ELK system).
It only offers tags and multiline function to parse special data which cannot be realized by Beats or Logstash.
If your willing to focus a error and its context, but the source log donot has a flag, this will get some help for you.

## configuration

### conf/Beats.yml

To configure this input, specify a list of glob-based paths that must be crawled to locate and fetch the log lines.

Example configuration:

```
prospectors:
- input_type:
  paths:
    - /var/log/mysql_3306/mysql*.log
    - /var/log/mysql_3307/mysql*.log
```

You can apply additional configuration settings (such as tags, fields, multiline)to the lines harvested from these files. The options that you specify are applied to all the files harvested by this input.

To apply different configuration settings to different files, you need to define multiple input sections:

```
prospectors:
- input_type:
  paths:
    - /var/log/mysql_3306/mysql*.log
- input_type:
  paths:
    - /var/logs/nginx/nginx*.log
```

> paths

A list of glob-based paths that will be crawled and fetched

> fields

Optional fields that you can specify to add additional information to the output. For example, you might add fields that you can use for filtering log data. Fields must be a json like values. The fields that you specify here will be grouped under a fields sub-dictionary in the output document.

> multiline

The multiline will control how Beats deals with messages that span multiple lines

The following example shows how to configure Beats to handle a multiline message where the first line of the message begins with a bracket ([).

```
...
multiline:
  patterns: '^\['
  negate:  false
```

Beats takes all the lines that do not start with [ and combines them with the previous line that does

#### patterns

Specifies the regular expression pattern to match. See Regular expression support for a list of supported regexp patterns. Depending on how you configure other multiline options, lines that match the specified regular expression are considered either continuations of a previous line or the start of a new multiline event. You can set the negate option to negate the pattern.

#### negate

Defines whether the pattern is negated. The default is false.

> tags

The tags will set a ssc field saving the same flag for context in different events. Such as a error occure in logs, the line error message occure cannot help location, but the above logs can be. The tags will help to set a field with same value in everyline

The following example shows how to configure Beats to handle a tag where the the message contains string 'Service Begin'.
The lines between the nearest two line which contain 'Serival Begin' will has a same value in field named ssc

```
tags:
  forward: 'Service Begin'
```

> attention: forward only requires a string, not regex pattern

> Example:

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
  multiline:
    patterns: '^\['
    negate:  false
  tags:
    forward: 'Service Begin'

output:
- kakfa:
  topics: '%{[fields.topic]}'
  bootstrap_server: '172.17.0.2:9092,172.17.0.3:9092'
```
