# OpenTSDB MQTT Exporter

This is a simple Python script that subscribes to an MQTT topic and sends the received messages to an OpenTSDB server.

```bash
usage: main.py [-h] [-c CONFIG] [--max_send_messages MAX_SEND_MESSAGES] [--max_time MAX_TIME] --broker BROKER [--port PORT] [--topic TOPIC] [--username USERNAME] [--password PASSWORD] [--root_ca ROOT_CA]
               [--tsdb_host TSDB_HOST] [--tsdb_port TSDB_PORT] [--tsdb_uri TSDB_URI] [--override_config OVERRIDE_CONFIG] [--log_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [--add_host_tag ADD_HOST_TAG]
               [--static_tags STATIC_TAGS] [--metric_prefix METRIC_PREFIX]

options:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Path to the YAML configuration file
  --max_send_messages MAX_SEND_MESSAGES
                        Maximum number of messages to send [env var: MAX_SEND_MESSAGES]
  --max_time MAX_TIME   Maximum time interval for sending messages [env var: MAX_TIME]
  --broker BROKER       MQTT broker address [env var: MQTT_BROKER]
  --port PORT           MQTT broker port [env var: MQTT_PORT]
  --topic TOPIC         MQTT topic to subscribe [env var: MQTT_TOPIC]
  --username USERNAME   MQTT username [env var: MQTT_USERNAME]
  --password PASSWORD   MQTT password [env var: MQTT_PASSWORD]
  --root_ca ROOT_CA     Path to root CA certificate [env var: MQTT_ROOT_CA]
  --tsdb_host TSDB_HOST
                        OpenTSDB host [env var: OPEN_TSDB_HOST]
  --tsdb_port TSDB_PORT
                        OpenTSDB port [env var: OPEN_TSDB_PORT]
  --tsdb_uri TSDB_URI   OpenTSDB URI [env var: OPEN_TSDB_URI]
  --override_config OVERRIDE_CONFIG
                        Path to the YAML override configuration file [env var: OVERRIDE_CONFIG]
  --log_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level [env var: LOG_LEVEL]
  --add_host_tag ADD_HOST_TAG
                        Add host tag to TSDB data [env var: ADD_HOST_TAG]
  --static_tags STATIC_TAGS
                        Static tags for TSDB in JSON format [env var: STATIC_TAGS]
  --metric_prefix METRIC_PREFIX
                        Metric prefix [env var: METRIC_PREFIX]

Args that start with '--' can also be set in a config file (config.yaml or config.yml or specified via -c). Config file syntax allows: key=value, flag=true, stuff=[a,b,c] (for details, see syntax at https://goo.gl/R74nmi). In
general, command-line values override environment variables which override config file values which override defaults.
```

## Topics

The topic has to conform to the following regex:

```regexp
^dt/(?P<app>[\w-]+)/(?P<context>[\w\-/]+)/(?P<thing>[\w-]+)/(?P<property>[\w-]+)$"
```

Example:

* `dt/myapp/room/esp32/temperature`
* `dt/myapp/room/esp32/humidity`
* `dt/myapp/myhouse/firstfloor/livingroom/esp32/temperature`

## Message format

* int
* float
* json

The message has to be a JSON object with the following format:

```json
{
  "value": 23.5,
  "timestamp": 1589784000,
  "extra_tag": "extra_value"
}
```

The `timestamp` field is optional. If not present, the current timestamp will be used.
The `extra_tag` field is optional. If present, it will be added to the tags of the metric. It can be any key value pair
as long as the key is a string and the value is a string, number or float.
If `value` is not present `-1` will be used.

## Metric format

The metric name will be composed as follows:

```
<metric_prefix>(<property>|<tags['property']>)
```

Example:
`dt/myapp/room/esp32/temperature` will be converted to `mqtt__temperature` with tags `app=myapp`, `context=room`, `thing=esp32`, `property=temperature`, `topic=dt/myapp/room/esp32/temperature`.

If there is context with more then one level, additional fields will be added to the tags:

```
`context_0`, `context_1`, `context_2`, ...
```
