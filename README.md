# OpenTSDB MQTT Exporter

This is a simple Python script that subscribes to an MQTT topic and sends the received messages to an OpenTSDB server.

```bash
$ python main.py --help
usage: main.py [-h] [-c CONFIG] [--max_send_messages MAX_SEND_MESSAGES] [--max_time MAX_TIME] --broker BROKER [--port PORT] [--client_id CLIENT_ID] [--topic TOPIC] [--username USERNAME] [--password PASSWORD]
               [--root_ca ROOT_CA] [--client_cert CLIENT_CERT] [--client_key CLIENT_KEY] [--tsdb_host TSDB_HOST] [--tsdb_port TSDB_PORT] [--tsdb_uri TSDB_URI] [--override_config OVERRIDE_CONFIG]
               [--log_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [--add_host_tag ADD_HOST_TAG] [--static_tags STATIC_TAGS] [--metric_prefix METRIC_PREFIX] [--victoria_metrics VICTORIA_METRICS] [--max_str_len MAX_STR_LEN]
               [--tags_exclude TAGS_EXCLUDE]

options:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Path to the YAML configuration file
  --max_send_messages MAX_SEND_MESSAGES
                        Maximum number of messages to send [env var: MAX_SEND_MESSAGES]
  --max_time MAX_TIME   Maximum time interval for sending messages [env var: MAX_TIME]
  --broker BROKER       MQTT broker address [env var: MQTT_BROKER]
  --port PORT           MQTT broker port [env var: MQTT_PORT]
  --client_id CLIENT_ID
                        MQTT client ID [env var: MQTT_CLIENT_ID]
  --topic TOPIC         MQTT topics to subscribe to, `,` coma separated list, you can use wildcards. Default: dt/# [env var: MQTT_TOPIC]
  --username USERNAME   MQTT username [env var: MQTT_USERNAME]
  --password PASSWORD   MQTT password [env var: MQTT_PASSWORD]
  --root_ca ROOT_CA     Path to root CA certificate [env var: MQTT_ROOT_CA]
  --client_cert CLIENT_CERT
                        Path to client certificate [env var: MQTT_CLIENT_CERT]
  --client_key CLIENT_KEY
                        Path to client key [env var: MQTT_CLIENT_KEY]
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
                        Add host tag to TSDB data, bool value, default: False [env var: ADD_HOST_TAG]
  --static_tags STATIC_TAGS
                        Static tags for TSDB in JSON format [env var: STATIC_TAGS]
  --metric_prefix METRIC_PREFIX
                        Metric prefix [env var: METRIC_PREFIX]
  --victoria_metrics VICTORIA_METRICS
                        Use VictoriaMetrics instead of OpenTSDB, does not return detail data. bool value, default: False [env var: VICTORIA_METRICS]
  --max_str_len MAX_STR_LEN
                        Maximum string length for TSDB tags, default: 128 [env var: MAX_STR_LEN]
  --tags_exclude TAGS_EXCLUDE
                        Tags to be removed from TSDB data, comma separated list, case insensitive default: metric_prefix [env var: TAGS_EXCLUDE]

Args that start with '--' can also be set in a config file (config.yaml or config.yml or specified via -c). Config file syntax allows: key=value, flag=true, stuff=[a,b,c] (for details, see syntax at https://goo.gl/R74nmi). In
general, command-line values override environment variables which override config file values which override defaults.
```

## Topics

### Topics with context

The topic can conform to the following regex:

```regexp
^dt/(?P<app>[ \w-]+)/(?P<context>[ \w\-/]+)/(?P<thing>[ \w-]+)/(?P<property>[ \w-]+)$"
```

Topic examples:

* `dt/myapp/room/esp32/temperature`
* `dt/myapp/room/esp32/humidity`
* `dt/myapp/myhouse/firstfloor/livingroom/esp32/temperature`

If they do conform, the following tags will be added to the metric:

* `app`
* `context`
* `thing`
* `context_...`

For a topic that does not conform to the regex, you can use the override config file to add these tags.

Example:

`dt/myapp/room/esp32/temperature` will be converted to `mqtt__temperature` with
tags `app=myapp`, `context=room`, `thing=esp32`, `property=temperature`, `topic=dt/myapp/room/esp32/temperature`.

If there is context with more then one level, additional fields will be added to the tags:

```
`context_0`, `context_1`, `context_2`, ...
```

### Topics without context

```regexp
^(([ \w-]+)/)*(?P<property>[ \w-]+)$"
```

The last part of the topic will be used as the property name.

## Message format

* int
* float
* json
* string
* bool (Only in JSON format) - True = 1, False = 0 (can be overridden in the override config

Message can be encoded binary with 0x00 padded around it (This can happen when sending a message from IoT devices
using C, with message size set to buffer size instead of content size.).

### JSON

JSON has to be in the following format:

```json
{
  "value": 23.5,
  "timestamp": 1589784000,
  "extra_tag": "extra_value"
}
```

The `timestamp` field is optional. If not present, the current timestamp will be used.
The `extra_tag` field is optional. If present, it will be added to the tags of the metric. It can be any key value pair
as long as the key is a string, and the value is a string, number or float.
If `value` is not present `-1` will be used.

### String

If a message is string and not a JSON, it will be converted to info metric (`<property>_info`) with the value of `1` and
the message as the
info label with user-configurable max length (default 128).

If the value is string, but it is a valid number or float, it will be converted to a metric with the value of the
number or float.

### Multiple values

#### Using a dictionary

##### Using `values` key

###### Using `values` key with dictionary

```json
{
  "values": {
    "outdoor": {
      "value": 23.5,
      "timestamp": 1589782000,
      "extra_tag": "extra_value"
    },
    "indoor": {
      "value": 24.5,
      "extra_tag": "extra_value2"
    },
    "underground": 25.5,
    "kitchen": "24",
    "bedroom": "too hot"
  },
  "timestamp": 1589784000,
  "extra_tag2": "extra_value3"
}
```

In this case the metric name will be `mqtt__temperature_indoor` and the tags will
be `app=myapp`, `context=room`, `thing=esp32`, `property=temperature`, `topic=dt/myapp/room/esp32/temperature`, `extra_tag=extra_value`, `extra_tag2=extra_value3`, `timestamp=1589782000`.
You can override properties for each value in the override config using the
key `dt/myapp/room/esp32/temperature:indoor`.

The order of the tags applied if using values is following:

* properties from the main json body
* properties from the value (`extra_tag` in the example above)
* properties extracted from the topic name
* properties from the override config

###### Using `values` key with list

In this case, you can send multiple values for the same property with different tags and some common tags for all
records:

```json
{
  "values": [
    {
      "value": 23.5,
      "timestamp": 1589782000,
      "extra_tag": "extra_value"
    },
    {
      "value": 24.5,
      "extra_tag": "extra_value2"
    },
    25,
    "24.5",
    "foo bar"
  ],
  "timestamp": 1589784000,
  "extra_tag2": "extra_value3"
}
```

##### Using a dictionary without `values` key

It is also possible to enable the `json_multi_value` in the override config. In this case the override config key will
be `dt/myapp/room/esp32/temperature`. This enables to send in messages in the following formats:

```json
{
  "indoor": 23.5,
  "outdoor": "24",
  "underground": {
    "value": 25.5,
    "timestamp": 1589782000,
    "extra_tag": "extra_value"
  },
  "kitchen": "too hot"
}
```

#### Using a list

```json
[
  {
    "value": 23.5,
    "timestamp": 1589782000,
    "extra_tag": "extra_value"
  },
  {
    "value": 24.5,
    "extra_tag2": "extra_value2"
  }
]
```

##### Using a list without `value` key

```json
[
  {
    "indoor": 23.5
  },
  {
    "outdoor": 24.5,
    "kitchen": "too hot"
  }
]
```

## Metric format

The metric name will be composed as follows:

```
<metric_prefix>(<property>(_<sub_value_name>)?|<override_config['property']>)(_info)?
```

## Override config

The override config is a YAML file that can be used to override the default configuration.
It can be used to override or set the following:

* `context` - Overrides the context from the topic. See [Metric format](#metric-format), useful
  for [Topics without context](#topics-without-context).
* `thing` - Overrides the thing name from the topic. See [Metric format](#metric-format), useful
  for [Topics without context](#topics-without-context).
* `property` - Overrides the property name from the topic. See [Metric format](#metric-format), useful
  for [Topics without context](#topics-without-context).
* `app` - the app name
* `extra_tags` - a dictionary of extra tags to add to the metric
* `is_json_multi_value` - if the message is a dictionary with multiple values
  see [Using a dictionary without `values` key](#using-a-dictionary-without-values-key)
* `metric_prefix` - Override the global metric prefix
* `value_replacement` - a dictionary of values to replace the value with follows same rules as
  extra_tags. See [Example with value removal](#example-with-value-removal)

The key in the override config is the topic name. The value is a dictionary with the keys above.
The key can be in a format of `dt/myapp/room/esp32/temperature:indoor` to override the property name for a specific
value in multivalued messages. The key can also be in a format of MQTT subscription topic with `#` or `+` wildcards.

### Basic Example:

```yaml
dt/myapp/room/esp32/temperature:
  context: warehouse/room
  thing: esp8266
  property: tmp
  app: app
  metric_prefix: my_mqtt__
  extra_tags:
    extra_tag: extra_value
    extra_tag2: extra_value2
  value_replacement:
    "too hot": 100
    15: "too cold"
    False: "-1"
    42: True
    15.5: "still too cold"
    "43": False
    "3.14": "pi"
    11: "12.5"
    10: "11"
  is_json_multi_value: true
```

message:

```json
{
  "indoor": {
    "value": 23.5,
    "location": "window"
  }
}
```

Output metric:

```
my_mqtt__tmp_indoor{app="app",context="warehouse/room",extra_tag="extra_value",extra_tag2="extra_value2",location="window",property="tmp",thing="esp8266",topic="dt/myapp/room/esp32/temperature:indoor"} 23.5 1589784000
```

### Example with wildcards:

```yaml
"dt/myapp/room/esp32/temperature:indoor":
  extra_tags:
    extra_tag2: extra_value2
dt/myapp/room/esp32/temperature:
  extra_tags:
    extra_tag2: extra_value3
    extra_tag3: extra_value3
dt/myapp/room/esp32/+:
  extra_tags:
    extra_tag2: extra_value4
    extra_tag3: extra_value4
    extra_tag4: extra_value4
dt/myapp/room/esp32/#:
  extra_tags:
    extra_tag2: extra_value5
    extra_tag3: extra_value5
    extra_tag4: extra_value5
    extra_tag5: extra_value5
dt/#:
  extra_tags:
    extra_tag2: extra_value6
    extra_tag3: extra_value6
    extra_tag4: extra_value6
    extra_tag5: extra_value6
    extra_tag6: extra_value6
```

message:

```json
{
  "values": {
    "indoor": {
      "value": 23.5,
      "extra_tag0": "extra_value0",
      "extra_tag2": "extra_value0"
    }
  },
  "extra_tag0": "extra_value1",
  "extra_tag1": "extra_value1"
}
```

Resulting tags:

* `extra_tag0=extra_value0`
* `extra_tag1=extra_value1`
* `extra_tag2=extra_value2`
* `extra_tag3=extra_value3`
* `extra_tag4=extra_value4`
* `extra_tag5=extra_value5`
* `extra_tag6=extra_value6`

### Example with value removal

Sometimes you want to hardcode a value for a specific topic, but you don't want to hardcode it for all topics that
match. In this case you can use `null` value in the override config. This will remove the value from the override and
will retain the value obtained from the message.

```yaml
dt/myapp/room/esp32/temperature:indoor:
  extra_tags:
    extra_tag2: null
dt/myapp/room/esp32/temperature:
  extra_tags:
    extra_tag2: extra_value3
```

message:

```json
{
  "values": {
    "indoor": {
      "value": 23.5,
      "extra_tag0": "extra_value0",
      "extra_tag2": "extra_value0"
    }
  },
  "extra_tag0": "extra_value1",
  "extra_tag1": "extra_value1"
}
```

Resulting tags:

* `extra_tag0=extra_value0`
* `extra_tag1=extra_value1`
* `extra_tag2=extra_value0`

All the following configurations will result in the same tags as above:

```yaml
dt/myapp/room/esp32/temperature:indoor:
  extra_tags: null
dt/myapp/room/esp32/temperature:
  extra_tags:
    extra_tag2: extra_value3
```

```yaml
dt/myapp/room/esp32/temperature:indoor: null
dt/myapp/room/esp32/temperature:
  extra_tags:
    extra_tag2: extra_value3
```

```yaml
dt/myapp/room/esp32/temperature: null
dt/myapp/room/esp32/+:
  extra_tags:
    extra_tag2: extra_value3
```