import asyncio
import functools
import json
import logging
import re
import time
from itertools import chain
from typing import List, Tuple, Dict, Optional, Union, Set

import configargparse
import yaml
from aiomqtt import Client, MqttError, Message, TLSParameters
from opentsdb import TSDBClient
from paho.mqtt.client import topic_matches_sub

# Regex to match MQTT topic structure
TOPIC_MATCHER = re.compile(
    r"^dt/(?P<app>[ \w-]+)/(?P<context>[ \w\-/]+)/(?P<thing>[ \w-]+)/(?P<property>[ \w-]+)?$"
)

# Configure the logger
logger = logging.getLogger(__name__)

VALUES_KEY = "values"
VALUE_KEY = "value"
TIMESTAMP_KEY = "timestamp"
EXTRA_TAGS_KEY = "extra_tags"
VALUE_REPLACEMENT_KEY = "value_replacement"
JSON_MULTI_VALUE_KEY = "json_multi_value"
METRIC_PREFIX_KEY = "metric_prefix"
PROPERTY_KEY = "property"
TOPIC_KEY = "topic"

STRING_LABEL_SUFFIX = "_info"
DEFAULT_METRIC_PREFIX = "mqtt__"


async def subscriber(
    queue: asyncio.Queue,
    broker: str,
    topic: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    port: int = 1883,
    client_id: Optional[str] = None,
    root_ca: Optional[str] = None,
    client_cert: Optional[str] = None,
    client_key: Optional[str] = None,
):
    """
    Asynchronously subscribes to MQTT messages and adds them to a queue.

    :param queue: The asyncio Queue to add messages to.
    :param broker: Address of the MQTT broker.
    :param topic: MQTT topic to subscribe to.
    :param username: Optional username for MQTT broker authentication.
    :param password: Optional password for MQTT broker authentication.
    :param port: Port of the MQTT broker.
    :param client_id: Optional client ID for MQTT connection.
    :param root_ca: Path to the root CA certificate for SSL/TLS connection.
    :param client_cert: Path to the client certificate for SSL/TLS connection.
    :param client_key: Path to the client key for SSL/TLS connection.
    """
    tls_params = {}
    if root_ca:
        tls_params["ca_certs"] = root_ca
    if client_cert:
        tls_params["certfile"] = client_cert
    if client_key:
        tls_params["keyfile"] = client_key

    client_config = {
        "hostname": broker,
        "port": port,
        "username": username,
        "password": password,
        "client_id": client_id,
        "tls_params": TLSParameters(**tls_params) if len(tls_params) > 0 else None,
    }

    logger.info(f"Connecting to {broker}:{port}")

    async with Client(**client_config) as client:
        async with client.messages() as messages:
            for topic in topic.split(","):
                await client.subscribe(topic)
            logger.info(f"Subscribed to {topic}")
            async for message in messages:
                logger.debug(f"Received message: {message.topic} {message.payload}")
                await queue.put((message, time.time()))


async def writer(
    queue: asyncio.Queue,
    tsdb: TSDBClient,
    override: Optional[Dict[str, Dict[str, Union[str, Dict[str, str]]]]] = None,
    max_time: int = 5,
    max_send_messages: int = 100,
    metric_prefix: str = DEFAULT_METRIC_PREFIX,
    max_str_len: int = 128,
    tags_exclude: Optional[Set[str]] = None,
):
    """
    Asynchronously processes messages from a queue and sends them to TSDB.

    :param queue: The asyncio Queue to process messages from.
    :param tsdb: TSDBClient instance to send data to TSDB.
    :param override: Dictionary for overriding topic metadata.
    :param max_time: Maximum time interval for sending messages.
    :param max_send_messages: Maximum number of messages to process in one batch.
    :param metric_prefix: Metric prefix.
    :param max_str_len: Maximum string length for TSDB tags.
    :param tags_exclude: Tags to be removed from TSDB data.
    """
    if tags_exclude is None:
        tags_exclude = {METRIC_PREFIX_KEY}

    time_limit = asyncio.get_event_loop().time() + max_time

    while True:
        items_to_process: List[Tuple[Message, float]] = []

        while True:
            try:
                item = await asyncio.wait_for(
                    queue.get(),
                    timeout=max(0, time_limit - asyncio.get_event_loop().time()),
                )
                items_to_process.append(item)
                queue.task_done()

                if len(items_to_process) >= max_send_messages:
                    break
            except asyncio.TimeoutError:
                break

        if len(items_to_process) > 0:
            logger.debug(f"Processing {len(items_to_process)} items")
            process_items(
                items_to_process,
                tsdb,
                override,
                metric_prefix,
                max_str_len,
                tags_exclude,
            )

        if (time_limit - asyncio.get_event_loop().time()) < 0.1:
            time_limit = asyncio.get_event_loop().time() + max_time


def sort_subs_by_specificity(topic: str, subs: List[str]) -> List[str]:
    """
    Sorts and filters a list of subscriptions by specificity.

    We try to guess some heuristic based on the string length and the position of the wildcards.

    :param topic: Topic string.
    :param subs: List of subscriptions.
    :return: List of subscriptions sorted by specificity.
    """
    matches = []

    for sub in subs:
        if topic_matches_sub(sub, topic):
            # Factor in the position of the wildcards
            specificity = sum(
                [
                    (1 if c not in ["+", "#"] else 0.6 if c == "+" else 0.5)
                    * (index + 1)
                    for index, c in enumerate(sub)
                ]
            )
            matches.append((specificity, sub))
    matches.sort(key=lambda x: x[0])

    return [m[1] for m in matches]


def get_topic_override_config(
    topic: Union[str, Tuple[str, str]],
    override: Dict[str, Dict[str, Union[str, Dict[str, str]]]],
    disable_cache: bool = False,
    lru_cache_size: int = 10240,
):
    """
    Gets the override configuration for a topic.

    :param topic: Topic string.
    :param override: Dictionary for overriding topic metadata.
    :param disable_cache: Disable the LRU cache.
    :param lru_cache_size: Size of the LRU cache.
    :return: Override configuration for the topic.

    :note: This function uses an LRU cache to speed up the lookup of the override configuration. The cache key is
           generated from the topic and the `override_cache_key` parameter. You can clear the cache by calling
           `get_topic_override_config.cache_clear()`. Or get the cache info by calling
           `get_topic_override_config.cache_info()`.
    """

    if disable_cache or not hasattr(get_topic_override_config, "_get_override"):

        def _get_override(topic):
            output_override = {}
            output_extra_tags = {}
            output_value_replacement = {}

            if isinstance(topic, tuple):
                output_override = _get_override(topic[0])
                output_extra_tags = output_override.pop(EXTRA_TAGS_KEY, {})
                output_value_replacement = output_override.pop(
                    VALUE_REPLACEMENT_KEY, {}
                )

                sub_override = override.get(f"{topic[0]}:{topic[1]}", {})

                if sub_override is None:
                    # If the sub is set to None, reset the override
                    return {}

                sub_override_extra_tags = sub_override.pop(EXTRA_TAGS_KEY, {})
                if sub_override_extra_tags is not None:
                    output_extra_tags.update(sub_override_extra_tags)
                else:
                    # Reset extra tags if the sub has extra tags set to None
                    output_extra_tags = {}

                sub_value_replacement = sub_override.pop(VALUE_REPLACEMENT_KEY, {})
                if sub_value_replacement is not None:
                    output_value_replacement.update(sub_value_replacement)
                else:
                    # Reset value replacement if the sub has value replacement set to None
                    output_value_replacement = {}

                output_override.update(sub_override)

            else:
                subs = list(override.keys())
                subs = sort_subs_by_specificity(topic, subs)

                for sub in subs:
                    sub_override_raw = override.get(sub, {})
                    if sub_override_raw is None:
                        # reset override if the sub has override set to None
                        output_override = {}
                        output_extra_tags = {}
                        output_value_replacement = {}
                        continue
                    sub_override = {**sub_override_raw}  # Shallow copy of the dict

                    sub_override_extra_tags = sub_override.pop(EXTRA_TAGS_KEY, {})
                    if sub_override_extra_tags is not None:
                        output_extra_tags.update(sub_override_extra_tags)
                    else:
                        # Reset extra tags if the sub has extra tags set to None
                        output_extra_tags = {}

                    sub_value_replacement = sub_override.pop(VALUE_REPLACEMENT_KEY, {})
                    if sub_value_replacement is not None:
                        output_value_replacement.update(sub_value_replacement)
                    else:
                        # Reset value replacement if the sub has value replacement set to None
                        output_value_replacement = {}

                    output_override.update(sub_override)

            if len(output_extra_tags) > 0:
                output_override[EXTRA_TAGS_KEY] = output_extra_tags

            if len(output_value_replacement) > 0:
                output_override[VALUE_REPLACEMENT_KEY] = output_value_replacement

            return {k: v for k, v in output_override.items() if v is not None}

        if disable_cache:
            return _get_override(topic)

        if not hasattr(get_topic_override_config, "_get_override"):
            get_topic_override_config._get_override = functools.lru_cache(
                maxsize=lru_cache_size, typed=True
            )(_get_override)
            get_topic_override_config.cache_clear = (
                get_topic_override_config._get_override.cache_clear
            )
            get_topic_override_config.cache_info = (
                get_topic_override_config._get_override.cache_info
            )

    return get_topic_override_config._get_override(topic)


def process_items(
    items: List[Tuple[Message, float]],
    tsdb: TSDBClient,
    override: Optional[Dict[str, Dict[str, Union[str, Dict[str, str]]]]] = None,
    metric_prefix: str = DEFAULT_METRIC_PREFIX,
    max_str_len: int = 128,
    tags_exclude: Set[str] = None,
    disable_cache: bool = False,
):
    """
    Processes a batch of MQTT messages and sends data to TSDB.

    :param items: List of tuples containing MQTT Message and timestamp.
    :param tsdb: TSDBClient instance to send data to TSDB.
    :param override: Dictionary for overriding topic metadata.
    :param metric_prefix: Metric prefix.
    :param max_str_len: Maximum string length for TSDB tags.
    :param tags_exclude: Tags to be removed from TSDB data.
    :param disable_cache: Disable the LRU cache.
    """
    if tags_exclude is None:
        tags_exclude = {METRIC_PREFIX_KEY}

    if override is None:
        override = {}

    for message, timestamp in items:
        topic, payload = message.topic.value.strip().strip("\x00"), message.payload

        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8", "ignore")

        # Remove trailing null bytes
        payload = payload.strip().strip("\x00")

        values = [(topic, payload)]

        topic_override = get_topic_override_config(
            topic, override, disable_cache=disable_cache
        )

        if (payload.startswith("{") and payload.endswith("}")) or (
            payload.startswith("[") and payload.endswith("]")
        ):
            try:
                payload = json.loads(payload)
            except ValueError as e:
                logger.debug(
                    f"Could not parse payload as JSON falling back to str '{e}' : {payload}",
                    exc_info=True,
                )
            else:
                if isinstance(payload, dict):
                    if isinstance(payload.get(VALUES_KEY, None), dict):
                        base_tags = {**payload}
                        base_tags.pop(VALUES_KEY)

                        values = [
                            (
                                (topic, k),
                                {**base_tags, **v}
                                if isinstance(v, dict)
                                else {**base_tags, VALUE_KEY: v},
                            )
                            for k, v in payload[VALUES_KEY].items()
                        ]
                    elif isinstance(payload.get(VALUES_KEY, None), list):
                        base_tags = {**payload}
                        base_tags.pop(VALUES_KEY)

                        values = [
                            (
                                topic,
                                {**base_tags, **v}
                                if isinstance(v, dict)
                                else {**base_tags, VALUE_KEY: v},
                            )
                            for v in payload[VALUES_KEY]
                        ]
                    else:
                        if topic_override.get(JSON_MULTI_VALUE_KEY, None) is True:
                            values = chain.from_iterable(
                                [
                                    [((topic, k), v)]
                                    if not isinstance(v, list)
                                    else [((topic, k), iv) for iv in v]
                                    for k, v in payload.items()
                                ]
                            )
                        else:
                            values = [(topic, payload)]
                elif isinstance(payload, list):
                    if topic_override.get(JSON_MULTI_VALUE_KEY, None) is True:
                        values = chain.from_iterable(
                            [[((topic, k), v) for k, v in iv.items()] for iv in payload]
                        )
                    else:
                        values = [(topic, v) for v in payload]
                else:
                    logger.debug(
                        f"Could not parse payload as JSON falling back to str: {payload}"
                    )

        for val_topic, val_payload in values:
            tags, value = extract_tags_and_value(
                val_topic,
                val_payload,
                timestamp,
                (
                    get_topic_override_config(
                        val_topic, override, disable_cache=disable_cache
                    )
                    if isinstance(val_topic, tuple)
                    else topic_override
                ),
                tags_exclude,
                max_str_len=max_str_len,
            )

            local_metric_prefix = tags.pop(METRIC_PREFIX_KEY, metric_prefix)

            if value is not None:
                try:
                    if isinstance(value, (int, float)):
                        tsdb.send(
                            f"{local_metric_prefix}{tags[PROPERTY_KEY]}", value, **tags
                        )
                    elif isinstance(value, str):
                        tsdb.send(
                            f"{local_metric_prefix}{tags[PROPERTY_KEY]}{STRING_LABEL_SUFFIX}",
                            1,
                            **tags,
                            val=value,
                        )
                    else:
                        logger.error(
                            f"Could not parse payload as number or string on topic {topic}: {value}"
                        )
                except Exception as e:
                    logger.error(
                        f"Error processing item '{value}' on topic {topic}: {e}",
                        exc_info=True,
                    )
            else:
                # If value is None, send value as string as we were unable to parse it
                tsdb.send(
                    f"{local_metric_prefix}{tags[PROPERTY_KEY]}{STRING_LABEL_SUFFIX}",
                    1,
                    **tags,
                    val=normalize_value(str(val_payload), topic_override, max_str_len),
                )


def extract_tags_and_value(
    topic: str,
    payload: str,
    timestamp: float,
    topic_override: Dict[str, Union[str, Dict[str, str]]],
    tags_exclude: Set[str],
    max_str_len: int = 128,
) -> Tuple[Dict[str, Union[str, int]], Optional[Union[int, float]]]:
    """
    Extracts tags and value from the topic and payload.

    :param topic: MQTT topic string.
    :param payload: MQTT message payload or part of the payload specific to a single value.
    :param timestamp: Timestamp when the message was received.
    :param topic_override: Override configuration for the topic.
    :param tags_exclude: Tags to be removed from TSDB data.
    :param max_str_len: Maximum string length for TSDB tags.
    :return: Tuple containing dictionary of tags and extracted value.
    """
    payload_tags, value = extract_payload_tags_and_value(
        payload, tags_exclude, topic_override, max_str_len
    )
    tags = extract_tags(topic, topic_override)

    # Merging payload tags with existing tags
    payload_tags.update(tags)

    if payload_tags.get(TIMESTAMP_KEY, None) is None:
        payload_tags[TIMESTAMP_KEY] = int(timestamp)

    return payload_tags, value


def normalize_timestamp(
    timestamp: Union[str, int, float]
) -> Optional[Union[int, float]]:
    """
    Tries to parse the timestamp as a number.

    :param timestamp: Timestamp when the message was received.
    :return: Parsed timestamp or None if it could not be parsed.
    """
    if isinstance(timestamp, (int, float)):
        return timestamp
    elif isinstance(timestamp, str):
        try:
            return int(timestamp)
        except ValueError:
            try:
                return float(timestamp)
            except ValueError:
                logger.warning(f"Could not parse timestamp as number: {timestamp}")
                return None
        except TypeError as e:
            logger.warning(
                f"Could not parse timestamp as number {e}: {timestamp}",
                exc_info=True,
            )
            return None
    else:
        logger.warning(f"Could not parse timestamp as number: {timestamp}")
        return None


def extract_payload_tags_and_value(
    payload: Union[str, bytes, Dict[str, Union[str, int, float]]],
    tags_exclude: Set[str],
    topic_override: Dict[str, Union[str, Dict[str, str]]],
    max_str_len: int = 128,
) -> Tuple[Dict[str, str], Optional[Union[int, float, str]]]:
    """
    Extracts tags and value from the payload if it's JSON, otherwise tries to parse it as a number.

    :param payload: MQTT message payload.
    :param tags_exclude: Tags to be removed from TSDB data.
    :param topic_override: Override configuration for the topic.
    :param max_str_len: Maximum string length for TSDB tags.
    :return: Tuple containing dictionary of payload tags and extracted value.
    """
    payload_tags = {}

    if isinstance(payload, dict):
        try:
            value = payload.pop(VALUE_KEY, -1)

            # Use remaining payload data as tags
            payload_tags.update(
                {
                    k: str(v) if k != TIMESTAMP_KEY else normalize_timestamp(v)
                    for k, v in payload.items()
                    if isinstance(v, (str, int, float))
                    and k.lower() not in tags_exclude
                }
            )

            return payload_tags, normalize_value(value, topic_override, max_str_len)
        except ValueError:
            logger.debug(
                f"Could not parse payload as JSON falling back to str: {payload}"
            )
            return {}, None
    else:
        return payload_tags, normalize_value(payload, topic_override, max_str_len)


def find_value_replacement(
    value: Union[str, int, float],
    value_replacement_dict: Dict[Union[str, int, float], Union[str, int, float]],
    max_str_len: int = 128,
) -> Optional[Union[int, float, str]]:
    """
    Tries to find a value replacement for the given value.

    :param value: MQTT message value or JSON value.
    :param value_replacement_dict: Value replacement dictionary.
    :param max_str_len: Maximum string length for TSDB tags.
    :return:
    """
    if len(value_replacement_dict) > 0:
        if isinstance(value, str):
            if value[:max_str_len] in value_replacement_dict:
                return value_replacement_dict[value[:max_str_len]]

        if isinstance(value, (int, float)):
            if value in value_replacement_dict:
                return value_replacement_dict[value]
            elif str(value) in value_replacement_dict:
                return value_replacement_dict[str(value)]
            elif (
                isinstance(value, float)
                and value == int(value)
                and int(value) in value_replacement_dict
            ):
                return value_replacement_dict[int(value)]
            elif (
                isinstance(value, float)
                and value == int(value)
                and str(int(value)) in value_replacement_dict
            ):
                return value_replacement_dict[str(int(value))]
            elif isinstance(value, int) and float(value) in value_replacement_dict:
                return value_replacement_dict[float(value)]
            elif isinstance(value, int) and str(float(value)) in value_replacement_dict:
                return value_replacement_dict[str(float(value))]

        if isinstance(value, bool):
            if value in value_replacement_dict:
                return value_replacement_dict[value]

    return None


def normalize_value(
    value: Union[str, int, float, bool],
    topic_override: Dict[str, Union[str, Dict[str, str]]],
    max_str_len: int = 128,
) -> Optional[Union[int, float, str]]:
    """
    Tries to parse the value as a number.

    :param value: MQTT message payload or JSON value.
    :param topic_override: Override configuration for the topic.
    :param max_str_len: Maximum string length for TSDB tags.
    :return: Parsed value or None if it could not be parsed.
    """
    value_replacement_dict = topic_override.get(VALUE_REPLACEMENT_KEY, {})
    was_replaced = False

    if len(value_replacement_dict) > 0:
        replacement_value = find_value_replacement(
            value, value_replacement_dict, max_str_len
        )
        if replacement_value is not None:
            if isinstance(replacement_value, (int, float, str, bool)):
                value = replacement_value
                was_replaced = True
            else:
                logger.warning(
                    f"Could not parse replacement value!: {replacement_value}"
                )
                return -1

    if isinstance(value, (int, float)):
        return value
    try:
        value = int(value)
    except ValueError:
        try:
            value = float(value)
        except ValueError:
            if isinstance(value, str):
                return value[:max_str_len]
            logger.warning(f"Could not parse payload as number or str: {value}")
            return -1
    except TypeError:
        logger.warning(f"Could not parse payload as number or str: {value}")
        return -1

    if was_replaced:
        return value
    else:
        return normalize_value(value, topic_override, max_str_len)


def generate_tags(
    keys: List[str],
    override_dict: Dict[str, Dict[str, Union[str, Dict[str, str]]]],
    topic_meta: Optional[re.Match],
) -> Dict[str, str]:
    """
    Generate tags from a list of given keys.
    :param keys: The list of keys to be added.
    :param override_dict: Dictionary to override the default value.
    :param topic_meta: Regular Expression match object.
    :return: Dictionary of tags.
    """
    if not topic_meta:
        return {
            key: override_dict.get(key, None)
            for key in keys
            if override_dict.get(key, None) is not None
        }
    else:
        return {
            key: (
                override_dict.get(key)
                if override_dict.get(key, None) is not None
                else topic_meta.group(key)
            )
            for key in keys
        }


def extract_tags(
    topic: Union[str, Tuple[str, str]],
    topic_override: Union[str, Dict[str, str]],
) -> Dict[str, str]:
    """
    Extracts and constructs tags from the topic and payload.
    :param topic: MQTT topic string.
    :param topic_override: Override configuration for the topic.
    :return: Dictionary of tags.
    """
    topic_meta = TOPIC_MATCHER.match(topic[0] if isinstance(topic, tuple) else topic)

    # construct generated tags
    generated_tags = generate_tags(
        ["app", "context", "thing"], topic_override, topic_meta
    )

    if not topic_meta:
        raw_topic = topic[0] if isinstance(topic, tuple) else topic
        property_tag = raw_topic.split("/")[-1].replace(" ", "_")
    else:
        property_tag = (
            f"{topic_meta.group(PROPERTY_KEY)}_{topic[1]}"
            if isinstance(topic, tuple)
            else topic_meta.group(PROPERTY_KEY)
        ).replace(" ", "_")

    tags = {
        TOPIC_KEY: f"{topic[0]}:{topic[1]}" if isinstance(topic, tuple) else topic,
        PROPERTY_KEY: topic_override.get(PROPERTY_KEY, property_tag),
        **generated_tags,
        **extract_context_tags(
            topic_override.get(
                "context", topic_meta.group("context") if topic_meta else None
            )
        ),
    }

    if EXTRA_TAGS_KEY in topic_override:
        tags.update(
            {
                key: value
                for key, value in topic_override[EXTRA_TAGS_KEY].items()
                if value is not None
            }
        )

    if METRIC_PREFIX_KEY in topic_override:
        tags[METRIC_PREFIX_KEY] = topic_override[METRIC_PREFIX_KEY]

    return tags


def extract_context_tags(
    context: Optional[str], key: str = "context"
) -> Dict[str, str]:
    """
    Extracts context tags from the context string.

    :param context: Context string from the topic.
    :param key: Key prefix for the context tags.
    :return: Dictionary of context tags.
    """
    if context is None:
        return {}

    context_tags = {}
    if "/" in context:
        context_data = context.split("/")
        for i, context_part in enumerate(context_data):
            context_tags[f"{key}_{i}"] = context_part
    else:
        context_tags[f"{key}_0"] = context
    return context_tags


def parse_args():
    parser = configargparse.ArgParser(
        default_config_files=["config.yaml", "config.yml"]
    )
    parser.add_argument(
        "-c",
        "--config",
        is_config_file=True,
        help="Path to the YAML configuration file",
    )
    parser.add_argument(
        "--max_send_messages",
        type=int,
        default=100,
        env_var="MAX_SEND_MESSAGES",
        help="Maximum number of messages to send",
    )
    parser.add_argument(
        "--max_time",
        type=int,
        default=5,
        env_var="MAX_TIME",
        help="Maximum time interval for sending messages",
    )
    parser.add_argument(
        "--broker",
        type=str,
        env_var="MQTT_BROKER",
        required=True,
        help="MQTT broker address",
    )
    parser.add_argument(
        "--port", type=int, default=1883, env_var="MQTT_PORT", help="MQTT broker port"
    )
    parser.add_argument(
        "--client_id",
        type=str,
        env_var="MQTT_CLIENT_ID",
        help="MQTT client ID",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="dt/#",
        env_var="MQTT_TOPIC",
        help="MQTT topics to subscribe to, `,` coma separated list, you can "
        "use wildcards. Default: dt/#",
    )
    parser.add_argument(
        "--username", type=str, env_var="MQTT_USERNAME", help="MQTT username"
    )
    parser.add_argument(
        "--password", type=str, env_var="MQTT_PASSWORD", help="MQTT password"
    )
    parser.add_argument(
        "--root_ca",
        type=str,
        env_var="MQTT_ROOT_CA",
        help="Path to root CA certificate",
    )
    parser.add_argument(
        "--client_cert",
        type=str,
        env_var="MQTT_CLIENT_CERT",
        help="Path to client certificate",
    )
    parser.add_argument(
        "--client_key",
        type=str,
        env_var="MQTT_CLIENT_KEY",
        help="Path to client key",
    )
    parser.add_argument(
        "--tsdb_host",
        type=str,
        default="127.0.0.1",
        env_var="OPEN_TSDB_HOST",
        help="OpenTSDB host",
    )
    parser.add_argument(
        "--tsdb_port",
        type=int,
        default=4242,
        env_var="OPEN_TSDB_PORT",
        help="OpenTSDB port",
    )
    parser.add_argument(
        "--tsdb_uri", type=str, env_var="OPEN_TSDB_URI", help="OpenTSDB URI"
    )
    parser.add_argument(
        "--override_config",
        type=str,
        env_var="OVERRIDE_CONFIG",
        help="Path to the YAML override configuration file",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        env_var="LOG_LEVEL",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    parser.add_argument(
        "--add_host_tag",
        type=bool,
        default=False,
        env_var="ADD_HOST_TAG",
        help="Add host tag to TSDB data, bool value, default: False",
    )
    parser.add_argument(
        "--static_tags",
        type=json.loads,
        default={},
        env_var="STATIC_TAGS",
        help="Static tags for TSDB in JSON format",
    )
    parser.add_argument(
        "--metric_prefix",
        type=str,
        default=DEFAULT_METRIC_PREFIX,
        env_var=METRIC_PREFIX_KEY,
        help="Metric prefix",
    )
    parser.add_argument(
        "--victoria_metrics",
        type=bool,
        default=False,
        env_var="VICTORIA_METRICS",
        help="Use VictoriaMetrics instead of OpenTSDB, does not return detail data. bool value, default: False",
    )
    parser.add_argument(
        "--max_str_len",
        type=int,
        default=128,
        env_var="MAX_STR_LEN",
        help="Maximum string length for TSDB tags, default: 128",
    )
    parser.add_argument(
        "--tags_exclude",
        type=str,
        default=METRIC_PREFIX_KEY,
        env_var="TAGS_EXCLUDE",
        help="Tags to be removed from TSDB data, comma separated list, case insensitive default: metric_prefix",
    )

    return parser.parse_args()


def load_yaml_file(file_path: str) -> Dict:
    """
    Loads a YAML configuration file.

    This function opens a YAML file, reads its contents, and returns a dictionary
    representing the configuration settings contained in the file.

    :param file_path: The path to the YAML configuration file.
    :return: A dictionary containing the configuration settings.
    """
    with open(file_path, "r") as file:
        # yaml.safe_load converts the YAML file content into a Python dictionary
        return yaml.safe_load(file)


def configure_logging(log_level: str):
    """
    Configures the logging level for the application.

    This function sets the logging level based on the string value provided.
    The level controls the threshold for the logging messages that will be displayed.

    :param log_level: A string representing the desired logging level.
                      Valid options are 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.
    :raises ValueError: If the provided log_level is not a valid logging level.
    """
    # Convert the log level string to its corresponding numeric value
    numeric_level = getattr(logging, log_level.upper(), None)

    # Check if the log level string is valid and convert it to the numeric value
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    # Configure the basic logging level for the entire application
    logging.basicConfig(level=numeric_level, force=True)
    logger.info(f"Logging level set to {log_level}")


async def main():
    args = parse_args()

    configure_logging(args.log_level)

    if args.override_config:
        override = load_yaml_file(args.override_config)
    else:
        override = {}

    queue = asyncio.Queue()

    if args.victoria_metrics:
        victoria_metrics = {
            "victoria_metrics": True,
        }
    else:
        victoria_metrics = {}

    tsdb = TSDBClient(
        args.tsdb_host,
        port=args.tsdb_port,
        host_tag=args.add_host_tag,
        static_tags=args.static_tags,
        uri=args.tsdb_uri,
        **victoria_metrics,
    )

    try:
        await asyncio.gather(
            subscriber(
                queue,
                args.broker,
                args.topic,
                args.username,
                args.password,
                args.port,
                args.client_id,
                args.root_ca,
                args.client_cert,
                args.client_key,
            ),
            writer(
                queue,
                tsdb,
                override,
                args.max_time,
                args.max_send_messages,
                args.metric_prefix,
                args.max_str_len,
                set(args.tags_exclude.split(",")),
            ),
        )
    except MqttError as e:
        logger.error(f"MQTT error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
