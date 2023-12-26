import asyncio
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
    r"^dt/(?P<app>[ \w-]+)/(?P<context>[ \w\-/]+)/(?P<thing>[ \w-]+)/(?P<property>[ \w-]+)(:(?P<sub_value>[ \w-]+))?$"
)

# Configure the logger
logger = logging.getLogger(__name__)


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
    metric_prefix: str = "mqtt__",
    max_str_len: int = 128,
    tags_exclude: Set[str] = {"metric_prefix"},
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
    topic: str, override: Dict[str, Dict[str, Union[str, Dict[str, str]]]]
):
    """
    Gets the override configuration for a topic.

    :param topic: Topic string.
    :param override: Dictionary for overriding topic metadata.
    :return: Override configuration for the topic.
    """
    subs = list(override.keys())
    subs = sort_subs_by_specificity(topic, subs)

    output_override = {}
    output_extra_tags = {}

    for sub in subs:
        sub_override = {**override.get(sub, {})}
        output_extra_tags.update(sub_override.pop("extra_tags", {}))
        output_override.update(sub_override)

    if len(output_extra_tags) > 0:
        output_override["extra_tags"] = output_extra_tags

    return output_override


def process_items(
    items: List[Tuple[Message, float]],
    tsdb: TSDBClient,
    override: Optional[Dict[str, Dict[str, Union[str, Dict[str, str]]]]] = None,
    metric_prefix: str = "mqtt__",
    max_str_len: int = 128,
    tags_exclude: Set[str] = {"metric_prefix"},
):
    """
    Processes a batch of MQTT messages and sends data to TSDB.

    :param items: List of tuples containing MQTT Message and timestamp.
    :param tsdb: TSDBClient instance to send data to TSDB.
    :param override: Dictionary for overriding topic metadata.
    :param metric_prefix: Metric prefix.
    :param max_str_len: Maximum string length for TSDB tags.
    :param tags_exclude: Tags to be removed from TSDB data.
    """
    if override is None:
        override = {}

    for message, timestamp in items:
        topic, payload = message.topic.value.strip().strip("\x00"), message.payload

        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8", "ignore")

        # Remove trailing null bytes
        payload = payload.strip().strip("\x00")

        values = [(topic, payload)]

        topic_override = get_topic_override_config(topic, override)

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
                    if isinstance(payload.get("values", None), dict):
                        base_tags = {**payload}
                        base_tags.pop("values")

                        values = [
                            (
                                f"{topic}:{k}",
                                {**base_tags, **v}
                                if isinstance(v, dict)
                                else {**base_tags, "value": v},
                            )
                            for k, v in payload["values"].items()
                        ]
                    elif isinstance(payload.get("values", None), list):
                        base_tags = {**payload}
                        base_tags.pop("values")

                        values = [
                            (
                                topic,
                                {**base_tags, **v}
                                if isinstance(v, dict)
                                else {**base_tags, "value": v},
                            )
                            for v in payload["values"]
                        ]
                    else:
                        if (
                            override.get(topic, {}).get("json_multi_value", None)
                            is True
                        ):
                            values = chain.from_iterable(
                                [
                                    [(f"{topic}:{k}", v)]
                                    if not isinstance(v, list)
                                    else [(f"{topic}:{k}", iv) for iv in v]
                                    for k, v in payload.items()
                                ]
                            )
                        else:
                            values = [(topic, payload)]
                elif isinstance(payload, list):
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
                override,
                topic_override,
                tags_exclude,
            )

            local_metric_prefix = tags.pop("metric_prefix", metric_prefix)

            if value is not None:
                try:
                    if isinstance(value, (int, float)):
                        tsdb.send(
                            f"{local_metric_prefix}{tags['property']}", value, **tags
                        )
                    elif isinstance(value, str):
                        tsdb.send(
                            f"{local_metric_prefix}{tags['property']}_info",
                            1,
                            **tags,
                            value=value[:max_str_len],
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
                    f"{local_metric_prefix}{tags['property']}_info",
                    1,
                    **tags,
                    value=value[:max_str_len],
                )


def extract_tags_and_value(
    topic: str,
    payload: Union[str, bytes],
    timestamp: float,
    override: Dict[str, Dict[str, Union[str, Dict[str, str]]]],
    topic_override: Dict[str, Union[str, Dict[str, str]]],
    tags_exclude: Set[str],
) -> Tuple[Dict[str, Union[str, int]], Optional[Union[int, float]]]:
    """
    Extracts tags and value from the topic and payload.

    :param topic: MQTT topic string.
    :param payload: MQTT message payload.
    :param timestamp: Timestamp when the message was received.
    :param override: Dictionary for overriding topic metadata.
    :param topic_override: Override configuration for the topic.
    :param tags_exclude: Tags to be removed from TSDB data.
    :return: Tuple containing dictionary of tags and extracted value.
    """
    payload_tags, value = extract_payload_tags_and_value(payload, tags_exclude)
    tags = extract_tags(topic, override, topic_override)

    # Merging payload tags with existing tags
    payload_tags.update(tags)

    if "timestamp" not in payload_tags:
        payload_tags["timestamp"] = int(timestamp)
    else:
        if isinstance(payload_tags["timestamp"], str):
            try:
                payload_tags["timestamp"] = int(payload_tags["timestamp"])
            except ValueError:
                try:
                    payload_tags["timestamp"] = float(payload_tags["timestamp"])
                except ValueError:
                    logger.warning(
                        f"Could not parse timestamp as number: {payload_tags['timestamp']}"
                    )
                    payload_tags["timestamp"] = int(timestamp)
            except TypeError as e:
                logger.warning(
                    f"Could not parse timestamp as number {e}: {payload_tags['timestamp']}",
                    exc_info=True,
                )
                payload_tags["timestamp"] = int(timestamp)
        elif not isinstance(payload_tags["timestamp"], (int, float)):
            logger.warning(
                f"Could not parse timestamp as number: {payload_tags['timestamp']}"
            )
            payload_tags["timestamp"] = int(timestamp)

    return payload_tags, value


def extract_payload_tags_and_value(
    payload: Union[str, bytes, Dict[str, Union[str, int, float]]],
    tags_exclude: Set[str],
) -> Tuple[Dict[str, str], Optional[Union[int, float, str]]]:
    """
    Extracts tags and value from the payload if it's JSON, otherwise tries to parse it as a number.

    :param payload: MQTT message payload.
    :param tags_exclude: Tags to be removed from TSDB data.
    :return: Tuple containing dictionary of payload tags and extracted value.
    """
    payload_tags = {}

    if isinstance(payload, dict):
        try:
            value = payload.pop("value", -1)

            # Use remaining payload data as tags
            payload_tags.update(
                {
                    k: str(v) if k != "timestamp" else v
                    for k, v in payload.items()
                    if isinstance(v, (str, int, float))
                    and k.lower() not in tags_exclude
                }
            )

            return payload_tags, normalize_value(value)
        except ValueError:
            logger.debug(
                f"Could not parse payload as JSON falling back to str: {payload}"
            )
            return {}, None
    else:
        return payload_tags, normalize_value(payload)


def normalize_value(value: Union[str, int, float]) -> Optional[Union[int, float, str]]:
    """
    Tries to parse the value as a number.

    :param value: MQTT message payload or JSON value.
    :return: Parsed value or None if it could not be parsed.
    """
    if isinstance(value, (int, float)):
        return value
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            if isinstance(value, str):
                return value
            logger.error(f"Could not parse payload as number or str: {value}")
            return -1
    except TypeError:
        logger.error(f"Could not parse payload as number or str: {value}")
        return -1


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
    topic: str,
    override: Dict[str, Dict[str, Union[str, Dict[str, str]]]],
    topic_override: Union[str, Dict[str, str]],
) -> Dict[str, str]:
    """
    Extracts and constructs tags from the topic and payload.
    :param topic: MQTT topic string.
    :param override: Dictionary for overriding topic metadata.
    :param topic_override: Override configuration for the topic.
    :return: Dictionary of tags.
    """
    if ":" in topic:
        override_dict = {**topic_override, **override.get(topic, {})}
        extra_tags = {
            **topic_override.get("extra_tags", {}),
            **override.get(topic, {}).get("extra_tags", {}),
        }
        if len(extra_tags) > 0:
            override_dict["extra_tags"] = extra_tags
    else:
        override_dict = topic_override
    topic_meta = TOPIC_MATCHER.match(topic)

    # construct generated tags
    generated_tags = generate_tags(
        ["app", "context", "thing"], override_dict, topic_meta
    )

    if not topic_meta:
        raw_topic = topic if ":" not in topic else topic.split(":")[0]
        property_tag = raw_topic.split("/")[-1].replace(" ", "_")
    else:
        property_tag = (
            f"{topic_meta.group('property')}_{topic_meta.group('sub_value')}"
            if topic_meta.group("sub_value")
            else topic_meta.group("property")
        ).replace(" ", "_")

    tags = {
        "topic": topic,
        "property": override_dict.get("property", property_tag),
        **generated_tags,
        **extract_context_tags(
            override_dict.get(
                "context", topic_meta.group("context") if topic_meta else None
            )
        ),
    }

    if "extra_tags" in override_dict:
        tags.update(
            {
                key: value
                for key, value in override_dict["extra_tags"].items()
                if value is not None
            }
        )

    if "metric_prefix" in override_dict:
        tags["metric_prefix"] = override_dict["metric_prefix"]

    return tags


def extract_context_tags(context: Optional[str]) -> Dict[str, str]:
    """
    Extracts context tags from the context string.

    :param context: Context string from the topic.
    :return: Dictionary of context tags.
    """
    if context is None:
        return {}

    context_tags = {}
    if "/" in context:
        context_data = context.split("/")
        for i, context_part in enumerate(context_data):
            context_tags[f"context_{i}"] = context_part
    else:
        context_tags["context_0"] = context
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
        default="mqtt__",
        env_var="METRIC_PREFIX",
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
        default="metric_prefix",
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
