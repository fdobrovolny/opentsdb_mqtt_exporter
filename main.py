import asyncio
import json
import logging
import re
import time
from typing import List, Tuple, Dict, Optional, Union

import configargparse
import yaml
from aiomqtt import Client, MqttError, Message, TLSParameters
from opentsdb import TSDBClient

# Regex to match MQTT topic structure
TOPIC_MATCHER = re.compile(
    r"^dt/(?P<app>[\w-]+)/(?P<context>[\w\-/]+)/(?P<thing>[\w-]+)/(?P<property>[\w-]+)$"
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
):
    """
    Asynchronously processes messages from a queue and sends them to TSDB.

    :param queue: The asyncio Queue to process messages from.
    :param tsdb: TSDBClient instance to send data to TSDB.
    :param override: Dictionary for overriding topic metadata.
    :param max_time: Maximum time interval for sending messages.
    :param max_send_messages: Maximum number of messages to process in one batch.
    :param metric_prefix: Metric prefix.
    """
    time_limit = asyncio.get_event_loop().time() + max_time

    while True:
        items_to_process: List[Tuple[Message, float]] = []

        while True:
            try:
                item = await asyncio.wait_for(
                    queue.get(),
                    timeout=max(0, int(time_limit - asyncio.get_event_loop().time())),
                )
                items_to_process.append(item)
                queue.task_done()

                if len(items_to_process) >= max_send_messages:
                    break
            except asyncio.TimeoutError:
                break

        if len(items_to_process) > 0:
            logger.debug(f"Processing {len(items_to_process)} items")
            process_items(items_to_process, tsdb, override, metric_prefix)

        remaining_time = time_limit - asyncio.get_event_loop().time()
        if remaining_time < 0:
            time_limit = asyncio.get_event_loop().time() + max_time


def process_items(
    items: List[Tuple[Message, float]],
    tsdb: TSDBClient,
    override: Optional[Dict[str, Dict[str, Union[str, Dict[str, str]]]]] = None,
    metric_prefix: str = "mqtt__",
):
    """
    Processes a batch of MQTT messages and sends data to TSDB.

    :param items: List of tuples containing MQTT Message and timestamp.
    :param tsdb: TSDBClient instance to send data to TSDB.
    :param override: Dictionary for overriding topic metadata.
    :param metric_prefix: Metric prefix.
    """
    if override is None:
        override = {}

    for message, timestamp in items:
        topic, payload = message.topic.value, message.payload
        tags, value = extract_tags_and_value(topic, payload, timestamp, override)

        if value is not None:
            try:
                tsdb.send(f"{metric_prefix}{tags['property']}", value, **tags)
            except Exception as e:
                logger.error(f"Error processing item: {e}", exc_info=True)


def extract_tags_and_value(
    topic: str,
    payload: Union[str, bytes],
    timestamp: float,
    override: Dict[str, Dict[str, Union[str, Dict[str, str]]]],
) -> Tuple[Dict[str, str], Optional[Union[int, float]]]:
    """
    Extracts tags and value from the topic and payload.

    :param topic: MQTT topic string.
    :param payload: MQTT message payload.
    :param timestamp: Timestamp when the message was received.
    :param override: Dictionary for overriding topic metadata.
    :return: Tuple containing dictionary of tags and extracted value.
    """
    payload_tags, value = extract_payload_tags_and_value(payload)
    tags = extract_tags(topic, timestamp, override)

    # Merging payload tags with existing tags
    payload_tags.update(tags)

    return payload_tags, value


def extract_payload_tags_and_value(
    payload: Union[str, bytes]
) -> Tuple[Dict[str, str], Optional[Union[int, float]]]:
    """
    Extracts tags and value from the payload if it's JSON, otherwise tries to parse it as a number.

    :param payload: MQTT message payload.
    :return: Tuple containing dictionary of payload tags and extracted value.
    """
    payload_tags = {}

    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8")

    payload = payload.strip()
    if payload.startswith("{") and payload.endswith("}"):
        try:
            payload_data = json.loads(payload)
            value = payload_data.pop("value", -1)

            # Use remaining payload data as tags
            payload_tags.update(
                {
                    k: str(v)
                    for k, v in payload_data.items()
                    if isinstance(v, (str, int, float))
                }
            )

            return payload_tags, value
        except ValueError:
            logger.error(f"Could not parse payload as JSON: {payload}")
            return {}, None
    else:
        try:
            return {}, int(payload)
        except ValueError:
            try:
                return {}, float(payload)
            except ValueError:
                logger.error(f"Could not parse payload as number: {payload}")
                return {}, None


def extract_tags(
    topic: str,
    timestamp: float,
    override: Dict[str, Dict[str, Union[str, Dict[str, str]]]],
) -> Dict[str, str]:
    """
    Extracts and constructs tags from the topic and payload.

    :param topic: MQTT topic string.
    :param timestamp: Timestamp when the message was received.
    :param override: Dictionary for overriding topic metadata.
    :return: Dictionary of tags.
    """
    override_dict = override.get(topic, {})
    topic_meta = TOPIC_MATCHER.match(topic)
    if not topic_meta:
        raise ValueError(f"Could not parse topic: {topic}")

    tags = {
        "topic": topic,
        "app": override_dict.get("app", topic_meta.group("app")),
        "context": override_dict.get("context", topic_meta.group("context")),
        "thing": override_dict.get("thing", topic_meta.group("thing")),
        "property": override_dict.get("property", topic_meta.group("property")),
        "timestamp": timestamp,
        **extract_context_tags(
            override_dict.get("context", topic_meta.group("context"))
        ),
    }

    if "extra_tags" in override_dict:
        tags.update(override_dict["extra_tags"])

    return tags


def extract_context_tags(context: str) -> Dict[str, str]:
    """
    Extracts context tags from the context string.

    :param context: Context string from the topic.
    :return: Dictionary of context tags.
    """
    context_tags = {}
    if "/" in context:
        context_data = context.split("/")
        for i, context_part in enumerate(context_data):
            context_tags[f"context_{i}"] = context_part
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
        help="MQTT topic to subscribe",
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
        help="Add host tag to TSDB data",
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

    tsdb = TSDBClient(
        args.tsdb_host,
        port=args.tsdb_port,
        host_tag=args.add_host_tag,
        static_tags=args.static_tags,
        uri=args.tsdb_uri,
        victoria_metrics=True,
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
            ),
        )
    except MqttError as e:
        logger.error(f"MQTT error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
