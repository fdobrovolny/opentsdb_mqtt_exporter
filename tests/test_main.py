import json
import time
import unittest
from unittest import mock

from aiomqtt.client import Message

from main import process_items


def get_message(topic, payload, timestamp=None):
    return (
        Message(
            topic=topic, payload=payload, qos=0, retain=False, mid=0, properties=None
        ),
        timestamp if timestamp else time.time(),
    )


def single_value_test(
    message_value,
    value,
    topic="dt/myapp/room/esp32/temperature",
    msg_topic=None,
    metric_name_suffix="temperature",
    property_name=None,
    override=None,
    extra_tags=None,
    app="myapp",
    context="room",
    thing="esp32",
    context_0="room",
    timestamp=None,
    msg_timestamp=None,
    called_once=True,
    no_context=False,
    msg_metric_prefix=None,
    metric_prefix="mqtt_exp__",
    max_str_len=128,
    tags_exclude=None,
):
    if extra_tags is None:
        extra_tags = {}
    tsdb_mock = mock.MagicMock()
    message = get_message(
        topic if msg_topic is None else msg_topic, message_value, msg_timestamp
    )
    process_items(
        [message],
        tsdb_mock,
        override=override if override is not None else {},
        metric_prefix=metric_prefix if msg_metric_prefix is None else msg_metric_prefix,
        max_str_len=max_str_len,
        tags_exclude={"metric_prefix"} if tags_exclude is None else tags_exclude,
        disable_cache=True,
    )
    if no_context:
        context_values = {}
    else:
        context_values = {
            "app": app,
            "context": context,
            "thing": thing,
            "context_0": context_0,
        }

    if called_once:
        tsdb_mock.send.assert_called_once_with(
            f"{metric_prefix}{metric_name_suffix}",
            value,
            topic=topic,
            property=metric_name_suffix if property_name is None else property_name,
            timestamp=int(message[1]) if timestamp is None else timestamp,
            **extra_tags,
            **context_values,
        )
    else:
        tsdb_mock.send.assert_not_called()


class TestProcessItems(unittest.TestCase):
    def test_int(self):
        single_value_test("25", 25)

    def test_float(self):
        single_value_test("25.5", 25.5)

    def test_string(self):
        single_value_test(
            "hello world",
            1,
            extra_tags={"val": "hello world"},
            metric_name_suffix="temperature_info",
            property_name="temperature",
        )

    def test_string_max_len(self):
        single_value_test(
            "a" * 129,
            1,
            extra_tags={"val": "a" * 128},
            metric_name_suffix="temperature_info",
            property_name="temperature",
        )

    def test_binary_int(self):
        single_value_test(b"25", 25)

    def test_binary_float(self):
        single_value_test(b"25.5", 25.5)

    def test_binary_string(self):
        single_value_test(
            b"hello world",
            1,
            extra_tags={"val": "hello world"},
            metric_name_suffix="temperature_info",
            property_name="temperature",
        )

    def test_binary_int_padded(self):
        single_value_test(b"\x0025\x00\x00\x00\x00", 25)

    def test_binary_float_padded(self):
        single_value_test(b"\x0025.5\x00\x00\x00\x00", 25.5)

    def test_binary_string_padded(self):
        single_value_test(
            b"\x00hello world\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            1,
            extra_tags={"val": "hello world"},
            metric_name_suffix="temperature_info",
            property_name="temperature",
        )

    def test_json_int(self):
        single_value_test('{"value": 25}', 25)

    def test_json_float(self):
        single_value_test('{"value": 25.5}', 25.5)

    def test_json_binary_int(self):
        single_value_test(b'{"value": 25}', 25)

    def test_json_binary_float(self):
        single_value_test(b'{"value": 25.5}', 25.5)

    def test_json_binary_int_padded(self):
        single_value_test(b'\x00\x00{"value": 25}\x00\x00\x00\x00', 25)

    def test_json_binary_float_padded(self):
        single_value_test(b'\x00\x00{"value": 25.5}\x00\x00\x00\x00', 25.5)

    def test_json_empty(self):
        single_value_test("{}", -1)

    def test_json_binary_empty(self):
        single_value_test(b"{}", -1)

    def test_json_empty_padded(self):
        single_value_test(b"\x00\x00{}\x00\x00\x00\x00", -1)

    def test_json_payload_str(self):
        single_value_test(
            json.dumps({"value": "twenty_three_point_five"}),
            value=1,
            extra_tags={"val": "twenty_three_point_five"},
            metric_name_suffix="temperature_info",
            property_name="temperature",
        )

    def test_json_payload_bool_true(self):
        single_value_test(
            json.dumps({"value": True}),
            value=1,
        )

    def test_json_payload_bool_false(self):
        single_value_test(
            json.dumps({"value": False}),
            value=0,
        )

    def test_json_additional_tags(self):
        single_value_test(
            '{"value": 25,"tag1": "value1", "tag2": "value2"}',
            25,
            extra_tags={"tag1": "value1", "tag2": "value2"},
        )

    def test_json_timestamp(self):
        single_value_test(
            '{"value": 25,"timestamp": 123456789}', 25, timestamp=123456789
        )

    def test_json_str_timestamp(self):
        single_value_test(
            '{"value": 25,"timestamp": "123456789"}', 25, timestamp=123456789
        )

    def test_json_float_timestamp(self):
        single_value_test(
            '{"value": 25,"timestamp": 123456789.4}', 25, timestamp=123456789.4
        )

    def test_incorrect_json_timestamp(self):
        single_value_test(
            '{"value": 25,"timestamp": "foo"}',
            25,
            timestamp=123456789,
            msg_timestamp=123456789,
        )

    def test_json_topic_context(self):
        single_value_test(
            '{"value": 25}',
            25,
            topic="dt/myapp/myroom/esp32/temperature",
            context_0="myroom",
            context="myroom",
        )

    def test_json_topic_context_nested(self):
        single_value_test(
            '{"value": 25}',
            25,
            topic="dt/myapp/mainbuilding/first_floor/myoffice/esp32/temperature",
            context_0="mainbuilding",
            context="mainbuilding/first_floor/myoffice",
            extra_tags={"context_1": "first_floor", "context_2": "myoffice"},
        )

    def test_json_override_extra_tags(self):
        single_value_test(
            '{"value": 25,"tag1": "value1", "tag2": "value2"}',
            25,
            extra_tags={"tag1": "value3", "tag2": "value2"},
            override={
                "dt/myapp/room/esp32/temperature": {"extra_tags": {"tag1": "value3"}}
            },
        )

    def test_json_override_context(self):
        single_value_test(
            '{"value": 25}',
            25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "context": "mainbuilding/first_floor/myoffice",
                }
            },
            context_0="mainbuilding",
            context="mainbuilding/first_floor/myoffice",
            extra_tags={"context_1": "first_floor", "context_2": "myoffice"},
        )

    def test_json_override_metric_property(self):
        single_value_test(
            '{"value": 25}',
            25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "property": "humidity",
                }
            },
            metric_name_suffix="humidity",
            property_name="humidity",
        )

    def test_json_override_app(self):
        single_value_test(
            '{"value": 25}',
            25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "app": "myapp2",
                }
            },
            app="myapp2",
        )

    def test_json_override_thing(self):
        single_value_test(
            '{"value": 25}',
            25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "thing": "esp32_2",
                }
            },
            thing="esp32_2",
        )

    def test_incorrect_topic(self):
        # incorrect topic structure that doesn't match the expected regex
        single_value_test(
            "25",
            25,
            topic="incorrect/topic/structure",
            metric_name_suffix="structure",
            no_context=True,
        )

    def test_incorrect_topic_override_context(self):
        # incorrect topic structure that doesn't match the expected regex
        single_value_test(
            "25",
            25,
            topic="incorrect/topic/structure",
            metric_name_suffix="structure",
            no_context=True,
            override={
                "incorrect/topic/structure": {
                    "context": "mainbuilding/first_floor/myoffice",
                }
            },
            extra_tags={
                "context_0": "mainbuilding",
                "context_1": "first_floor",
                "context_2": "myoffice",
                "context": "mainbuilding/first_floor/myoffice",
            },
        )

    def test_incorrect_topic_override_app(self):
        # incorrect topic structure that doesn't match the expected regex
        single_value_test(
            "25",
            25,
            topic="incorrect/topic/structure",
            metric_name_suffix="structure",
            no_context=True,
            override={
                "incorrect/topic/structure": {
                    "app": "myapp2",
                }
            },
            extra_tags={"app": "myapp2"},
        )

    def test_incorrect_topic_override_thing(self):
        # incorrect topic structure that doesn't match the expected regex
        single_value_test(
            "25",
            25,
            topic="incorrect/topic/structure",
            metric_name_suffix="structure",
            no_context=True,
            override={
                "incorrect/topic/structure": {
                    "thing": "esp32_2",
                }
            },
            extra_tags={"thing": "esp32_2"},
        )

    def test_incorrect_topic_values_dict(self):
        # incorrect topic structure that doesn't match the expected regex
        single_value_test(
            '{"values": {"indoor": {"value": 25}}}',
            25,
            topic="incorrect/topic/structure:indoor",
            msg_topic="incorrect/topic/structure",
            metric_name_suffix="structure",
            no_context=True,
        )

    def test_incorrect_json_payload_dict(self):
        # 'value' field in json payload as string instead of number
        single_value_test(
            json.dumps({"value": {"key": "value"}}),
            value=-1,
        )

    def test_incorrect_json(self):
        single_value_test(
            '{"value":}',
            value=1,
            extra_tags={"val": '{"value":}'},
            metric_name_suffix="temperature_info",
            property_name="temperature",
        )

    def test_topic_name_padded(self):
        single_value_test("25", 25, msg_topic="dt/myapp/room/esp32/temperature\x00\x00")

    def test_json_multi_value_int(self):
        single_value_test(
            '{"indoor": 25}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={"dt/myapp/room/esp32/temperature": {"json_multi_value": True}},
            metric_name_suffix="temperature_indoor",
        )

    def test_json_multi_value_float(self):
        single_value_test(
            '{"indoor": 25.5}',
            25.5,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={"dt/myapp/room/esp32/temperature": {"json_multi_value": True}},
            metric_name_suffix="temperature_indoor",
        )

    def test_json_multi_value_str_int(self):
        single_value_test(
            '{"indoor": "25"}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={"dt/myapp/room/esp32/temperature": {"json_multi_value": True}},
            metric_name_suffix="temperature_indoor",
        )

    def test_json_multi_value_str_float(self):
        single_value_test(
            '{"indoor": "25.5"}',
            25.5,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={"dt/myapp/room/esp32/temperature": {"json_multi_value": True}},
            metric_name_suffix="temperature_indoor",
        )

    def test_json_multi_value_str(self):
        single_value_test(
            '{"indoor": "foo bar"}',
            1,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={
                "dt/myapp/room/esp32/temperature": {
                    "json_multi_value": True,
                }
            },
            metric_name_suffix="temperature_indoor_info",
            property_name="temperature_indoor",
            extra_tags={"val": "foo bar"},
        )

    def test_json_list_multi_value_str_int(self):
        single_value_test(
            '[{"indoor": "25"}]',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={"dt/myapp/room/esp32/temperature": {"json_multi_value": True}},
            metric_name_suffix="temperature_indoor",
        )

    def test_json_list_multi_value_str_bool(self):
        single_value_test(
            '[{"indoor": true}]',
            1,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={"dt/myapp/room/esp32/temperature": {"json_multi_value": True}},
            metric_name_suffix="temperature_indoor",
        )

    def test_json_multi_value_dict(self):
        single_value_test(
            '{"indoor": {"value": 25.5}}',
            25.5,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={"dt/myapp/room/esp32/temperature": {"json_multi_value": True}},
            metric_name_suffix="temperature_indoor",
        )

    def test_json_multi_value_dict_with_timestamp(self):
        single_value_test(
            '{"indoor": {"value": 25.5, "timestamp": 123456789}}',
            25.5,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={"dt/myapp/room/esp32/temperature": {"json_multi_value": True}},
            metric_name_suffix="temperature_indoor",
            timestamp=123456789,
        )

    def test_json_multi_value_dict_with_tags(self):
        single_value_test(
            '{"indoor": {"value": 25.5, "tag1": "value1"}}',
            25.5,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            override={"dt/myapp/room/esp32/temperature": {"json_multi_value": True}},
            metric_name_suffix="temperature_indoor",
            extra_tags={"tag1": "value1"},
        )

    def test_json_list(self):
        single_value_test(
            '[{"value": 25}]',
            25,
        )

    def test_json_values_list(self):
        single_value_test(
            '{"values": [{"value": 25}]}',
            25,
        )

    def test_json_values_list_common_tags(self):
        single_value_test(
            '{"values": [{"value": 25}], "tag1": "value1"}',
            25,
            extra_tags={"tag1": "value1"},
        )

    def test_json_values_list_specific_tags(self):
        single_value_test(
            '{"values": [{"value": 25, "tag1": "value1"}]}',
            25,
            extra_tags={"tag1": "value1"},
        )

    def test_json_values_list_specific_and_common_tags(self):
        single_value_test(
            '{"values": [{"value": 25, "tag1": "value1"}], "tag2": "value2"}',
            25,
            extra_tags={"tag1": "value1", "tag2": "value2"},
        )

    def test_json_values_list_specific_and_common_tags_local_override(self):
        single_value_test(
            '{"values": [{"value": 25, "tag1": "value2"}], "tag1": "value1"}',
            25,
            extra_tags={"tag1": "value2"},
        )

    def test_json_values_list_specific_and_common_tags_override(self):
        single_value_test(
            '{"values": [{"value": 25, "tag1": "value1"}], "tag2": "value2"}',
            25,
            extra_tags={"tag1": "value1", "tag2": "value3"},
            override={
                "dt/myapp/room/esp32/temperature": {"extra_tags": {"tag2": "value3"}}
            },
        )

    def test_json_values_dict(self):
        single_value_test(
            '{"values": {"indoor": {"value": 25}}}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
        )

    def test_json_values_dict_common_tags(self):
        single_value_test(
            '{"values": {"indoor": {"value": 25}}, "tag1": "value1"}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
            extra_tags={"tag1": "value1"},
        )

    def test_json_values_dict_specific_tags(self):
        single_value_test(
            '{"values": {"indoor": {"value": 25, "tag1": "value1"}}}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
            extra_tags={"tag1": "value1"},
        )

    def test_json_values_dict_specific_and_common_tags(self):
        single_value_test(
            '{"values": {"indoor": {"value": 25, "tag1": "value1"}}, "tag2": "value2"}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
            extra_tags={"tag1": "value1", "tag2": "value2"},
        )

    def test_json_values_dict_common_tags_local_override(self):
        single_value_test(
            '{"values": {"indoor": {"value": 25, "tag1": "value2"}}, "tag1": "value1"}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
            extra_tags={"tag1": "value2"},
        )

    def test_json_values_dict_specific_and_common_tags_override(self):
        single_value_test(
            '{"values": {"indoor": {"value": 25, "tag1": "value1"}}, "tag2": "value2"}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
            extra_tags={"tag1": "value1", "tag2": "value3"},
            override={
                "dt/myapp/room/esp32/temperature:indoor": {
                    "extra_tags": {"tag2": "value3"}
                }
            },
        )

    def test_multi_subscription_override_app(self):
        single_value_test(
            '{"value": 25}',
            25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "app": "myapp2",
                },
                "dt/myapp/room/esp32/+": {
                    "app": "myapp3",
                },
            },
            app="myapp2",
        )

    def test_multi_subscription_override_app_removal(self):
        single_value_test(
            '{"value": 25}',
            25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "app": None,
                },
                "dt/myapp/room/esp32/+": {
                    "app": "myapp3",
                },
            },
        )

    def test_multi_subscription_override_app_full_removal(self):
        single_value_test(
            '{"value": 25}',
            25,
            override={
                "dt/myapp/room/esp32/temperature": None,
                "dt/myapp/room/esp32/+": {
                    "app": "myapp3",
                },
            },
        )

    def test_multi_subscription_values_override_app_removal(self):
        single_value_test(
            '{"values": {"indoor":{"value": 25}}}',
            25,
            override={
                "dt/myapp/room/esp32/temperature:indoor": {
                    "app": None,
                },
                "dt/myapp/room/esp32/+": {
                    "app": "myapp3",
                },
            },
            metric_name_suffix="temperature_indoor",
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
        )

    def test_multi_subscription_values_override_app_full_removal(self):
        single_value_test(
            '{"values": {"indoor":{"value": 25}}}',
            25,
            override={
                "dt/myapp/room/esp32/temperature:indoor": None,
                "dt/myapp/room/esp32/+": {
                    "app": "myapp3",
                },
            },
            metric_name_suffix="temperature_indoor",
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
        )

    def test_multi_subscription_override_tags(self):
        single_value_test(
            '{"value": 25, "tag0": "value1", "tag1": "value1"}',
            25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "extra_tags": {"tag1": "value2", "tag2": "value2"},
                },
                "dt/myapp/room/esp32/+": {
                    "extra_tags": {
                        "tag1": "value3",
                        "tag2": "value3",
                        "tag3": "value3",
                    },
                },
                "dt/myapp/room/esp32/#": {
                    "extra_tags": {
                        "tag1": "value4",
                        "tag2": "value4",
                        "tag3": "value4",
                        "tag4": "value4",
                    },
                },
                "dt/myapp/+/esp32/#": {
                    "extra_tags": {
                        "tag1": "value5",
                        "tag2": "value5",
                        "tag3": "value5",
                        "tag4": "value5",
                        "tag5": "value5",
                    },
                },
                "dt/#": {
                    "extra_tags": {
                        "tag1": "value6",
                        "tag2": "value6",
                        "tag3": "value6",
                        "tag4": "value6",
                        "tag5": "value6",
                        "tag6": "value6",
                    },
                },
            },
            extra_tags={
                "tag0": "value1",
                "tag1": "value2",
                "tag2": "value2",
                "tag3": "value3",
                "tag4": "value4",
                "tag5": "value5",
                "tag6": "value6",
            },
        )

    def test_multi_subscription_override_tags_sub_value(self):
        single_value_test(
            message_value='{"values": {"indoor": {"value": 25, "tag0": "value0"}}, "tag0": "value1","tag1": "value1", "tag2": "value1"}',
            value=25,
            override={
                "dt/myapp/room/esp32/temperature:indoor": {
                    "extra_tags": {"tag2": "value2"},
                },
                "dt/myapp/room/esp32/temperature": {
                    "extra_tags": {"tag2": "value3", "tag3": "value3"},
                },
                "dt/myapp/room/esp32/+": {
                    "extra_tags": {
                        "tag2": "value4",
                        "tag3": "value4",
                        "tag4": "value4",
                    },
                },
                "dt/myapp/room/esp32/#": {
                    "extra_tags": {
                        "tag2": "value5",
                        "tag3": "value5",
                        "tag4": "value5",
                        "tag5": "value5",
                    },
                },
                "dt/#": {
                    "extra_tags": {
                        "tag2": "value6",
                        "tag3": "value6",
                        "tag4": "value6",
                        "tag5": "value6",
                        "tag6": "value6",
                    },
                },
            },
            extra_tags={
                "tag0": "value0",
                "tag1": "value1",
                "tag2": "value2",
                "tag3": "value3",
                "tag4": "value4",
                "tag5": "value5",
                "tag6": "value6",
            },
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
        )

    def test_multi_subscription_override_extra_tag_removal(self):
        single_value_test(
            message_value='{"value": 25, "tag0": "value1","tag1": "value1"}',
            value=25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "extra_tags": {"tag1": None},
                },
                "dt/myapp/room/esp32/+": {
                    "extra_tags": {"tag1": "value3"},
                },
            },
            extra_tags={
                "tag0": "value1",
                "tag1": "value1",
            },
        )

    def test_multi_subscription_override_value_extra_tags_removal(self):
        single_value_test(
            message_value='{"value": 25, "tag0": "value1","tag1": "value1"}',
            value=25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "extra_tags": None,
                },
                "dt/myapp/room/esp32/+": {
                    "extra_tags": {"tag1": "value3"},
                },
            },
            extra_tags={
                "tag0": "value1",
                "tag1": "value1",
            },
        )

    def test_multi_subscription_override_value_extra_tags_full_removal(self):
        single_value_test(
            message_value='{"value": 25, "tag0": "value1","tag1": "value1"}',
            value=25,
            override={
                "dt/myapp/room/esp32/temperature": None,
                "dt/myapp/room/esp32/+": {
                    "extra_tags": {"tag1": "value3"},
                },
            },
            extra_tags={
                "tag0": "value1",
                "tag1": "value1",
            },
        )

    def test_multi_subscription_override_sub_value_removal(self):
        single_value_test(
            message_value='{"values": {"indoor": {"value": 25, "tag0": "value0"}}, "tag0": "value1","tag1": "value1"}',
            value=25,
            override={
                "dt/myapp/room/esp32/temperature:indoor": {
                    "extra_tags": {"tag2": None, "tag3": "value2"},
                },
                "dt/myapp/room/esp32/temperature": {
                    "extra_tags": {
                        "tag2": "value3",
                        "tag3": "value3",
                        "tag4": "value3",
                    },
                },
            },
            extra_tags={
                "tag0": "value0",
                "tag1": "value1",
                "tag3": "value2",
                "tag4": "value3",
            },
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
        )

    def test_multi_subscription_override_sub_value_extra_tags_removal(self):
        single_value_test(
            message_value='{"values": {"indoor": {"value": 25, "tag0": "value0"}}, "tag0": "value1","tag1": "value1"}',
            value=25,
            override={
                "dt/myapp/room/esp32/temperature:indoor": {
                    "extra_tags": None,
                },
                "dt/myapp/room/esp32/temperature": {
                    "extra_tags": {"tag2": "value3"},
                },
            },
            extra_tags={
                "tag0": "value0",
                "tag1": "value1",
            },
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
        )

    def test_multi_subscription_override_sub_value_extra_tags_full_removal(self):
        single_value_test(
            message_value='{"values": {"indoor": {"value": 25, "tag0": "value0"}}, "tag0": "value1","tag1": "value1"}',
            value=25,
            override={
                "dt/myapp/room/esp32/temperature:indoor": None,
                "dt/myapp/room/esp32/temperature": {
                    "extra_tags": {"tag2": "value3"},
                },
            },
            extra_tags={
                "tag0": "value0",
                "tag1": "value1",
            },
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
        )

    def test_metrix_prefix_override(self):
        single_value_test(
            '{"value": 25}',
            25,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "metric_prefix": "myapp2__",
                }
            },
            msg_metric_prefix="mqtt__",
            metric_prefix="myapp2__",
        )

    def test_metric_with_spaces(self):
        single_value_test(
            '{"value": 25}',
            25,
            topic="dt/myapp/room/esp32/temperature with spaces",
            metric_name_suffix="temperature_with_spaces",
        )

    def test_metric_sub_value_with_spaces(self):
        single_value_test(
            '{"values": {"indoor sensor": {"value": 25}}}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor sensor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor_sensor",
        )

    def test_metric_prefix_in_message(self):
        single_value_test(
            '{"value": 25, "metric_prefix": "myapp2__"}',
            25,
        )

    def test_metric_prefix_in_message_without_exclude(self):
        single_value_test(
            '{"value": 25, "metric_prefix": "myapp2__"}',
            25,
            tags_exclude={},
            metric_prefix="myapp2__",
        )

    def test_tags_exclude(self):
        single_value_test(
            '{"value": 25, "extra_tag": "foo"}',
            25,
            tags_exclude={"extra_tag"},
        )

    def test_metric_sub_values_dict_int(self):
        single_value_test(
            '{"values": {"indoor": 25}}',
            25,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
        )

    def test_metric_sub_values_dict_float(self):
        single_value_test(
            '{"values": {"indoor": 25.5}}',
            25.5,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
        )

    def test_metric_sub_values_dict_str_int(self):
        single_value_test(
            '{"values": {"indoor": "42"}}',
            42,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
        )

    def test_metric_sub_values_dict_str_float(self):
        single_value_test(
            '{"values": {"indoor": "42.5"}}',
            42.5,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor",
        )

    def test_metric_sub_values_dict_str(self):
        single_value_test(
            '{"values": {"indoor": "foo"}}',
            1,
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            metric_name_suffix="temperature_indoor_info",
            property_name="temperature_indoor",
            extra_tags={"val": "foo"},
        )

    def test_metric_sub_values_dict_with_common_tags(self):
        single_value_test(
            '{"values": {"indoor":25}, "tag1": "value1"}',
            25,
            extra_tags={"tag1": "value1"},
            topic="dt/myapp/room/esp32/temperature:indoor",
            msg_topic="dt/myapp/room/esp32/temperature",
            property_name="temperature_indoor",
            metric_name_suffix="temperature_indoor",
        )

    def test_metric_sub_values_list_int(self):
        single_value_test(
            '{"values": [25]}',
            25,
        )

    def test_metric_sub_values_list_float(self):
        single_value_test(
            '{"values": [25.5]}',
            25.5,
        )

    def test_metric_sub_values_list_str_int(self):
        single_value_test(
            '{"values": ["25"]}',
            25,
        )

    def test_metric_sub_values_list_str_float(self):
        single_value_test(
            '{"values": ["25.5"]}',
            25.5,
        )

    def test_metric_sub_values_list_str(self):
        single_value_test(
            '{"values": ["foo"]}',
            1,
            metric_name_suffix="temperature_info",
            property_name="temperature",
            extra_tags={"val": "foo"},
        )

    def test_metric_values_list_int(self):
        single_value_test(
            "[25]",
            25,
        )

    def test_metric_values_list_float(self):
        single_value_test(
            "[25.5]",
            25.5,
        )

    def test_metric_values_list_str_int(self):
        single_value_test(
            '["25"]',
            25,
        )

    def test_metric_values_list_str_float(self):
        single_value_test(
            '["25.5"]',
            25.5,
        )

    def test_metric_values_list_str(self):
        single_value_test(
            '["foo"]',
            1,
            metric_name_suffix="temperature_info",
            property_name="temperature",
            extra_tags={"val": "foo"},
        )

    def test_replacement_value_str_int(self):
        single_value_test(
            '{"value": "foo"}',
            42,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        "foo": 42,
                    }
                }
            },
        )

    def test_replacement_value_str_str(self):
        single_value_test(
            '{"value": "foo"}',
            1,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        "foo": "bar",
                    }
                }
            },
            metric_name_suffix="temperature_info",
            property_name="temperature",
            extra_tags={"val": "bar"},
        )

    def test_replacement_value_str_float(self):
        single_value_test(
            '{"value": "foo"}',
            14.5,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        "foo": 14.5,
                    }
                }
            },
        )

    def test_replacement_value_int_int(self):
        single_value_test(
            '{"value": 1}',
            42,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        1: 42,
                    }
                }
            },
        )

    def test_replacement_value_int_str(self):
        single_value_test(
            '{"value": 2}',
            1,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        2: "bar",
                    }
                }
            },
            metric_name_suffix="temperature_info",
            property_name="temperature",
            extra_tags={"val": "bar"},
        )

    def test_replacement_value_int_float(self):
        single_value_test(
            '{"value": 1}',
            14.5,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        1: 14.5,
                    }
                }
            },
        )

    def test_replacement_value_str_int_int(self):
        single_value_test(
            '{"value": 1}',
            42,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        "1": 42,
                    }
                }
            },
        )

    def test_replacement_value_str_int_str(self):
        single_value_test(
            '{"value": 2}',
            1,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        "2": "bar",
                    }
                }
            },
            metric_name_suffix="temperature_info",
            property_name="temperature",
            extra_tags={"val": "bar"},
        )

    def test_replacement_value_str_int_float(self):
        single_value_test(
            '{"value": 1}',
            14.5,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        "1": 14.5,
                    }
                }
            },
        )

    def test_replacement_str_value_int_int(self):
        single_value_test(
            '{"value": "1"}',
            42,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        1: 42,
                    }
                }
            },
        )

    def test_replacement_str_value_int_str(self):
        single_value_test(
            '{"value": "2"}',
            1,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        2: "bar",
                    }
                }
            },
            metric_name_suffix="temperature_info",
            property_name="temperature",
            extra_tags={"val": "bar"},
        )

    def test_replacement_str_value_int_float(self):
        single_value_test(
            '{"value": "1"}',
            14.5,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        1: 14.5,
                    }
                }
            },
        )

    def test_replacement_value_float_int(self):
        single_value_test(
            '{"value": 2.5}',
            42,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        2.5: 42,
                    }
                }
            },
        )

    def test_replacement_value_float_str(self):
        single_value_test(
            '{"value": 2.5}',
            1,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        2.5: "bar",
                    }
                }
            },
            metric_name_suffix="temperature_info",
            property_name="temperature",
            extra_tags={"val": "bar"},
        )

    def test_replacement_value_float_float(self):
        single_value_test(
            '{"value": 2.5}',
            14.5,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        2.5: 14.5,
                    }
                }
            },
        )

    def test_replacement_str_value_float_int(self):
        single_value_test(
            '{"value": "2.5"}',
            42,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        2.5: 42,
                    }
                }
            },
        )

    def test_replacement_str_value_float_str(self):
        single_value_test(
            '{"value": "2.5"}',
            1,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        2.5: "bar",
                    }
                }
            },
            metric_name_suffix="temperature_info",
            property_name="temperature",
            extra_tags={"val": "bar"},
        )

    def test_replacement_str_value_float_float(self):
        single_value_test(
            '{"value": "2.5"}',
            14.5,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        2.5: 14.5,
                    }
                }
            },
        )

    def test_replacement_value_str_float_int(self):
        single_value_test(
            '{"value": 2.5}',
            42,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        "2.5": 42,
                    }
                }
            },
        )

    def test_replacement_value_str_float_str(self):
        single_value_test(
            '{"value": 2.5}',
            1,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        "2.5": "bar",
                    }
                }
            },
            metric_name_suffix="temperature_info",
            property_name="temperature",
            extra_tags={"val": "bar"},
        )

    def test_replacement_value_str_float_float(self):
        single_value_test(
            '{"value": 2.5}',
            14.5,
            override={
                "dt/myapp/room/esp32/temperature": {
                    "value_replacement": {
                        "2.5": 14.5,
                    }
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
