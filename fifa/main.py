import sys
import os
import logging
import json

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
)

from operators.frame_generator_function import FrameGeneratorFunction
from operators.game_time_detector_function import GameTimeDetectorFunction
from operators.paddle_ocr_detector_function import PaddleOcrDetectorFunction
from operators.video_shot_detector_function import VideoShotDetectorFunction
from operators.video_wipe_detector_function import VideoWipeDetectorFunction
from operators.detector_collector_function import (
    DetectorCollectorFunction, DetectorReducerFunction, DetectorAggregatorFunction
)


def main():
    print("Creating Execution Environment")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.disable_operator_chaining()
    env.set_parallelism(1)

    # add dependencies
    print("Adding dependencies")
    # python_dep = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'udfs')
    # env.add_python_file(file_path=python_dep)
    data_dep = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'videos.zip')
    env.add_python_archive(archive_path=data_dep, target_dir="videos")

    # add kafka connector dependency
    print("Adding Kafka connector dependency")
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka-1.16.0.jar')
    env.add_jars("file://{}".format(kafka_jar))

    # consume data from kafka source topic
    print("Setting up Kafka source")

    # kafka_consumer = KafkaSource.builder() \
    #     .set_bootstrap_servers("broker:29092") \
    #     .set_topics("test-source-topic") \
    #     .set_group_id("test_group") \
    #     .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    #     .set_value_only_deserializer(SimpleStringSchema()) \
    #     .build()
    #
    # ds_main = env.from_source(kafka_consumer, WatermarkStrategy.no_watermarks(), "kafka-source")

    # source topic
    #    {"recording_id": 1, "filename": "index_720p30_00001.ts"},
    #    {"recording_id": 2, "filename": "index_720p30_00002.ts"},

    ds_main = env.from_collection([
       {"recording_id": 1, "filename": "index_720p30_00001.ts"},
       {"recording_id": 2, "filename": "index_720p30_00002.ts"},
    ])

    # perform transformation
    print("Setting up operations")
    # topology definition
    ds_frames = (
        ds_main  # input datastream from source
        .process(FrameGeneratorFunction()).name("frame_generator")  # pass to frame extracting operator
    )
    # pass ds_frames to multiple detectors in parallel
    ds_game_time = (
        ds_frames
        .key_by(lambda x: x["recording_id"])  # partition by the "recording_id" field
        .process(GameTimeDetectorFunction()).name("game_time_detector")
        .set_parallelism(2)
    )
    ds_paddle_ocr = (
        ds_frames
        .process(PaddleOcrDetectorFunction()).name("paddle_ocr_detector")
        .set_parallelism(2)
    )
    ds_video_shot = (
        ds_frames
        .process(VideoShotDetectorFunction()).name("video_shot_detector")
        .set_parallelism(2)
    )
    ds_video_wipe = (
        ds_frames
        .process(VideoWipeDetectorFunction()).name("video_wipe_detector")
        .set_parallelism(2)
    )
    # combine the results for each individual frame
    ds_final = (
        ds_game_time
        .union(ds_paddle_ocr, ds_video_shot, ds_video_wipe)  # combine detector results into one datastream
        .key_by(lambda x: (x["recording_id"], x["frame_index"]))  # key based on 2 field values
        .count_window(4)  # assign a count window that fires once the number of elements equals 4
        .process(DetectorCollectorFunction(), output_type=Types.STRING()).name("detector_collector")
        # .aggregate(DetectorAggregatorFunction(), output_type=Types.STRING()).name("detector_aggregator")
        # .reduce(DetectorReducerFunction()).name("detector_reducer")
        # .map(lambda x: json.dumps(x), output_type=Types.STRING()).name("json_converter")
        .set_parallelism(1)
    )
    # print out the results on the console
    ds_final.print()

    # produce data to kafka sink topic
    # print("Setting up Kafka sink")
    # kafka_producer = KafkaSink.builder() \
    #     .set_bootstrap_servers("broker:29092") \
    #     .set_record_serializer(
    #     KafkaRecordSerializationSchema.builder()
    #     .set_topic("test-sink-topic")
    #     .set_value_serialization_schema(SimpleStringSchema())
    #     .build()
    # ) \
    #     .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    #     .build()

    # ds_final.sink_to(kafka_producer).name("kafka-sink")

    # execute
    print("Executing Environment")
    env.execute("video_processing")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    main()
