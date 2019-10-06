import base64
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Tuple

import boto3
import ffmpeg as ffmpeg_client

logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(level=logging.INFO)

s3 = boto3.client("s3")

input_formats = {"video": ["mp4", "mov", "mkv", "m4a"]}
input_formats_list = list(set(*input_formats.values()))

target_formats = {"video": ["mp3", "mp4", "mov", "m4a"]}
target_formats_list = list(set(*target_formats.values()))


def get_formats(s3_object) -> Tuple[str, str]:
    return (
        s3_object["Metadata"].get("input-format"),
        s3_object["Metadata"].get("target-format"),
    )


def video(record, metadata, input_bucket_name, key_name, target_format):
    file_path = f"/tmp/{key_name}"
    with open(file_path, "wb") as writer:
        s3.download_fileobj(input_bucket_name, key_name, writer)

    output_file_name = f"{uuid.uuid4()}.{target_format}"
    output_file_path = f"/tmp/{output_file_name}"

    ffmpeg_client.input(file_path).output(output_file_path).run(
        cmd="/opt/ffmpeg/ffmpeg"
    )

    with open(output_file_path, "rb") as reader:
        os.remove(output_file_path)
        os.remove(file_path)

        s3.put_object(
            Bucket=os.environ["OUTPUT_BUCKET_NAME"],
            Key=f"{output_file_name}",
            Body=reader.read(),
            Expires=datetime.now() + timedelta(hours=1),
            Metadata=metadata,
            ACL="public-read",
        )


def convert(event, context):
    if not "Records" in event:
        logger.info("Not a S3 invocation")
        return

    for record in event["Records"]:
        if not "s3" in record:
            logger.info("Not a S3 invocation")
            continue

        input_bucket_name = record["s3"]["bucket"]["name"]
        key_name = record["s3"]["object"]["key"]

        s3_object = s3.get_object(Bucket=input_bucket_name, Key=key_name)
        input_format, target_format = get_formats(s3_object)

        if not input_format or not input_format in input_formats_list:
            logger.info("Invalid input-format metadata... Skipping")
            continue

        if not target_format or target_format not in target_formats_list:
            logger.info("Invalid target-format metadata... Skipping")
            continue

        if (
            input_format in input_formats["video"]
            and target_format in target_formats["video"]
        ):
            video(
                record,
                s3_object["Metadata"],
                input_bucket_name,
                key_name,
                target_format,
            )
