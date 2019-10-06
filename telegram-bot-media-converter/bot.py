import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta

import boto3
import telegram

logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(level=logging.INFO)

s3 = boto3.client("s3")

OK_RESPONSE = {
    "statusCode": 200,
    "headers": {"Content-Type": "application/json"},
    "body": json.dumps("ok"),
}
ERROR_RESPONSE = {"statusCode": 400, "body": json.dumps("Oops, something went wrong!")}

BOT_USERMAME = os.environ.get("BOT_USERMAME")


def configure_telegram():
    """
    Configures the bot with a Telegram Token.
    Returns a bot instance.
    """

    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
    if not TELEGRAM_TOKEN:
        logger.error("The TELEGRAM_TOKEN must be set")
        raise NotImplementedError

    return telegram.Bot(TELEGRAM_TOKEN)


def bot(event, context):
    bot = configure_telegram()
    logger.info(f"Event: {event}")

    if event.get("httpMethod") == "POST" and event.get("body"):
        update = telegram.Update.de_json(json.loads(event.get("body")), bot)
        chat_id = update.effective_message.chat.id if update.effective_message else None

        text = update.effective_message.text
        attachment = update.effective_message.effective_attachment

        if text in ["/start", f"/start@{BOT_USERMAME}"]:
            bot.send_message(chat_id=chat_id, text="Beep boop")
        elif attachment:
            bot.send_message(chat_id=chat_id, text="Processing...")

            file_name = uuid.uuid4()
            file_path = f"/tmp/{file_name}.mov"

            attachment_file = bot.get_file(attachment.file_id)
            attachment_file.download(file_path)

            with open(file_path, "rb") as reader:
                os.remove(file_path)

                s3.put_object(
                    Bucket=os.environ["INPUT_BUCKET_NAME"],
                    Key=f"{file_name}.mov",
                    Body=reader.read(),
                    Expires=datetime.now() + timedelta(hours=1),
                    Metadata={
                        "chat-id": str(chat_id),
                        "input-format": "mov",
                        "target-format": "mp4",
                    },
                )

        return OK_RESPONSE

    return ERROR_RESPONSE


def on_convert(event, context):
    logger.info(f"Event: {event}")

    if not "Records" in event:
        logger.info("Not a S3 invocation")
        return

    for record in event["Records"]:
        if not "s3" in record:
            logger.info("Not a S3 invocation")
            continue

        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        if bucket != os.environ["OUTPUT_BUCKET_NAME"]:
            logger.info("Not an output bucket invocation")
            continue

        s3_object = s3.get_object(Bucket=bucket, Key=key)

        chat_id = s3_object["Metadata"].get("chat-id")

        bot = configure_telegram()
        bot.send_message(
            chat_id=chat_id, text=f"https://{bucket}.s3.amazonaws.com/{key}"
        )


def set_webhook(event, context):
    """
    Sets the Telegram bot webhook.
    """

    bot = configure_telegram()
    host = event.get("headers").get("Host")
    stage = event.get("requestContext").get("stage")
    url = f"https://{host}/{stage}/"
    webhook = bot.set_webhook(url)

    if webhook:
        return OK_RESPONSE

    return ERROR_RESPONSE
