from json import dump
from kafka_components.consumer import all_events
from core.config import TOPICS

import boto3
from botocore.exceptions import NoCredentialsError


def upload_result(filename, result):
    with open(filename, "w") as write_file:
        dump(result, write_file)

    # specify credentials
    s3 = boto3.client('s3', aws_access_key_id="",
                      aws_secret_access_key="",
                      aws_session_token="")
    try:
        # specify bucket name
        s3.upload_file(filename, "", filename)
        print("Uploaded!")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")


def create_report():
    for i in TOPICS:
        upload_result(f"report-{i}.json", all_events(i))
