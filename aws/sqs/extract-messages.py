import argparse
import json
import logging
import os
from time import time

import boto3

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    force=True,
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

curr_dir = os.path.dirname(os.path.abspath(__file__))


def parse_cmd():
    p = argparse.ArgumentParser()

    p.add_argument(
        '-r',
        '--region',
        help='The name of AWS region where the script should be executed, f.e. us-east-1',
        required=True
    )
    p.add_argument(
        '-p',
        '--profile-name',
        help='The name of the AWS profile which script should use'
    )

    p.add_argument(
        '-u',
        '--url',
        help='URL of the SQS queue, messages from which must be extracted',
        required=True
    )

    p.add_argument(
        '-f',
        '--file',
        help='The name of the file where to save extracted messages',
        required=True
    )

    p.add_argument(
        '--delete',
        help='Delete retrieved messages from the queue after extraction',
        action='store_true'
    )

    args = p.parse_args()

    return args


def create_aws_session(region, profile_name):
    global sqs

    if profile_name:
        logger.info(f'Using profile {profile_name}')
        my_session = boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )

        sqs = my_session.client('sqs')
    else:
        logger.info('Using default profile')

        sqs = boto3.client('sqs', region_name=region)


def save_data_to_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, sort_keys=True, default=str)


def extract_messages(sqs_url, filename, delete=False):
    messages = []

    while True:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
        batch = response.get('Messages', [])
        if not batch:
            break

        messages.extend(batch)

        if delete:
            delete_response = sqs.delete_message_batch(
                QueueUrl=sqs_url,
                Entries=[
                    {'Id': m['MessageId'], 'ReceiptHandle': m['ReceiptHandle']}
                    for m in batch
                ]
            )
            failed = delete_response.get('Failed', [])
            if failed:
                logger.warning(f'Failed to delete {len(failed)} messages: {[f["Id"] for f in failed]}')

    save_data_to_json(messages, filename)

    return len(messages)


def main():
    args = parse_cmd()

    region = args.region
    sqs_url = args.url
    filename = curr_dir + '/' + args.file
    profile_name = args.profile_name
    delete = args.delete

    create_aws_session(region, profile_name)

    start = time()
    extracted = extract_messages(sqs_url, filename, delete)

    logger.info(
        f'[extract-messages] url={sqs_url} extracted={extracted} '
        f'delete={delete} duration={time() - start:.1f}s file={args.file}'
    )


if __name__ == "__main__":
    main()