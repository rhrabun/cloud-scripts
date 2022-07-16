import os
import json
import os.path
import argparse

import boto3


# TODO:
# * Add option to delete retrieved messages from the queue

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

    args = p.parse_args()

    return args


def create_aws_session(region, profile_name):
    global sqs

    if profile_name:
        print(f'Using profile {profile_name}\n')
        my_session = boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )

        sqs = my_session.client('sqs')
    else:
        print('Using default profile')

        sqs = boto3.client('sqs', region_name=region)


def save_data_to_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, sort_keys=True, default=str)


def extract_messages(sqs_url, filename):
    messages = []

    while True:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10
        )
        try:
            messages.extend(response['Messages'])
        except KeyError:
            break
    
    save_data_to_json(messages, filename)


def main():
    args = parse_cmd()

    region = args.region
    sqs_url = args.url
    filename = curr_dir + '/' + args.file
    profile_name = args.profile_name

    create_aws_session(region, profile_name)
    
    extract_messages(sqs_url, filename)


if __name__ == "__main__":
    main()
