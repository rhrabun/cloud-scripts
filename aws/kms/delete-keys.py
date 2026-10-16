#!/usr/bin/env python3
import argparse
import logging
from time import time

import boto3

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    force=True,
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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

    args = p.parse_args()

    return args


def create_aws_session(region, profile_name):
    global kms

    if profile_name:
        logger.info(f'Using profile {profile_name}')
        my_session = boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )
        kms = my_session.client('kms')
    else:
        logger.info('Using default profile')
        kms = boto3.client('kms', region_name=region)


def get_keys():
    paginator = kms.get_paginator('list_keys')
    keys = []

    iterator_response = paginator.paginate()

    for page in iterator_response:
        keys.extend(page['Keys'])

    return keys


def delete_keys(keys):
    scheduled = 0
    for key in keys:
        key_desc = kms.describe_key(
            KeyId=key['KeyId']
        )

        if key_desc['KeyMetadata']['KeyManager'] == 'CUSTOMER' and key_desc['KeyMetadata']['KeyState'] == 'Enabled':
            logger.info(f"Scheduling deletion for key {key_desc['Arn']}")

            kms.schedule_key_deletion(
                KeyId=key_desc['KeyMetadata']['KeyId'],
                PendingWindowInDays=7
            )
            scheduled += 1

    return scheduled


def main():
    args = parse_cmd()

    region = args.region
    profile_name = args.profile_name

    start = time()

    create_aws_session(region, profile_name)

    keys = get_keys()
    scheduled = delete_keys(keys)

    logger.info(
        f'[delete-keys] region={region} keys_total={len(keys)} '
        f'scheduled_for_deletion={scheduled} duration={time() - start:.1f}s'
    )


if __name__ == '__main__':
    main()
