#!/usr/bin/env python3
import boto3
import argparse


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
        print(f'Using profile {profile_name}\n')
        my_session = boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )
        kms = my_session.client('kms')
    else:
        print('Using default profile')
        kms = boto3.client('kms', region_name=region)


def get_keys():
    paginator = kms.get_paginator('list_keys')
    keys = []

    iterator_response = paginator.paginate()

    for page in iterator_response:
        keys.extend(page['Keys'])

    return keys


def delete_keys(keys):
    for key in keys:
        key_desc = kms.describe_key(
            KeyId=key['KeyId']
        )

        if key_desc['KeyMetadata']['KeyManager'] == 'CUSTOMER' and key_desc['KeyMetadata']['KeyState'] == 'Enabled':
            print(f"Scheduling deletion for key {key_desc['Arn']}")

            kms.schedule_key_deletion(
                KeyId=key_desc['KeyMetadata']['KeyId'],
                PendingWindowInDays=7
            )

    return


def main():
    args = parse_cmd()

    region = args.region
    profile_name = args.profile_name

    create_aws_session(region, profile_name)

    try:
        keys = get_keys()
        delete_keys(keys)
        print('Succesfully deleted all keys')
    except Exception as e:
        print(f'Smth went wrong. Error: \n{e}')


if __name__ == '__main__':
    main()
