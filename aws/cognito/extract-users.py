import argparse
import json
import logging
import sys
from os.path import isfile
from time import time

import boto3
from boto3.dynamodb.types import TypeDeserializer

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

    p.add_argument(
        '-d',
        '--dynamodb',
        help='The name of the DynamoDB table, where users are stored'
    )

    p.add_argument(
        '-u',
        '--userpool',
        help='The ID of the User Pool, where users are stored'
    )

    p.add_argument(
        '-o',
        '--output',
        help='The name of the result JSON file (without file extension)',
        default='users_data'
    )

    args = p.parse_args()

    return args


def create_aws_session(region, profile_name):
    global dynamodb, cognito

    if profile_name:
        logger.info(f'Using profile {profile_name}')
        my_session = boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )

        dynamodb = my_session.client('dynamodb')
        cognito = my_session.client('cognito-idp')
    else:
        logger.info('Using default profile')

        dynamodb = boto3.client('dynamodb', region_name=region)
        cognito = boto3.client('cognito-idp', region_name=region)


def save_data_to_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, sort_keys=True, default=str)


def get_users_from_cognito(user_pool_id):
    paginator = cognito.get_paginator('list_users')

    users_list = []

    logger.info('Getting a list of users from Cognito')
    response_iterator = paginator.paginate(
        UserPoolId=user_pool_id
    )

    for page in response_iterator:
        users_list.extend(page['Users'])

    logger.info(f'Number of users in userpool "{user_pool_id}" - {len(users_list)}')

    # Transform Cognito nested dicts structure to flat dict
    for user in users_list:
        for item in user['Attributes']:
            user[item['Name'].replace('custom:','')] = item['Value']

        del user['Attributes']

    return users_list


def get_users_from_dynamodb(table_name):
    logger.info('Getting a list of users from DynamoDB table')

    paginator = dynamodb.get_paginator('scan')
    response_iterator = paginator.paginate(
        TableName=table_name
    )

    response = []
    for page in response_iterator:
        response.extend(page['Items'])

    # Deserialize the DynamoDB response to get rid of variable type declarations
    deserializer = TypeDeserializer()
    deserialized_response = []

    for record in response:
        flat_record = {k: deserializer.deserialize(v) for k, v in record.items()}
        deserialized_response.append(flat_record)

    logger.info(f'Number of DynamoDB items - {len(deserialized_response)}')

    return deserialized_response


def main():
    args = parse_cmd()

    region = args.region
    profile_name = args.profile_name
    dynamodb_name = args.dynamodb
    userpool_id = args.userpool
    result_file = args.output + '.json'

    # Check if file with raw users data exists
    if isfile(result_file):
        logger.error('File with users data already exists in target directory, exiting')

        sys.exit(1)

    if not dynamodb_name and not userpool_id:
        logger.error('Either specify Cognito userpool ID or DynamoDB Table name to extract users from')

        sys.exit(1)
    elif dynamodb_name and userpool_id:
        logger.error('Either specify Cognito userpool ID or DynamoDB Table name to extract users from; not both')

        sys.exit(1)

    create_aws_session(region, profile_name)

    start = time()

    if dynamodb_name:
        users_list = get_users_from_dynamodb(dynamodb_name)
        source = f'dynamodb={dynamodb_name}'
    else:
        users_list = get_users_from_cognito(userpool_id)
        source = f'userpool={userpool_id}'

    save_data_to_json(users_list, f'./{result_file}')

    logger.info(
        f'[extract-users] {source} users={len(users_list)} '
        f'duration={time() - start:.1f}s file={result_file}'
    )


if __name__ == "__main__":
    main()
