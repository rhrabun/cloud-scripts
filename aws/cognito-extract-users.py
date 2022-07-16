import json
import argparse
from time import time
from os.path import isfile
from functools import wraps

import boto3
from boto3.dynamodb.types import TypeDeserializer


def timer(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        ts = time()
        result = f(*args, **kwargs)
        te = time()
        print(f'Function "{f.__name__}" took {te-ts} seconds to run\n')
        return result
    return wrapper


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
        print(f'Using profile {profile_name}\n')
        my_session = boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )

        dynamodb = my_session.client('dynamodb')
        cognito = my_session.client('cognito-idp')
    else:
        print('Using default profile')

        dynamodb = boto3.client('dynamodb', region_name=region)
        cognito = boto3.client('cognito-idp', region_name=region)


def save_data_to_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, sort_keys=True, default=str)


@timer
def get_users_from_cognito(user_pool_id):
    paginator = cognito.get_paginator('list_users')

    users_list = []

    print('Getting a list of users from Cognito\n')
    response_iterator = paginator.paginate(
        UserPoolId=user_pool_id
    )

    for page in response_iterator:
        users_list.extend(page['Users'])

    print(f'Number of users in userpool "{user_pool_id}" - {len(users_list)}')

    # Transform Cognito nested dicts structure to flat dict
    for user in users_list:
        for item in user['Attributes']:
            user[item['Name'].replace('custom:','')] = item['Value']

        del user['Attributes']

    return users_list


@timer
def get_users_from_dynamodb(table_name):
    print('Gettings a list of users from DynamoDB table\n')

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

    del response
    
    print(f'Number of DynamoDB items - {len(deserialized_response)}')

    return deserialized_response


def main():
    args = parse_cmd()

    region = args.region
    profile_name = args.profile_name
    dynamodb_name = args.dynamodb
    userpool_id = args.userpool
    result_file = args.output + '.json'

    create_aws_session(region, profile_name)

    users_list = []
    
    # Check if file with raw users data exists
    if not isfile(result_file):
        if not dynamodb_name and not userpool_id:
            print('Either specify Cognito userpool ID or DynamoDB Table name to extract users from')

            return
        elif dynamodb_name and userpool_id:
            print('Either specify Cognito userpool ID or DynamoDB Table name to extract users from; not both')

            return
        elif dynamodb_name:
            users_list = get_users_from_dynamodb(dynamodb_name)

        elif userpool_id:
            users_list = get_users_from_cognito(userpool_id)

        save_data_to_json(users_list, f'./{result_file}')
    else:
        print('File with users data already exists in target directory, exiting')
        
        return

    print('Finished')


if __name__ == "__main__":
    main()
