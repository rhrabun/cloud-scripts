import argparse
from multiprocessing import Pool

import boto3
from boto3.dynamodb.types import TypeDeserializer


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
        help='The name of the DynamoDB table',
        required=True
    )

    p.add_argument(
        '-k',
        '--primary-key',
        help='The name of the primary key in the DynamoDB table',
        required=True
    )

    p.add_argument(
        '--process-num',
        help='Number of processes that should handle deletion (one process uses up to 200 WCU); default: 1',
        default=1
    )

    args = p.parse_args()

    return args


def create_aws_session(region, profile_name):
    global dynamodb_client, dynamodb_resource

    if profile_name:
        print(f'Using profile {profile_name}\n')
        my_session = boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )

        dynamodb_client = my_session.client('dynamodb')
        dynamodb_resource = my_session.resource('dynamodb')
    else:
        print('Using default profile')

        dynamodb_client = boto3.client('dynamodb', region_name=region)
        dynamodb_resource = boto3.resource('dynamodb', region_name=region)


def get_records_from_dynamodb(table_name, primary_key):
    print('Gettings records from DynamoDB table\n')

    paginator = dynamodb_client.get_paginator('scan')
    response_iterator = paginator.paginate(
        TableName=table_name,
        AttributesToGet=[
            primary_key,
        ],
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

    return deserialized_response


def delete_records(data, table_name):
    table = dynamodb_resource.Table(table_name)

    with table.batch_writer() as writer:
        for index, item in enumerate(data):
            writer.delete_item(item)
            print(f"Finished processing item number {index}", end='\r', flush=True)


def split_list(lst, n):
    return [lst[i::n] for i in range(n)]


def main():
    args = parse_cmd()

    region = args.region
    dynamodb_name = args.dynamodb
    primary_key = args.primary_key
    profile_name = args.profile_name
    process_num = int(args.process_num)

    create_aws_session(region, profile_name)

    data = get_records_from_dynamodb(dynamodb_name, primary_key)

    print(f'Got {len(data)} records from the DynamoDB table\n')

    if process_num > 1:
        with Pool(process_num) as p:
            data_chunks = split_list(data, process_num)

            p.starmap(delete_records, [(part, dynamodb_name) for part in data_chunks])
    else:
        delete_records(data, dynamodb_name)

    print('\nFinished')


if __name__ == "__main__":
    main()
