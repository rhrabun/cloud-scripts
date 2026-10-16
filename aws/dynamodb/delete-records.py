import argparse
import logging
from multiprocessing import Pool
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
        help='The name of the DynamoDB table',
        required=True
    )

    p.add_argument(
        '--process-num',
        help='Number of processes that should handle deletion (one process uses up to 200 WCU); default: 1',
        type=int,
        default=1
    )

    args = p.parse_args()

    return args


def create_aws_session(region, profile_name):
    # Returns a session instead of clients, so that both the main process and
    # worker processes can build their own clients. boto3 clients are not
    # pickle-able, and globals set in the parent process are not guaranteed to
    # reach workers, so each worker creates its own clients.
    if profile_name:
        logger.info(f'Using profile {profile_name}')
        return boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )

    logger.info('Using default profile')
    return boto3.session.Session(region_name=region)


def get_key_schema(session, table_name):
    client = session.client('dynamodb')
    response = client.describe_table(TableName=table_name)

    return [key['AttributeName'] for key in response['Table']['KeySchema']]


def get_records_from_dynamodb(session, table_name, key_names):
    logger.info('Getting records from DynamoDB table')

    client = session.client('dynamodb')
    paginator = client.get_paginator('scan')
    response_iterator = paginator.paginate(
        TableName=table_name,
        ProjectionExpression=', '.join(key_names),
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

    return deserialized_response


def delete_records(data, table_name, region, profile_name):
    session = boto3.session.Session(region_name=region, profile_name=profile_name)
    table = session.resource('dynamodb').Table(table_name)

    with table.batch_writer() as writer:
        for index, item in enumerate(data):
            writer.delete_item(item)
            print(f"Finished processing item number {index}", end='\r', flush=True)

    print()

    return len(data)


def split_list(lst, n):
    return [lst[i::n] for i in range(n)]


def main():
    args = parse_cmd()

    region = args.region
    dynamodb_name = args.dynamodb
    profile_name = args.profile_name
    process_num = args.process_num

    session = create_aws_session(region, profile_name)

    # Resolve the full key schema, since the table may have a sort key in
    # addition to the partition key
    key_names = get_key_schema(session, dynamodb_name)

    data = get_records_from_dynamodb(session, dynamodb_name, key_names)

    logger.info(f'Got {len(data)} records from the DynamoDB table')

    start = time()

    if process_num > 1:
        with Pool(process_num) as p:
            data_chunks = split_list(data, process_num)

            results = p.starmap(delete_records, [(part, dynamodb_name, region, profile_name) for part in data_chunks])

        deleted = sum(results)
    else:
        deleted = delete_records(data, dynamodb_name, region, profile_name)

    logger.info(
        f'[delete-records] table={dynamodb_name} region={region} '
        f'deleted={deleted} processes={process_num} duration={time() - start:.1f}s'
    )


if __name__ == "__main__":
    main()