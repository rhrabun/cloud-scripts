#!/usr/bin/env python3
import argparse
import logging
from multiprocessing import Pool, current_process
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

    p.add_argument(
        '-u',
        '--userpool',
        help='The ID of the User Pool, where users must be deleted',
        required=True
    )

    p.add_argument(
        '--processes',
        help='Number of processes which should handle the deletion (default - 1)',
        type=int,
        default=1
    )

    args = p.parse_args()

    return args


def create_aws_session(region, profile_name):
    # Returns a session instead of a client, so that both the main process and
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


def get_users(session, user_pool_id):
    cognito = session.client('cognito-idp')
    paginator = cognito.get_paginator('list_users')
    users_list = []

    logger.info('Getting a list of users from Cognito')

    response_iterator = paginator.paginate(
        UserPoolId=user_pool_id,
    )

    for page in response_iterator:
        users_list.extend(page['Users'])

    logger.info(f'Number of users in userpool "{user_pool_id}" - {len(users_list)}')

    return users_list


def delete_worker(user_pool_id, region, profile_name, users_list):
    session = boto3.session.Session(region_name=region, profile_name=profile_name)
    cognito = session.client('cognito-idp')
    p = current_process()
    deleted = 0
    not_found = 0
    for user in users_list:
        logger.info(
            f"Deleting user - {user['Username']} - in process: {p.name} - pid: {p.pid}")
        try:
            cognito.admin_delete_user(
                UserPoolId=user_pool_id,
                Username=user['Username']
            )
            deleted += 1
        except cognito.exceptions.UserNotFoundException:
            logger.warning(f"User {user['Username']} is not found")
            not_found += 1

    return deleted, not_found


def split_list(lst, n):
    return [lst[i::n] for i in range(n)]


def delete_users(user_pool_id, users, process_num, region, profile_name):
    if process_num <= 1:
        return delete_worker(user_pool_id, region, profile_name, users)

    users_list_divided = split_list(users, process_num)

    with Pool(process_num) as p:
        # Assign each part of users_list_divided [[part 1], [part 2], ...] to different process
        results = p.starmap(delete_worker, [(user_pool_id, region, profile_name, part) for part in users_list_divided])

    return sum(r[0] for r in results), sum(r[1] for r in results)


def main():
    args = parse_cmd()

    region = args.region
    user_pool_id = args.userpool
    profile_name = args.profile_name
    process_num = args.processes

    start = time()

    session = create_aws_session(region, profile_name)
    users = get_users(session, user_pool_id)
    deleted, not_found = delete_users(user_pool_id, users, process_num, region, profile_name)

    logger.info(
        f'[delete-users] userpool={user_pool_id} region={region} found={len(users)} '
        f'deleted={deleted} not_found={not_found} processes={process_num} '
        f'duration={time() - start:.1f}s'
    )


if __name__ == '__main__':
    main()
