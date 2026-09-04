#!/usr/bin/env python3
import argparse
from multiprocessing import Pool, current_process

import boto3


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
        print(f'Using profile {profile_name}\n')
        return boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )

    print('Using default profile')
    return boto3.session.Session(region_name=region)


def get_users(session, user_pool_id):
    cognito = session.client('cognito-idp')
    paginator = cognito.get_paginator('list_users')
    users_list = []

    print('Getting a list of users from Cognito')

    response_iterator = paginator.paginate(
        UserPoolId=user_pool_id,
    )

    for page in response_iterator:
        users_list.extend(page['Users'])

    print(f'Number of users in userpool "{user_pool_id}" - {len(users_list)}\n')

    return users_list


def delete_worker(user_pool_id, region, profile_name, users_list):
    session = boto3.session.Session(region_name=region, profile_name=profile_name)
    cognito = session.client('cognito-idp')
    p = current_process()
    for user in users_list:
        print(
            f"Deleting user - {user['Username']} - in process: {p.name} - pid: {p.pid}")
        try:
            cognito.admin_delete_user(
                UserPoolId=user_pool_id,
                Username=user['Username']
            )
        except cognito.exceptions.UserNotFoundException:
            print(f"User {user['Username']} is not found")


def split_list(lst, n):
    return [lst[i::n] for i in range(n)]


def delete_users(user_pool_id, users, process_num, region, profile_name):
    users_list_divided = split_list(users, process_num)

    with Pool(process_num) as p:
        # Assign each part of users_list_divided [[part 1], [part 2], ...] to different process
        p.starmap(delete_worker, [(user_pool_id, region, profile_name, part) for part in users_list_divided])


def main():
    args = parse_cmd()

    region = args.region
    user_pool_id = args.userpool
    profile_name = args.profile_name
    process_num = int(args.processes)

    session = create_aws_session(region, profile_name)
    users = get_users(session, user_pool_id)
    delete_users(user_pool_id, users, process_num, region, profile_name)
    print("Successfully deleted all users!")


if __name__ == '__main__':
    main()