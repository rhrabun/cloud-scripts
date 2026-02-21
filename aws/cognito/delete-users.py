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
    # Creates global cognito session, as multiprocessing doesnt support taking boto3.client
    # object as parameter.
    # Objects passed to mp.starmap() must be pickle-able, and AWS clients are not pickle-able
    global cognito

    if profile_name:
        print(f'Using profile {profile_name}\n')
        my_session = boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )
        cognito = my_session.client('cognito-idp')
    else:
        print('Using default profile')
        cognito = boto3.client('cognito-idp', region_name = region)


def get_users(user_pool_id):
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


def delete_worker(user_pool_id, users_list):
    p = current_process()
    for user in users_list:
        print(
            f"Deteling user - {user['Username']} - in process: {p.name} - pid: {p.pid}")
        try:
            cognito.admin_delete_user(
                UserPoolId=user_pool_id,
                Username=user['Username']
            )
        except Exception as e:
            print(f"User {user['Username']} is not found")


def split_list(lst, n):
    return [lst[i::n] for i in range(n)]


def delete_users(user_pool_id, users, process_num):
    users_list_divided = split_list(users, process_num)

    with Pool(process_num) as p:
        # Assign each part of users_list_divided [[part 1], [part 2], ...] to different process
        p.starmap(delete_worker, [(user_pool_id, part) for part in users_list_divided])


def main():
    args = parse_cmd()

    region = args.region
    user_pool_id = args.userpool
    profile_name = args.profile_name
    process_num = int(args.processes)

    create_aws_session(region, profile_name)

    try:
        users = get_users(user_pool_id)
        delete_users(user_pool_id, users, process_num)
        print("Successfully deleted all users!")
    except Exception as e:
        print(f'Smth went wrong. Error: \n{e}')


if __name__ == '__main__':
    main()
