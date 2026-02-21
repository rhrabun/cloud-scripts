#!/usr/bin/env python3
import argparse

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

    args = p.parse_args()

    return args


def create_aws_session(region, profile_name):
    global aws_lambda

    if profile_name:
        print(f'Using profile {profile_name}\n')
        my_session = boto3.session.Session(
            region_name=region,
            profile_name=profile_name
        )
        aws_lambda = my_session.client('lambda')
    else:
        print('Using default profile')
        aws_lambda = boto3.client('lambda', region_name=region)


def get_lambdas_versions():
    paginator = aws_lambda.get_paginator('list_functions')
    func_list = []
    versions_to_delete = []

    iterator_response = paginator.paginate(
        FunctionVersion='ALL'
    )

    for page in iterator_response:
        func_list.extend(page['Functions'])

    for func in func_list:
        if func['Version'] != '$LATEST':
            versions_to_delete.append(func['FunctionArn'])

    return versions_to_delete


def delete_versions(lambdas_to_delete_list):
    for arn in lambdas_to_delete_list:
        print(f"Deleting function version {arn}")

        aws_lambda.delete_function(
            FunctionName=arn,
        )

        print('done')

    return


def main():
    args = parse_cmd()

    region = args.region
    profile_name = args.profile_name

    create_aws_session(region, profile_name)

    try:
        arns_to_delete = get_lambdas_versions()
        delete_versions(arns_to_delete)
        print('Successfully deleted all lambda versions except for LATEST')
    except Exception as e:
        print(f'Smth went wrong. Error text:\n{e}')


if __name__ == '__main__':
    main()
