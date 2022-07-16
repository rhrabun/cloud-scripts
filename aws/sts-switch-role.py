import sys
import json
import argparse
import subprocess


def parse_cmd():
    p = argparse.ArgumentParser()

    p.add_argument(
        '-s',
        '--source-profile',
        help='The name of the AWS profile which script should use to assume role',
        required=True
    )

    p.add_argument(
        '-r',
        '--role',
        help='The name of the AWS IAM role to be assumed',
        required=True
    )

    p.add_argument(
        '-a',
        '--account-id',
        help='The ID of the AWS IAM account in which role will be assumed',
        required=True
    )

    p.add_argument(
        '-t',
        '--target-profile',
        help='The name of the AWS profile for which script should save temporary credentials',
        required=False,
        default='temporary'
    )

    args = p.parse_args()

    return args


def run_command(command):
    try:
        response = subprocess.run(
            [command], stdout=subprocess.PIPE, shell=True, check=True)


        return response
    except subprocess.CalledProcessError as e:
        print('-'*60)
        print(f'Something went wrong. Subprocess error:\n{e}')
        print('-'*60)

        sys.exit()


def assume_role(account_id, role_name, source_profile, target_profile):
    response = run_command(
        f"aws sts assume-role --role-arn arn:aws:iam::{account_id}:role/{role_name}" +
        f" --role-session-name {target_profile} --external-id {account_id} --duration-seconds 3600 --profile {source_profile}"
    )
    credentials = json.loads(response.stdout.decode('utf-8'))

    return credentials


def set_profile(credentials, target_profile):
    run_command(
        f"aws configure set profile.{target_profile}.aws_access_key_id '{credentials['Credentials']['AccessKeyId']}'")
    run_command(
        f"aws configure set profile.{target_profile}.aws_secret_access_key '{credentials['Credentials']['SecretAccessKey']}'")
    run_command(
        f"aws configure set profile.{target_profile}.aws_session_token '{credentials['Credentials']['SessionToken']}'")

    return


def main():
    args = parse_cmd()
    print(f'Executing script with following parameters:\n{args}\n')

    source_profile = args.source_profile
    target_profile = args.target_profile
    role_name = args.role
    account_id = args.account_id

    credentials = assume_role(account_id, role_name,
                              source_profile, target_profile)
    set_profile(credentials, target_profile)

    return


if __name__ == "__main__":
    main()
