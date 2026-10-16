import argparse
import json
import logging
import subprocess
import sys

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    force=True,
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_cmd():
    p = argparse.ArgumentParser()

    p.add_argument(
        "-s",
        "--source-profile",
        help="The name of the AWS profile which script should use to assume role",
        required=True,
    )

    p.add_argument(
        "-r", "--role", help="The name of the AWS IAM role to be assumed", required=True
    )

    p.add_argument(
        "-a",
        "--account-id",
        help="The ID of the AWS IAM account in which role will be assumed",
        required=True,
    )

    p.add_argument(
        "-t",
        "--target-profile",
        help="The name of the AWS profile for which script should save temporary credentials",
        required=False,
        default="temporary",
    )

    args = p.parse_args()

    return args


def run_command(command):
    try:
        return subprocess.run(command, stdout=subprocess.PIPE, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Something went wrong. Subprocess error:\n{e}")

        sys.exit(1)


def assume_role(account_id, role_name, source_profile, target_profile):
    response = run_command(
        [
            "aws",
            "sts",
            "assume-role",
            "--role-arn",
            f"arn:aws:iam::{account_id}:role/{role_name}",
            "--role-session-name",
            target_profile,
            "--external-id",
            account_id,
            "--duration-seconds",
            "3600",
            "--profile",
            source_profile,
        ]
    )
    credentials = json.loads(response.stdout.decode("utf-8"))

    return credentials


def set_profile(credentials, target_profile):
    run_command(
        [
            "aws",
            "configure",
            "set",
            f"profile.{target_profile}.aws_access_key_id",
            credentials["Credentials"]["AccessKeyId"],
        ]
    )
    run_command(
        [
            "aws",
            "configure",
            "set",
            f"profile.{target_profile}.aws_secret_access_key",
            credentials["Credentials"]["SecretAccessKey"],
        ]
    )
    run_command(
        [
            "aws",
            "configure",
            "set",
            f"profile.{target_profile}.aws_session_token",
            credentials["Credentials"]["SessionToken"],
        ]
    )


def main():
    args = parse_cmd()
    logger.info(f"Executing script with following parameters: {args}")

    source_profile = args.source_profile
    target_profile = args.target_profile
    role_name = args.role
    account_id = args.account_id

    credentials = assume_role(account_id, role_name, source_profile, target_profile)
    set_profile(credentials, target_profile)


if __name__ == "__main__":
    main()
