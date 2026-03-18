import json
import time
import boto3
import os
from pathlib import Path

CONFIG_PATH = "infra/config.json"


# Load / Save config.json
def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


def save_config(data):
    with open(CONFIG_PATH, "w") as f:
        json.dump(data, f, indent=4)
    print("Updated config.json")


# Get Elastic Beanstalk client
def get_eb(region):
    return boto3.client("elasticbeanstalk", region_name=region)


# Ensure application exists
def ensure_application(eb, app_name):
    print(f"Checking if application '{app_name}' exists...")
    resp = eb.describe_applications(ApplicationNames=[app_name])

    if resp.get("Applications"):
        print(f"Application exists: {app_name}")
        return

    print(f"Creating application '{app_name}'...")
    eb.create_application(
        ApplicationName=app_name,
        Description="EasyCart Elastic Beanstalk Application"
    )
    print("Application created")


# Get latest Python platform
def get_latest_platform(eb):
    print("Finding latest Python platform...")

    resp = eb.list_platform_versions(
        Filters=[
            {
                "Type": "PlatformName",
                "Operator": "contains",
                "Values": ["Python"]
            }
        ]
    )

    platforms = resp.get("PlatformSummaryList", [])
    if not platforms:
        raise RuntimeError("No Python EB platforms found!")

    ready = [p for p in platforms if p.get("PlatformStatus") in (None, "Ready")]
    ready.sort(key=lambda p: p["PlatformArn"], reverse=True)

    latest = ready[0]
    print(f"✔ Using platform: {latest['PlatformArn']}")
    return latest["PlatformArn"]


# Ensure EB environment exists
def ensure_environment(eb, config):
    app_name = config["eb_application_name"]
    env_name = config["eb_environment_name"]
    cname_prefix = config["eb_cname_prefix"]
    service_role = config["eb_service_role"]
    instance_profile = config["eb_instance_profile"]

    print(f"Checking if environment '{env_name}' exists...")
    resp = eb.describe_environments(
        ApplicationName=app_name,
        EnvironmentNames=[env_name],
        IncludeDeleted=False
    )

    envs = [e for e in resp.get("Environments", []) if e["Status"] != "Terminated"]

    # If environment exists
    if envs:
        env = envs[0]
        print(f"✔ Environment exists: {env_name}")
        print(f"   URL: http://{env.get('CNAME')}")
        config["eb_environment_url"] = f"http://{env.get('CNAME')}"
        save_config(config)
        return

    # Create environment
    platform_arn = get_latest_platform(eb)

    print(f"Creating environment '{env_name}'...")

    resp = eb.create_environment(
        ApplicationName=app_name,
        EnvironmentName=env_name,
        CNAMEPrefix=cname_prefix,
        PlatformArn=platform_arn,
        Tier={"Name": "WebServer", "Type": "Standard"},
        OptionSettings=[
            {
                "Namespace": "aws:elasticbeanstalk:environment",
                "OptionName": "EnvironmentType",
                "Value": "SingleInstance"
            },
            {
                "Namespace": "aws:elasticbeanstalk:environment",
                "OptionName": "ServiceRole",
                "Value": service_role
            },
            {
                "Namespace": "aws:autoscaling:launchconfiguration",
                "OptionName": "IamInstanceProfile",
                "Value": instance_profile
            }
        ]
    )

    print(f"Environment creation started: {resp['EnvironmentId']}")


# MAIN
def main():
    print(" Running Elastic Beanstalk deployment")

    config = load_config()

    required = [
        "region",
        "eb_application_name",
        "eb_environment_name",
        "eb_cname_prefix",
        "eb_service_role",
        "eb_instance_profile"
    ]

    for r in required:
        if r not in config:
            raise RuntimeError(f"Missing '{r}' in config.json")

    eb = get_eb(config["region"])

    ensure_application(eb, config["eb_application_name"])
    ensure_environment(eb, config)

    print("EB deployment script finished.")


if __name__ == "__main__":
    main()
