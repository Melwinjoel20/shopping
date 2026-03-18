import boto3
import os
import json
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET_NAME = "easycart-proj-nci"

s3 = boto3.client("s3", region_name=REGION)

def create_bucket():
    """Create S3 bucket + attach public-read policy (ACL-free)."""
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": REGION}
            )
        print(f"✔ Bucket created: {BUCKET_NAME}")

    except ClientError as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            print(f"ℹ Bucket already exists: {BUCKET_NAME}")
        else:
            raise e
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicRead",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}/*"
            }
        ]
    }

    try:
        s3.put_bucket_policy(
            Bucket=BUCKET_NAME,
            Policy=json.dumps(policy)
        )
        print("✔ Public-read bucket policy applied")

    except Exception as e:
        print(f"⚠ Could not set bucket policy: {e}")


def logo_exists():
    object_key = "images/EasyCartLogo.png"
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=object_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise e

def upload_logo():
    logo_path = "infra/EasyCartLogo.png"
    object_key = "images/EasyCartLogo.png"

    if logo_exists():
        print(" Logo already exists in S3, skipping upload.")
    else:
        s3.upload_file(
            logo_path,
            BUCKET_NAME,
            object_key,
            ExtraArgs={"ContentType": "image/png"}
        )
        print("✔ Logo uploaded successfully!")

    return f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{object_key}"

def save_config(url):
    config_path = "infra/config.json"
    with open(config_path, "r") as f:
        data = json.load(f)

    data["s3_logo_url"] = url

    with open(config_path, "w") as f:
        json.dump(data, f, indent=4)

    print(" S3 logo URL saved in config.json")

def main():
    print(" Setting up S3 for EasyCart...")
    create_bucket()
    url = upload_logo()
    save_config(url)

if __name__ == "__main__":
    main()
