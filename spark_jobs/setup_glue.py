"""
setup_glue.py
=============
Run this ONCE to set everything up in AWS.
It will:
  1. Ensure your S3 bucket exists
  2. Upload the Glue script to S3
  3. Fetch the pre-made LabRole (Learner Lab doesn't allow IAM creation)
  4. Create the Glue job

Usage:
    python setup_glue.py

Make sure your AWS credentials are configured (aws configure)
"""

import boto3
import json
import time
import os

# =========================
# CONFIG
# =========================
REGION     = "us-east-1"
BUCKET     = "easycart1-proj-nci"
GLUE_JOB   = "weekly-sales-analytics"
SCRIPT_KEY = "scripts/weekly_sales_glue.py"

# Path to the glue script — must be in the same folder as this file
SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weekly_sales_glue.py")


def step(msg):
    print(f"\n{'='*55}")
    print(f"  {msg}")
    print('='*55)


# =========================
# STEP 1 — S3 bucket
# =========================
def ensure_bucket(s3):
    step("Step 1 — Checking S3 bucket")
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"✅ Bucket '{BUCKET}' already exists.")
    except Exception:
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        print(f"✅ Bucket '{BUCKET}' created.")


# =========================
# STEP 2 — Upload Glue script
# =========================
def upload_script(s3):
    step("Step 2 — Uploading Glue script to S3")

    if not os.path.exists(SCRIPT_PATH):
        raise FileNotFoundError(
            f"Could not find {SCRIPT_PATH}\n"
            "Make sure weekly_sales_glue.py is in the same folder as setup_glue.py"
        )

    with open(SCRIPT_PATH, "rb") as f:
        s3.put_object(
            Bucket=BUCKET,
            Key=SCRIPT_KEY,
            Body=f.read(),
            ContentType="text/x-python"
        )

    script_s3_path = f"s3://{BUCKET}/{SCRIPT_KEY}"
    print(f"✅ Script uploaded → {script_s3_path}")
    return script_s3_path


# =========================
# STEP 3 — Get LabRole
# (Learner Lab blocks IAM creation so we use the pre-made LabRole)
# =========================
def get_lab_role(iam):
    step("Step 3 — Fetching Learner Lab pre-made LabRole")
    try:
        role_arn = iam.get_role(RoleName="LabRole")["Role"]["Arn"]
        print(f"✅ Using LabRole: {role_arn}")
        return role_arn
    except Exception as e:
        raise RuntimeError(f"❌ Could not fetch LabRole: {e}")


# =========================
# STEP 4 — Create Glue Job
# =========================
def create_glue_job(glue, role_arn, script_s3_path):
    step("Step 4 — Creating Glue Job")

    job_config = {
        "Name": GLUE_JOB,
        "Role": role_arn,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_s3_path,
            "PythonVersion": "3"
        },
        "DefaultArguments": {
            "--TempDir":                          f"s3://{BUCKET}/tmp/",
            "--job-language":                     "python",
            "--enable-metrics":                   "",
            "--enable-continuous-cloudwatch-log": "true",
        },
        "GlueVersion":     "4.0",
        "WorkerType":      "G.1X",
        "NumberOfWorkers":  2,
        "Timeout":          10,
        "MaxRetries":       0,
        "Description":     "Weekly sales analytics — DynamoDB → Spark → S3"
    }

    try:
        glue.get_job(JobName=GLUE_JOB)
        update = {k: v for k, v in job_config.items() if k != "Name"}
        glue.update_job(JobName=GLUE_JOB, JobUpdate=update)
        print(f"✅ Glue job '{GLUE_JOB}' updated.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(**job_config)
        print(f"✅ Glue job '{GLUE_JOB}' created.")


# =========================
# STEP 5 — Test run
# =========================
def test_run(glue):
    step("Step 5 — Running a test job to verify everything works")
    print("⏳ This takes ~3-5 minutes on Learner Lab...")

    response = glue.start_job_run(JobName=GLUE_JOB)
    run_id   = response["JobRunId"]
    print(f"   RunId: {run_id}")

    max_wait   = 600
    poll_every = 15
    elapsed    = 0

    while elapsed < max_wait:
        time.sleep(poll_every)
        elapsed    += poll_every
        status      = glue.get_job_run(JobName=GLUE_JOB, RunId=run_id)
        state       = status["JobRun"]["JobRunState"]
        print(f"  ⏳ {state} ({elapsed}s)")

        if state == "SUCCEEDED":
            print("✅ Test run succeeded!")
            return True

        if state in ("FAILED", "ERROR", "TIMEOUT", "STOPPED"):
            err = status["JobRun"].get("ErrorMessage", "No details")
            print(f"❌ Test run failed — {state}: {err}")
            return False

    print("❌ Timed out waiting for test run.")
    return False


# =========================
# MAIN
# =========================
def main():
    print("\n🚀 EasyCart — Glue Analytics Setup")
    print(f"   Region  : {REGION}")
    print(f"   Bucket  : {BUCKET}")
    print(f"   Glue Job: {GLUE_JOB}")

    s3   = boto3.client("s3",   region_name=REGION)
    iam  = boto3.client("iam",  region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)

    ensure_bucket(s3)
    script_s3_path = upload_script(s3)
    role_arn       = get_lab_role(iam)
    create_glue_job(glue, role_arn, script_s3_path)

    print("\n✅ Setup complete!")
    run_test = input("\nRun a test job now? (y/n): ").strip().lower()

    if run_test == "y":
        ok = test_run(glue)
        if ok:
            print("\n🎉 Everything is working! Your Django dashboard is ready.")
        else:
            print("\n⚠️  Setup done but test run failed.")
            print("   Check AWS Glue console → Jobs → your job → Run details for the error.")
    else:
        print("\n   Skipped. You can trigger it from your Django dashboard.")


if __name__ == "__main__":
    main()