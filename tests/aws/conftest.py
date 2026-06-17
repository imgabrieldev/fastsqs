"""Tier 2 real-AWS e2e harness (opt-in: ``pytest --run-aws``).

Session-scoped: builds a Lambda deployment zip (fastsqs + the e2e handler +
linux-x86_64 pydantic), creates an IAM execution role and a deployed Lambda
function. A per-test ``pipeline`` factory wires a main SQS queue + DLQ
(redrive policy) + an event-source mapping with ReportBatchItemFailures.
Everything is torn down. Uses the ``gabe`` profile.
"""

import io
import json
import shutil
import subprocess
import sys
import time
import uuid
import zipfile
from pathlib import Path

import boto3
import pytest

REGION = "us-east-1"
PROFILE = "gabe"
REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def aws():
    s = boto3.Session(profile_name=PROFILE, region_name=REGION)
    return {"iam": s.client("iam"), "lambda": s.client("lambda"), "sqs": s.client("sqs")}


def _build_zip(tmp: Path) -> bytes:
    build = tmp / "build"
    build.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            sys.executable, "-m", "pip", "install", "--target", str(build),
            "--platform", "manylinux2014_x86_64", "--python-version", "3.13",
            "--only-binary=:all:", "pydantic>=2", "-q",
        ],
        check=True,
    )
    shutil.copytree(REPO / "fastsqs", build / "fastsqs")
    shutil.copy(REPO / "tests" / "aws" / "_e2e_handler.py", build / "lambda_function.py")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in build.rglob("*"):
            if p.is_file() and "__pycache__" not in str(p):
                z.write(p, p.relative_to(build))
    return buf.getvalue()


@pytest.fixture(scope="session")
def deployed_lambda(aws, tmp_path_factory):
    iam, lam = aws["iam"], aws["lambda"]
    suffix = uuid.uuid4().hex[:8]
    role_name = f"fastsqs-e2e-role-{suffix}"
    fn_name = f"fastsqs-e2e-{suffix}"
    trust = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
    perms = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"], "Resource": "*"}]}

    zip_bytes = _build_zip(tmp_path_factory.mktemp("lambda"))
    role_arn = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
    iam.put_role_policy(RoleName=role_name, PolicyName="perms", PolicyDocument=json.dumps(perms))
    try:
        for _ in range(15):  # IAM role propagation
            try:
                lam.create_function(
                    FunctionName=fn_name, Runtime="python3.13", Architectures=["x86_64"],
                    Handler="lambda_function.lambda_handler", Role=role_arn,
                    Code={"ZipFile": zip_bytes}, Timeout=10, MemorySize=256)
                break
            except lam.exceptions.InvalidParameterValueException:
                time.sleep(3)
        else:
            raise RuntimeError("Lambda did not accept the role in time")
        lam.get_waiter("function_active_v2").wait(FunctionName=fn_name)
        yield fn_name
    finally:
        try:
            lam.delete_function(FunctionName=fn_name)
        except Exception:
            pass
        try:
            iam.delete_role_policy(RoleName=role_name, PolicyName="perms")
            iam.delete_role(RoleName=role_name)
        except Exception:
            pass


@pytest.fixture
def pipeline(aws, deployed_lambda):
    """Factory: create a main queue + DLQ + ESM bound to the deployed Lambda.

    Returns ``make(fifo=False, max_receive_count=2, visibility=2) -> (main_url, dlq_url)``.
    """
    sqs, lam = aws["sqs"], aws["lambda"]
    queues: list = []
    esms: list = []

    def make(fifo: bool = False, max_receive_count: int = 2, visibility: int = 10):
        sfx = uuid.uuid4().hex[:8]
        ext = ".fifo" if fifo else ""
        fifo_attrs = {"FifoQueue": "true", "ContentBasedDeduplication": "true"} if fifo else {}
        dlq_url = sqs.create_queue(QueueName=f"fastsqs-e2e-dlq-{sfx}{ext}", Attributes=dict(fifo_attrs))["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        main_url = sqs.create_queue(
            QueueName=f"fastsqs-e2e-main-{sfx}{ext}",
            Attributes={**fifo_attrs, "VisibilityTimeout": str(visibility),
                        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(max_receive_count)})},
        )["QueueUrl"]
        main_arn = sqs.get_queue_attributes(QueueUrl=main_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        esm = lam.create_event_source_mapping(
            EventSourceArn=main_arn, FunctionName=deployed_lambda, Enabled=True,
            BatchSize=10, FunctionResponseTypes=["ReportBatchItemFailures"])["UUID"]
        for _ in range(30):
            if lam.get_event_source_mapping(UUID=esm)["State"] == "Enabled":
                break
            time.sleep(2)
        queues.extend([main_url, dlq_url])
        esms.append(esm)
        return main_url, dlq_url

    yield make

    for esm in esms:
        try:
            lam.delete_event_source_mapping(UUID=esm)
        except Exception:
            pass
    time.sleep(5)  # ESM deletion is async; let it detach before deleting queues
    for url in queues:
        for _ in range(4):  # retry: delete can race with async ESM detachment
            try:
                sqs.delete_queue(QueueUrl=url)
                break
            except Exception:
                time.sleep(3)


@pytest.fixture
def drain(aws):
    """Return ``drain(url, timeout=120, min_count=1) -> [raw_body, ...]``: poll a
    queue (e.g. a DLQ) until at least ``min_count`` messages arrive or timeout,
    deleting them as read (so re-polls don't double-count). Bodies are returned
    raw (callers parse) so malformed payloads are handled."""
    def _drain(url, timeout=120, min_count=1):
        got = []
        deadline = time.time() + timeout
        while time.time() < deadline and len(got) < min_count:
            r = aws["sqs"].receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
            for m in r.get("Messages", []):
                got.append(m["Body"])
                aws["sqs"].delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
            if len(got) < min_count:
                time.sleep(1)
        return got
    return _drain
