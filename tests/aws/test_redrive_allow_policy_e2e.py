"""Real-AWS e2e: RedriveAllowPolicy=denyAll blocks dead-lettering authorization
(opt-in: ``pytest --run-aws``).

A dead-letter queue carries a ``RedriveAllowPolicy`` that controls which source
queues may name it as their dead-letter target. With
``{"redrivePermission": "denyAll"}`` no source queue is permitted to dead-letter
into it. SQS enforces this at the moment a source queue tries to install a
``RedrivePolicy`` pointing at that DLQ: ``SetQueueAttributes`` is REJECTED with a
``ClientError`` (the redrive never takes), so a misconfigured denyAll silently
breaks dead-lettering for any queue that points at it.

This is an SQS-authorization fact between two queues — no Lambda, ESM, or fastsqs
involvement — so the test is boto-only and uses just the ``aws`` clients fixture
(creating/tearing down its own raw queues). It asserts the rejection AND, as a
no-op fallback for any region/account where SQS accepts-then-ignores instead of
rejecting, confirms via ``GetQueueAttributes`` that the RedrivePolicy did not take.
Harness in conftest.py.
"""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

pytestmark = pytest.mark.aws


def test_denyall_dlq_blocks_dead_lettering(aws):
    """A DLQ with RedriveAllowPolicy={redrivePermission:denyAll} cannot be named
    as a dead-letter target: setting a source queue's RedrivePolicy at it is
    rejected by SQS (ClientError) and the redrive never takes."""
    sqs = aws["sqs"]
    sfx = uuid.uuid4().hex[:8]
    created = []
    try:
        # DLQ that forbids ALL source queues from dead-lettering into it.
        dlq_url = sqs.create_queue(
            QueueName=f"fastsqs-e2e-denyall-dlq-{sfx}",
            Attributes={
                "RedriveAllowPolicy": json.dumps({"redrivePermission": "denyAll"})
            },
        )["QueueUrl"]
        created.append(dlq_url)
        dlq_arn = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        # Sanity: the denyAll policy actually stuck on the DLQ. get_queue_attributes
        # only returns requested keys that are SET, so guard with .get() — if the
        # attribute is absent the response carries no "Attributes" key at all.
        rap = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["RedriveAllowPolicy"]
        ).get("Attributes", {}).get("RedriveAllowPolicy")
        assert rap is not None, "denyAll RedriveAllowPolicy did not stick on the DLQ"
        assert json.loads(rap)["redrivePermission"] == "denyAll"

        # A source queue with no redrive yet.
        main_url = sqs.create_queue(QueueName=f"fastsqs-e2e-denyall-main-{sfx}")[
            "QueueUrl"
        ]
        created.append(main_url)

        redrive_policy = json.dumps(
            {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"}
        )

        # Point the source queue's RedrivePolicy at the denyAll DLQ. Real SQS
        # rejects this with a ClientError; capture whether it raised so the
        # no-op fallback below can still assert the redrive did not take.
        rejected = False
        try:
            sqs.set_queue_attributes(
                QueueUrl=main_url, Attributes={"RedrivePolicy": redrive_policy}
            )
        except ClientError as exc:
            rejected = True
            # SQS surfaces this as an InvalidParameterValue-class rejection.
            code = exc.response.get("Error", {}).get("Code", "")
            assert code in (
                "InvalidParameterValue",
                "InvalidAttributeValue",
                "AccessDenied",
            ), f"unexpected rejection code {code!r}: {exc}"

        # Whether SQS rejected outright or (hypothetically) accepted-then-ignored,
        # the source queue MUST NOT have a RedrivePolicy pointing at the denyAll
        # DLQ — dead-lettering into it is not authorized either way.
        # On rejection the source queue has NO RedrivePolicy, so the response
        # contains no "Attributes" key — guard with .get() on both levels.
        attrs = sqs.get_queue_attributes(
            QueueUrl=main_url, AttributeNames=["RedrivePolicy"]
        ).get("Attributes", {})
        installed = attrs.get("RedrivePolicy")
        took = installed is not None and json.loads(installed).get(
            "deadLetterTargetArn"
        ) == dlq_arn
        assert not took, (
            "denyAll DLQ must not become the source queue's dead-letter target; "
            f"GetQueueAttributes returned RedrivePolicy={installed!r}"
        )
        # At least one of the two enforcement signals must hold (in practice SQS
        # rejects outright).
        assert rejected or not took
    finally:
        for url in created:
            try:
                sqs.delete_queue(QueueUrl=url)
            except Exception:
                pass
