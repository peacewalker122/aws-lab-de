import hashlib
import json
from datetime import datetime, timezone

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError


def s3_client(config):
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        region_name=config.region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=BotoConfig(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def put_json(s3, bucket, key, payload):
    body = json_body(payload)
    s3.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json"
    )
    return body


def json_body(payload):
    return json.dumps(payload, sort_keys=True, default=str).encode()


def json_digest(payload):
    return sha256_hex(json_body(payload))


def get_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as error:
        if error.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return None
        raise


def read_bytes(s3, bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def list_keys(s3, bucket, prefix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return keys


def delete_keys(s3, bucket, keys):
    for start in range(0, len(keys), 1000):
        chunk = keys[start : start + 1000]
        s3.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": key} for key in chunk]}
        )


def parse_instant(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(
            f"Manifest cursor timestamp {value!r} has no UTC offset; "
            "naive timestamps are ambiguous and are rejected"
        )
    return parsed.astimezone(timezone.utc)


def cursor_end(manifest):
    timestamp, identifier = manifest["cursor"]["end"]
    return (parse_instant(timestamp), identifier)


def raw_manifests(s3, bucket, dataset):
    prefix = f"{dataset}/"
    manifests = [
        json.loads(read_bytes(s3, bucket, key))
        for key in list_keys(s3, bucket, prefix)
        if key.endswith("/manifest.json")
    ]
    return sorted(manifests, key=cursor_end)


def s3_uri(bucket, key):
    return f"s3://{bucket}/{key}"


def committed_pointer(s3, bucket, zone):
    return get_json(s3, bucket, f"{zone}/_committed.json")
