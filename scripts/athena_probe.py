import os
import time
from urllib.parse import urlsplit

import boto3

endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
url = urlsplit(endpoint)
if (
    url.scheme != "http"
    or url.hostname not in {"127.0.0.1", "localhost"}
    or not url.port
):
    raise SystemExit("Athena probe requires a local HTTP endpoint with a port")

bucket = os.environ.get("ASTER_BUCKET", "aster-local-m0-spike")
client = boto3.client(
    "athena",
    endpoint_url=endpoint,
    region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
execution = client.start_query_execution(
    QueryString="SELECT 42 AS answer",
    ResultConfiguration={"OutputLocation": f"s3://{bucket}/athena-results/"},
)["QueryExecutionId"]
for _ in range(30):
    status = client.get_query_execution(QueryExecutionId=execution)["QueryExecution"][
        "Status"
    ]
    if status["State"] == "SUCCEEDED":
        rows = client.get_query_results(QueryExecutionId=execution)["ResultSet"]["Rows"]
        if len(rows) < 2 or rows[1]["Data"][0].get("VarCharValue") != "42":
            raise SystemExit(f"Unexpected Athena result: {rows}")
        print("Athena SELECT 42 returned 42")
        break
    if status["State"] in {"FAILED", "CANCELLED"}:
        raise SystemExit(
            f"Athena query {status['State']}: {status.get('StateChangeReason', 'no reason returned')}"
        )
    time.sleep(1)
else:
    raise SystemExit(f"Athena query {execution} did not finish within 30 seconds")
