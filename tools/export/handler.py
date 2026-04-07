"""clAWS export tool — materialize results to an approved destination.

Export payload is scanned via ApplyGuardrail as a final content gate.
Provenance chain is included when requested.
"""

import csv
import hashlib
import hmac
import io
import json
import os
from datetime import UTC, datetime
from typing import Any

import boto3

from tools.shared import (
    RUNS_BUCKET,
    audit_log,
    dynamodb_resource,
    error,
    load_result,
    new_export_id,
    s3_client,
    scan_payload,
    success,
)

EVENTS_CLIENT = None
CALLBACK_SECRET = os.environ.get("CLAWS_CALLBACK_SECRET", "")
QUICKSIGHT_ACCOUNT_ID = os.environ.get("QUICKSIGHT_ACCOUNT_ID", "")
CLAWS_LOOKUP_TABLE = os.environ.get("CLAWS_LOOKUP_TABLE", "")
# Comma-separated URI prefixes for export destination allowlist (#80).
# When set, all export destinations must match at least one prefix.
# Callback destinations additionally require HTTPS regardless of this setting.
# Leave unset to allow any destination (backward compatible).
CLAWS_EXPORT_ALLOWED_DESTINATIONS = os.environ.get("CLAWS_EXPORT_ALLOWED_DESTINATIONS", "")

_qs_client = None


def _validate_destination_uri(dest_type: str, dest_uri: str) -> str | None:
    """Validate export destination URI against the operator allowlist.

    Returns None on success, or an error message string on failure.

    Callback destinations always require HTTPS regardless of allowlist configuration.
    All other destinations are allowed when CLAWS_EXPORT_ALLOWED_DESTINATIONS is unset
    (backward compatible). When set, the URI must start with one of the listed prefixes.
    """
    if dest_type == "callback" and not dest_uri.startswith("https://"):
        return "Callback destination must use HTTPS"
    allowlist_raw = CLAWS_EXPORT_ALLOWED_DESTINATIONS.strip()
    if not allowlist_raw:
        return None  # no allowlist configured — allow any destination
    allowed = [p.strip() for p in allowlist_raw.split(",") if p.strip()]
    if any(dest_uri.startswith(p) for p in allowed):
        return None
    return "Destination URI is not in the approved allowlist"


def _quicksight_client() -> Any:
    global _qs_client
    if _qs_client is None:
        _qs_client = boto3.client("quicksight")
    return _qs_client


def _events_client() -> Any:
    global EVENTS_CLIENT
    if EVENTS_CLIENT is None:
        EVENTS_CLIENT = boto3.client("events")
    return EVENTS_CLIENT


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.export."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    run_id = body.get("run_id", "")
    destination = body.get("destination", {})
    include_provenance = body.get("include_provenance", True)
    export_mode = body.get("mode", "overwrite")  # "overwrite" | "append"
    diff_summary = body.get("diff_summary")      # optional drift diff_results dict
    principal = event.get("requestContext", {}).get("authorizer", {}).get("principalId", "unknown")
    request_id = event.get("requestContext", {}).get("requestId", "")

    if not run_id:
        return error("run_id is required")
    if not destination or not destination.get("type") or not destination.get("uri"):
        return error("destination with type and uri is required")

    # Load results
    try:
        payload = load_result(run_id)
    except Exception as e:
        return error(f"Failed to load results for {run_id}: {e}", status_code=404)

    # Final content scan before export
    scan_result = scan_payload(payload)
    if scan_result["status"] == "blocked":
        audit_log("export", principal, body, {
            "status": "blocked",
            "reason": "Export payload contains sensitive content",
        }, request_id=request_id)
        return success({
            "status": "blocked",
            "reason": "Export payload contains sensitive content detected by guardrail",
        })

    export_id = new_export_id()

    # Build provenance if requested
    provenance = None
    if include_provenance:
        provenance = _build_provenance(run_id, principal, destination, export_mode, diff_summary)

    # Export to destination
    dest_type = destination["type"]
    dest_uri = destination["uri"]

    # Validate destination URI against operator allowlist (#80)
    uri_error = _validate_destination_uri(dest_type, dest_uri)
    if uri_error:
        audit_log("export", principal, body, {
            "status": "rejected",
            "reason": uri_error,
        }, request_id=request_id)
        return error(uri_error)

    if dest_type == "s3":
        result = _export_to_s3(dest_uri, payload, provenance, export_id, export_mode)
    elif dest_type == "eventbridge":
        result = _export_to_eventbridge(dest_uri, payload, export_id)
    elif dest_type == "callback":
        result = _export_to_callback(dest_uri, payload, export_id)
    elif dest_type == "quicksight":
        result = _export_to_quicksight(dest_uri, payload, run_id, export_id)
    else:
        return error(f"Unsupported destination type: {dest_type}")

    if result.get("status") == "error":
        return error(result["error"], status_code=500)

    response_body = {
        "export_id": export_id,
        "status": "complete",
        "destination_uri": result.get("actual_uri", dest_uri),
        "export_mode": export_mode,
    }
    if provenance:
        response_body["provenance_uri"] = result.get("provenance_uri")
    if result.get("dataset_id"):
        response_body["dataset_id"] = result["dataset_id"]

    audit_log("export", principal, body, response_body, request_id=request_id)

    return success(response_body)


def _build_provenance(
    run_id: str,
    principal: str,
    destination: dict,
    export_mode: str = "overwrite",
    diff_summary: dict | None = None,
) -> dict:
    """Build a provenance record tracing the full excavation chain."""
    prov: dict = {
        "export_timestamp": datetime.now(UTC).isoformat(),
        "principal": principal,
        "run_id": run_id,
        "destination": destination,
        "export_mode": export_mode,
        "chain": {
            "note": "Full provenance chain: plan → query → raw result → refinement → export",
            "run_id": run_id,
        },
    }
    if diff_summary is not None:
        prov["diff_summary"] = diff_summary
    return prov


def _export_to_s3(
    uri: str,
    payload: Any,
    provenance: dict | None,
    export_id: str,
    export_mode: str = "overwrite",
) -> dict:
    """Export results to S3.

    mode="overwrite" (default): writes to the specified key, replacing any existing object.
    mode="append": writes to a new timestamped key under the same prefix, leaving existing
                   objects intact.
    """
    try:
        # Parse s3://bucket/key
        parts = uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        base_key = parts[1] if len(parts) > 1 else f"claws-export-{export_id}.json"

        if export_mode == "append":
            # Derive a timestamped variant: base becomes a prefix (strip extension)
            ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            stem = base_key.rsplit(".", 1)[0] if "." in base_key else base_key
            key = f"{stem}-{ts}-{export_id}.json"
        else:
            key = base_key

        # Write results
        s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, default=str),
            ContentType="application/json",
            Metadata={"claws-export-id": export_id, "claws-export-mode": export_mode},
        )

        result: dict = {"status": "complete", "actual_uri": f"s3://{bucket}/{key}"}

        # Write provenance alongside results
        if provenance:
            prov_key = key.rsplit(".", 1)[0] + ".provenance.json"
            s3_client().put_object(
                Bucket=bucket,
                Key=prov_key,
                Body=json.dumps(provenance, default=str, indent=2),
                ContentType="application/json",
            )
            result["provenance_uri"] = f"s3://{bucket}/{prov_key}"

        return result

    except Exception as e:
        return {"status": "error", "error": f"S3 export failed: {e}"}


def _export_to_eventbridge(uri: str, payload: Any, export_id: str) -> dict:
    """Export results as an EventBridge event.

    URI format: events://event-bus-name/detail-type
    """
    remainder = uri.replace("events://", "", 1)
    parts = remainder.split("/", 1)
    event_bus = parts[0]
    detail_type = parts[1] if len(parts) > 1 else "ClawsExportReady"

    try:
        response = _events_client().put_events(Entries=[{
            "Source": "claws",
            "DetailType": detail_type,
            "Detail": json.dumps({
                "export_id": export_id,
                "row_count": len(payload) if isinstance(payload, list) else 1,
                "payload": payload,
            }, default=str),
            "EventBusName": event_bus,
        }])
        failed = response.get("FailedEntryCount", 0)
        if failed > 0:
            return {"status": "error", "error": f"{failed} EventBridge entries failed"}
        return {"status": "complete", "event_bus": event_bus, "detail_type": detail_type}
    except Exception as e:
        return {"status": "error", "error": f"EventBridge export failed: {e}"}


def _export_to_callback(uri: str, payload: Any, export_id: str) -> dict:
    """Export results via HTTP POST callback with optional HMAC-SHA256 signature.

    Set CLAWS_CALLBACK_SECRET env var to enable X-Claws-Signature header.
    """
    import requests as _requests  # noqa: PLC0415

    body = json.dumps({"export_id": export_id, "payload": payload}, default=str)
    headers: dict = {"Content-Type": "application/json", "X-Claws-Export-Id": export_id}

    if CALLBACK_SECRET:
        sig = hmac.new(CALLBACK_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
        headers["X-Claws-Signature"] = f"sha256={sig}"

    try:
        resp = _requests.post(uri, data=body, headers=headers, timeout=10)
        resp.raise_for_status()
        return {"status": "complete", "http_status": resp.status_code}
    except Exception as e:
        return {"status": "error", "error": f"Callback export failed: {e}"}


def _export_to_quicksight(uri: str, payload: Any, run_id: str, export_id: str) -> dict:
    """Export results to Quick Sight as a new SPICE dataset.

    URI format: quicksight://dataset-name

    Writes a CSV to S3, creates a QuickSight data source and dataset on top
    of it, and registers the resulting dataset ID in ClawsLookupTable so the
    dataset can be resolved via claws:// URIs in downstream compute jobs.
    """
    if not QUICKSIGHT_ACCOUNT_ID:
        return {"status": "error", "error": "QUICKSIGHT_ACCOUNT_ID not configured"}

    dataset_name = uri.replace("quicksight://", "").strip("/") or f"claws-{export_id}"
    source_id = f"claws-{run_id}"

    # Convert payload to CSV rows
    rows = payload if isinstance(payload, list) else [payload]
    if not rows:
        return {"status": "error", "error": "No results to export to Quick Sight"}

    first = rows[0]
    columns = list(first.keys()) if isinstance(first, dict) else ["value"]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row if isinstance(row, dict) else {"value": row})

    # Write CSV and manifest to S3 in the runs bucket
    csv_key = f"{run_id}/export-{export_id}.csv"
    manifest_key = f"{run_id}/export-{export_id}-manifest.json"
    s3_client().put_object(
        Bucket=RUNS_BUCKET,
        Key=csv_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    manifest = {
        "fileLocations": [{"URIs": [f"s3://{RUNS_BUCKET}/{csv_key}"]}],
        "globalUploadSettings": {
            "format": "CSV",
            "delimiter": ",",
            "containsHeader": "true",
        },
    }
    s3_client().put_object(
        Bucket=RUNS_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest),
        ContentType="application/json",
    )

    region = os.environ.get("AWS_REGION", "us-east-1")
    ds_id = f"claws-ds-{export_id}"
    dataset_id = f"claws-dset-{export_id}"

    try:
        _quicksight_client().create_data_source(
            AwsAccountId=QUICKSIGHT_ACCOUNT_ID,
            DataSourceId=ds_id,
            Name=f"claws-{dataset_name}",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {
                    "ManifestFileLocation": {
                        "Bucket": RUNS_BUCKET,
                        "Key": manifest_key,
                    }
                }
            },
        )

        _quicksight_client().create_data_set(
            AwsAccountId=QUICKSIGHT_ACCOUNT_ID,
            DataSetId=dataset_id,
            Name=dataset_name,
            ImportMode="SPICE",
            PhysicalTableMap={
                "claws-table": {
                    "S3Source": {
                        "DataSourceArn": (
                            f"arn:aws:quicksight:{region}:{QUICKSIGHT_ACCOUNT_ID}"
                            f":datasource/{ds_id}"
                        ),
                        "UploadSettings": {
                            "Format": "CSV",
                            "StartFromRow": 1,
                            "ContainsHeader": True,
                            "Delimiter": ",",
                        },
                        "InputColumns": [
                            {"Name": col, "Type": "STRING"} for col in columns
                        ],
                    }
                }
            },
        )

        # Register in ClawsLookupTable for claws:// URI resolution
        if CLAWS_LOOKUP_TABLE:
            dynamodb_resource().Table(CLAWS_LOOKUP_TABLE).put_item(Item={
                "source_id": source_id,
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "export_id": export_id,
            })

        return {"status": "complete", "dataset_id": dataset_id, "source_id": source_id}

    except Exception as e:
        return {"status": "error", "error": f"Quick Sight export failed: {e}"}
