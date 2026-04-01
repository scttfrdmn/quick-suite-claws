"""clAWS export tool — materialize results to an approved destination.

Export payload is scanned via ApplyGuardrail as a final content gate.
Provenance chain is included when requested.
"""

import json
from datetime import UTC, datetime
from typing import Any

from tools.shared import (
    audit_log,
    error,
    load_result,
    new_export_id,
    s3_client,
    scan_payload,
    success,
)


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for claws.export."""
    body = json.loads(event.get("body", "{}")) if isinstance(event.get("body"), str) else event
    run_id = body.get("run_id", "")
    destination = body.get("destination", {})
    include_provenance = body.get("include_provenance", True)
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
        provenance = _build_provenance(run_id, principal, destination)

    # Export to destination
    dest_type = destination["type"]
    dest_uri = destination["uri"]

    if dest_type == "s3":
        result = _export_to_s3(dest_uri, payload, provenance, export_id)
    elif dest_type == "eventbridge":
        result = _export_to_eventbridge(dest_uri, payload, export_id)
    elif dest_type == "callback":
        result = _export_to_callback(dest_uri, payload, export_id)
    else:
        return error(f"Unsupported destination type: {dest_type}")

    if result.get("status") == "error":
        return error(result["error"], status_code=500)

    response_body = {
        "export_id": export_id,
        "status": "complete",
        "destination_uri": dest_uri,
    }
    if provenance:
        response_body["provenance_uri"] = result.get("provenance_uri")

    audit_log("export", principal, body, response_body, request_id=request_id)

    return success(response_body)


def _build_provenance(run_id: str, principal: str, destination: dict) -> dict:
    """Build a provenance record tracing the full excavation chain."""
    return {
        "export_timestamp": datetime.now(UTC).isoformat(),
        "principal": principal,
        "run_id": run_id,
        "destination": destination,
        "chain": {
            "note": "Full provenance chain: plan → query → raw result → refinement → export",
            "run_id": run_id,
        },
    }


def _export_to_s3(uri: str, payload: Any, provenance: dict | None, export_id: str) -> dict:
    """Export results to S3."""
    try:
        # Parse s3://bucket/key
        parts = uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else f"claws-export-{export_id}.json"

        # Write results
        s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, default=str),
            ContentType="application/json",
            Metadata={"claws-export-id": export_id},
        )

        result = {"status": "complete"}

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
    """Export results as an EventBridge event."""
    # TODO: Implement EventBridge PutEvents
    return {"status": "error", "error": "EventBridge export not yet implemented"}


def _export_to_callback(uri: str, payload: Any, export_id: str) -> dict:
    """Export results via HTTP callback."""
    # TODO: Implement callback with signature verification
    return {"status": "error", "error": "Callback export not yet implemented"}
