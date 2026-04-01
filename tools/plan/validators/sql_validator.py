"""SQL validator for clAWS plan tool.

Parses generated SQL and checks for mutations, dangerous patterns,
and constraint violations before the query is included in a plan.
"""

import re


# Patterns that indicate a mutation — these MUST cause rejection
MUTATION_PATTERNS = [
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bALTER\b",
    r"\bCREATE\b",
    r"\bTRUNCATE\b",
    r"\bMERGE\b",
    r"\bREPLACE\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bEXEC\b",
    r"\bEXECUTE\b",
    r"\bCALL\b",
]

# Patterns that are suspicious but not necessarily mutations
WARNING_PATTERNS = [
    (r"\bINTO\b", "INTO clause detected — may indicate INSERT INTO or SELECT INTO"),
    (r"\bUNION\b", "UNION detected — verify this is intentional"),
    (r";\s*\S", "Multiple statements detected — only single statements allowed"),
]


def validate_sql(query: str, constraints: dict) -> dict:
    """Validate a generated SQL query.

    Args:
        query: The SQL query string to validate.
        constraints: The excavation constraints from the plan request.

    Returns:
        {"ok": True} or {"ok": False, "reason": "..."}
    """
    if not query or not query.strip():
        return {"ok": False, "reason": "Empty query"}

    query_upper = query.upper().strip()

    # Must start with SELECT (or WITH for CTEs)
    if not (query_upper.startswith("SELECT") or query_upper.startswith("WITH")):
        return {
            "ok": False,
            "reason": f"Query must start with SELECT or WITH, got: {query_upper[:20]}...",
        }

    # Check for mutation patterns
    for pattern in MUTATION_PATTERNS:
        if re.search(pattern, query_upper):
            keyword = re.search(pattern, query_upper).group()
            return {
                "ok": False,
                "reason": f"Mutation keyword detected: {keyword}. Only read-only queries allowed.",
            }

    # Check for multiple statements (SQL injection vector)
    # Allow semicolons only at the very end
    stripped = query.strip().rstrip(";").strip()
    if ";" in stripped:
        return {
            "ok": False,
            "reason": "Multiple SQL statements detected. Only single statements allowed.",
        }

    # Check read_only constraint
    if constraints.get("read_only", True):
        # Already covered by mutation check, but be explicit
        pass

    # Collect warnings (non-blocking)
    warnings = []
    for pattern, message in WARNING_PATTERNS:
        if re.search(pattern, query_upper):
            warnings.append(message)

    result = {"ok": True}
    if warnings:
        result["warnings"] = warnings

    return result
