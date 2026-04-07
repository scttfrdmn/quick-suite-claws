"""clAWS typed exception hierarchy.

All exceptions carry a message and an HTTP-equivalent status code for
use in Lambda response helpers.
"""


class ClawsError(Exception):
    """Base exception for all clAWS tool errors."""

    def __init__(self, message: str, status_code: int = 500) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class ValidationError(ClawsError):
    """Request failed input validation (400)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=400)


class NotFoundError(ClawsError):
    """Referenced resource does not exist (404)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=404)


class ForbiddenError(ClawsError):
    """Request denied by policy or plan linkage check (403)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=403)


class ExecutionError(ClawsError):
    """Query or tool execution failed unexpectedly (500)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=500)


class UpstreamError(ClawsError):
    """Upstream AWS service returned an error (502)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=502)


class GuardrailBlockedError(ClawsError):
    """Bedrock Guardrail intervened on content (200 — blocked is a valid outcome)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=200)
