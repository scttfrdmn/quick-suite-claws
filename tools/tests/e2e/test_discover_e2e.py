"""
E2E tests for claws-discover.

Runs against the deployed ClawsToolsStack. Tests Glue catalog search
and error handling without requiring any particular data to be present.
"""

import pytest

from tools.tests.e2e.conftest import invoke

pytestmark = pytest.mark.e2e


class TestDiscoverE2E:
    def test_discover_missing_query_returns_error(self, lam):
        """discover without query returns an error."""
        result = invoke(lam, "claws-discover", {})
        assert "error" in result, f"Expected error for missing query: {result}"

    def test_discover_empty_domains_returns_empty_sources(self, lam):
        """discover with no domains in scope returns empty sources list."""
        result = invoke(lam, "claws-discover", {
            "query": "sample",
            "scope": {"domains": []},
        })
        assert "error" not in result, f"Unexpected error: {result}"
        sources = result.get("sources", [])
        assert isinstance(sources, list), f"Expected sources list: {result}"
        assert len(sources) == 0, f"Expected empty sources with no domains: {sources}"

    def test_discover_athena_domain_returns_sources_list(self, lam, glue_table):
        """discover with athena domain returns a list (finds the e2e table)."""
        result = invoke(lam, "claws-discover", {
            "query": "sample",
            "scope": {"domains": ["athena"]},
        })
        assert "error" not in result, f"Unexpected error: {result}"
        sources = result.get("sources", [])
        assert isinstance(sources, list), f"Expected sources list: {result}"

    def test_discover_finds_e2e_table(self, lam, glue_table):
        """discover with athena domain finds the e2e Glue table by name."""
        result = invoke(lam, "claws-discover", {
            "query": "sample_data",
            "scope": {"domains": ["athena"]},
        })
        assert "error" not in result, f"Unexpected error: {result}"
        sources = result.get("sources", [])
        source_ids = [s.get("id", "") for s in sources]
        assert any("sample_data" in sid or "claws_e2e" in sid for sid in source_ids), \
            f"E2E table not found in sources: {source_ids}"

    def test_discover_source_has_required_fields(self, lam, glue_table):
        """Each discovered source has id, kind, and confidence fields."""
        result = invoke(lam, "claws-discover", {
            "query": "sample",
            "scope": {"domains": ["athena"]},
        })
        sources = result.get("sources", [])
        for src in sources:
            assert "id" in src, f"Source missing 'id': {src}"
            assert "kind" in src, f"Source missing 'kind': {src}"
            assert "confidence" in src, f"Source missing 'confidence': {src}"

    def test_discover_confidence_is_between_0_and_1(self, lam, glue_table):
        """Confidence scores are in [0, 1]."""
        result = invoke(lam, "claws-discover", {
            "query": "sample",
            "scope": {"domains": ["athena"]},
        })
        for src in result.get("sources", []):
            conf = src.get("confidence", -1)
            assert 0 <= conf <= 1.0, f"Confidence out of range: {conf}"

    def test_discover_limit_is_respected(self, lam, glue_table):
        """discover returns at most limit sources."""
        result = invoke(lam, "claws-discover", {
            "query": "a",
            "scope": {"domains": ["athena"]},
            "limit": 1,
        })
        sources = result.get("sources", [])
        assert len(sources) <= 1, f"Expected at most 1 source: {sources}"

    def test_discover_s3_domain_with_known_bucket(self, lam, runs_bucket):
        """discover with s3 domain searches bucket prefixes."""
        result = invoke(lam, "claws-discover", {
            "query": "e2e",
            "scope": {
                "domains": ["s3"],
                "spaces": [runs_bucket],
            },
        })
        assert "error" not in result, f"Unexpected error: {result}"
        sources = result.get("sources", [])
        assert isinstance(sources, list), f"Expected sources list: {result}"
        # S3 prefix 'e2e-test/' should be found
        source_ids = [s.get("id", "") for s in sources]
        assert any("e2e" in sid.lower() for sid in source_ids), \
            f"Expected e2e prefix in sources: {source_ids}"
