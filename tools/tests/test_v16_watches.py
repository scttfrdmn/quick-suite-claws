"""
Tests for clAWS v0.16.0 science literature surveillance watches.

Covers:
- #71: literature watch — watch handler validation + runner _run_literature_watch
- #72: cross_discipline watch — watch handler validation + runner _run_cross_discipline_watch
- call_router() grounding_mode param passthrough
"""

import json  # noqa: I001
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MOCK_PLAN = {
    "plan_id": "plan-lit00001",
    "source_id": "athena:pubmed_results",
    "query": "SELECT * FROM papers LIMIT 50",
    "status": "ready",
}


def _create_watch(body: dict) -> dict:
    from tools.watch.handler import _create
    return _create(body, "user:alice", "req-001")


def _paper_row(title="Crispr in bacteria", abstract="CRISPR-Cas9 mechanism study", score=0.85):
    return {"pmid": "12345", "title": title, "abstract_text": abstract, "_score": score}


# ---------------------------------------------------------------------------
# Literature watch — handler validation (#71)
# ---------------------------------------------------------------------------

class TestLiteratureWatchValidation:
    def test_literature_without_semantic_match_returns_error(self):
        with patch("tools.watch.handler.load_plan", return_value=_MOCK_PLAN), \
             patch("tools.watch.handler.store_watch"), \
             patch("tools.watch.handler._create_schedule"):
            result = _create_watch({
                "plan_id": "plan-lit00001",
                "schedule": "rate(1 day)",
                "type": "literature",
                # no semantic_match
            })
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "semantic_match" in body["error"]

    def test_literature_without_ssm_key_returns_error(self):
        with patch("tools.watch.handler.load_plan", return_value=_MOCK_PLAN), \
             patch("tools.watch.handler.store_watch"), \
             patch("tools.watch.handler._create_schedule"):
            result = _create_watch({
                "plan_id": "plan-lit00001",
                "schedule": "rate(1 day)",
                "type": "literature",
                "semantic_match": {"abstract_similarity_threshold": 0.75},  # missing key
            })
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "lab_profile_ssm_key" in body["error"]

    def test_literature_with_valid_spec_creates_successfully(self):
        with patch("tools.watch.handler.load_plan", return_value=_MOCK_PLAN), \
             patch("tools.watch.handler.store_watch") as mock_store, \
             patch("tools.watch.handler._create_schedule"), \
             patch("tools.watch.handler.audit_log"), \
             patch("tools.watch.handler.new_watch_id", return_value="watch-lit00001"):
            result = _create_watch({
                "plan_id": "plan-lit00001",
                "schedule": "rate(1 day)",
                "type": "literature",
                "semantic_match": {
                    "lab_profile_ssm_key": "/quick-suite/claws/lab/crispr-lab-profile",
                    "abstract_similarity_threshold": 0.75,
                },
                "reagent_config_uri": "s3://claws-config/reagents.json",
                "protocol_config_uri": "s3://claws-config/protocols.json",
            })
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["watch_id"] == "watch-lit00001"
        stored = mock_store.call_args[0][1]
        assert stored["type"] == "literature"
        assert stored["semantic_match"]["lab_profile_ssm_key"] == "/quick-suite/claws/lab/crispr-lab-profile"
        assert stored["reagent_config_uri"] == "s3://claws-config/reagents.json"
        assert stored["protocol_config_uri"] == "s3://claws-config/protocols.json"


# ---------------------------------------------------------------------------
# Literature watch — runner _run_literature_watch (#71)
# ---------------------------------------------------------------------------

class TestRunLiteratureWatch:
    def _run(self, watch: dict, rows: list) -> list:
        from tools.watch.runner import _run_literature_watch
        return _run_literature_watch(watch, rows)

    def _watch(self, **kwargs):
        base = {
            "type": "literature",
            "semantic_match": {
                "lab_profile_ssm_key": "/quick-suite/claws/lab/profile",
                "abstract_similarity_threshold": 0.75,
            },
        }
        base.update(kwargs)
        return base

    def test_paper_above_threshold_included_with_relevance_type(self):
        watch = self._watch()
        rows = [_paper_row()]
        with patch("tools.watch.runner._ssm") as mock_ssm, \
             patch("tools.watch.runner.call_router", return_value="0.88"):
            mock_ssm.return_value.get_parameter.return_value = {
                "Parameter": {"Value": "Lab studies CRISPR in bacteria"}
            }
            result = self._run(watch, rows)
        assert len(result) == 1
        assert "relevance_type" in result[0]
        assert "validation_steps" in result[0]
        assert result[0]["relevance_type"] == "methodology"
        assert result[0]["validation_steps"] == ["cite_and_review"]

    def test_paper_below_threshold_excluded(self):
        watch = self._watch()
        rows = [_paper_row()]
        with patch("tools.watch.runner._ssm") as mock_ssm, \
             patch("tools.watch.runner.call_router", return_value="0.30"):
            mock_ssm.return_value.get_parameter.return_value = {
                "Parameter": {"Value": "Completely different research area"}
            }
            result = self._run(watch, rows)
        assert result == []

    def test_reagent_config_uri_sets_reagent_relevance_type(self):
        watch = self._watch(reagent_config_uri="s3://bucket/reagents.json")
        rows = [_paper_row(abstract="anti-GFP antibody catalog RRID:AB_123456")]
        with patch("tools.watch.runner._ssm") as mock_ssm, \
             patch("tools.watch.runner.call_router", return_value="0.82"), \
             patch("tools.watch.runner.load_config_from_uri", return_value=["anti-gfp", "antibody"]):
            mock_ssm.return_value.get_parameter.return_value = {
                "Parameter": {"Value": "We use anti-GFP antibodies in our experiments"}
            }
            result = self._run(watch, rows)
        assert len(result) == 1
        assert result[0]["relevance_type"] == "reagent"
        assert "confirm_antibody_catalog_number" in result[0]["validation_steps"]

    def test_protocol_config_uri_sets_protocol_relevance_type(self):
        watch = self._watch(protocol_config_uri="s3://bucket/protocols.json")
        rows = [_paper_row(abstract="We performed western blot and ELISA assay")]
        with patch("tools.watch.runner._ssm") as mock_ssm, \
             patch("tools.watch.runner.call_router", return_value="0.80"), \
             patch("tools.watch.runner.load_config_from_uri", return_value=["western blot", "elisa"]):
            mock_ssm.return_value.get_parameter.return_value = {
                "Parameter": {"Value": "Our lab uses western blot techniques"}
            }
            result = self._run(watch, rows)
        assert len(result) == 1
        assert result[0]["relevance_type"] == "protocol"
        assert "replicate_protocol" in result[0]["validation_steps"]

    def test_router_failure_skips_paper_no_crash(self):
        watch = self._watch()
        rows = [_paper_row(), _paper_row(title="Another paper")]
        with patch("tools.watch.runner._ssm") as mock_ssm, \
             patch("tools.watch.runner.call_router", side_effect=Exception("Router down")):
            mock_ssm.return_value.get_parameter.return_value = {
                "Parameter": {"Value": "Lab profile text"}
            }
            result = self._run(watch, rows)
        assert result == []

    def test_empty_rows_returns_empty(self):
        watch = self._watch()
        with patch("tools.watch.runner._ssm") as mock_ssm, \
             patch("tools.watch.runner.call_router"):
            mock_ssm.return_value.get_parameter.return_value = {
                "Parameter": {"Value": "Lab profile text"}
            }
            result = self._run(watch, [])
        assert result == []

    def test_ssm_failure_returns_empty(self):
        watch = self._watch()
        rows = [_paper_row()]
        with patch("tools.watch.runner._ssm") as mock_ssm:
            mock_ssm.return_value.get_parameter.side_effect = Exception("SSM unavailable")
            result = self._run(watch, rows)
        assert result == []

    def test_matches_sorted_by_score_descending(self):
        watch = self._watch()
        rows = [
            _paper_row(title="Low score paper", abstract="vaguely related"),
            _paper_row(title="High score paper", abstract="very relevant crispr study"),
        ]
        scores = iter(["0.78", "0.95"])
        with patch("tools.watch.runner._ssm") as mock_ssm, \
             patch("tools.watch.runner.call_router", side_effect=lambda *a, **kw: next(scores)):
            mock_ssm.return_value.get_parameter.return_value = {
                "Parameter": {"Value": "CRISPR lab research"}
            }
            result = self._run(watch, rows)
        assert len(result) == 2
        assert result[0]["_relevance_score"] >= result[1]["_relevance_score"]


# ---------------------------------------------------------------------------
# Cross-discipline watch — handler validation (#72)
# ---------------------------------------------------------------------------

class TestCrossDisciplineWatchValidation:
    def test_cross_discipline_without_open_problems_uri_returns_error(self):
        with patch("tools.watch.handler.load_plan", return_value=_MOCK_PLAN), \
             patch("tools.watch.handler.store_watch"), \
             patch("tools.watch.handler._create_schedule"):
            result = _create_watch({
                "plan_id": "plan-lit00001",
                "schedule": "rate(1 day)",
                "type": "cross_discipline",
                "primary_field": "structural biology",
                # no open_problems_uri
            })
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "open_problems_uri" in body["error"]

    def test_cross_discipline_without_primary_field_returns_error(self):
        with patch("tools.watch.handler.load_plan", return_value=_MOCK_PLAN), \
             patch("tools.watch.handler.store_watch"), \
             patch("tools.watch.handler._create_schedule"):
            result = _create_watch({
                "plan_id": "plan-lit00001",
                "schedule": "rate(1 day)",
                "type": "cross_discipline",
                "open_problems_uri": "s3://claws-config/gaps.json",
                # no primary_field
            })
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "primary_field" in body["error"]

    def test_cross_discipline_with_valid_spec_creates_successfully(self):
        with patch("tools.watch.handler.load_plan", return_value=_MOCK_PLAN), \
             patch("tools.watch.handler.store_watch") as mock_store, \
             patch("tools.watch.handler._create_schedule"), \
             patch("tools.watch.handler.audit_log"), \
             patch("tools.watch.handler.new_watch_id", return_value="watch-cd00001"):
            result = _create_watch({
                "plan_id": "plan-lit00001",
                "schedule": "rate(1 day)",
                "type": "cross_discipline",
                "open_problems_uri": "s3://claws-config/gaps.json",
                "primary_field": "structural biology",
                "field_distance": 0.6,
                "citations_in_primary_field": 3,
            })
        assert result["statusCode"] == 200
        stored = mock_store.call_args[0][1]
        assert stored["type"] == "cross_discipline"
        assert stored["open_problems_uri"] == "s3://claws-config/gaps.json"
        assert stored["primary_field"] == "structural biology"
        assert stored["field_distance"] == 0.6
        assert stored["citations_in_primary_field"] == 3


# ---------------------------------------------------------------------------
# Cross-discipline watch — runner _run_cross_discipline_watch (#72)
# ---------------------------------------------------------------------------

class TestRunCrossDisciplineWatch:
    def _run(self, watch: dict, rows: list) -> list:
        from tools.watch.runner import _run_cross_discipline_watch
        return _run_cross_discipline_watch(watch, rows)

    def _watch(self, **kwargs):
        base = {
            "type": "cross_discipline",
            "open_problems_uri": "s3://claws-config/gaps.json",
            "primary_field": "structural biology",
            "field_distance": 0.5,
            "citations_in_primary_field": 5,
        }
        base.update(kwargs)
        return base

    def _gap_list(self):
        return [{"gap_statement": "Protein folding energy landscape is unknown", "domain": "biochemistry"}]

    def test_qualifying_paper_returned_with_gap_metadata(self):
        watch = self._watch()
        rows = [_paper_row(abstract="Machine learning approach to protein folding")]
        router_response = '{"cross_field_score": 0.75, "source_field": "machine learning", "citations_in_primary_field": 2}'
        with patch("tools.watch.runner.load_config_from_uri", return_value=self._gap_list()), \
             patch("tools.watch.runner.call_router", return_value=router_response):
            result = self._run(watch, rows)
        assert len(result) == 1
        assert result[0]["gap_id"] == "gap-0"
        assert result[0]["source_field"] == "machine learning"
        assert result[0]["cross_field_score"] == 0.75
        assert "gap_statement" in result[0]

    def test_paper_below_field_distance_excluded(self):
        watch = self._watch(field_distance=0.8)
        rows = [_paper_row()]
        router_response = '{"cross_field_score": 0.4, "source_field": "chemistry", "citations_in_primary_field": 1}'
        with patch("tools.watch.runner.load_config_from_uri", return_value=self._gap_list()), \
             patch("tools.watch.runner.call_router", return_value=router_response):
            result = self._run(watch, rows)
        assert result == []

    def test_paper_with_high_primary_field_citations_excluded(self):
        watch = self._watch(citations_in_primary_field=5)
        rows = [_paper_row()]
        router_response = '{"cross_field_score": 0.85, "source_field": "physics", "citations_in_primary_field": 20}'
        with patch("tools.watch.runner.load_config_from_uri", return_value=self._gap_list()), \
             patch("tools.watch.runner.call_router", return_value=router_response):
            result = self._run(watch, rows)
        assert result == []

    def test_open_problems_uri_ssm_loaded(self):
        watch = self._watch(open_problems_uri="ssm:/claws/gaps")
        rows = [_paper_row()]
        router_response = '{"cross_field_score": 0.7, "source_field": "cs", "citations_in_primary_field": 0}'
        with patch("tools.watch.runner.load_config_from_uri", return_value=self._gap_list()) as mock_load, \
             patch("tools.watch.runner.call_router", return_value=router_response):
            self._run(watch, rows)
        mock_load.assert_called_once_with("ssm:/claws/gaps")

    def test_router_failure_skips_paper_no_crash(self):
        watch = self._watch()
        rows = [_paper_row()]
        with patch("tools.watch.runner.load_config_from_uri", return_value=self._gap_list()), \
             patch("tools.watch.runner.call_router", side_effect=Exception("network error")):
            result = self._run(watch, rows)
        assert result == []

    def test_all_rules_satisfied_empty_results(self):
        watch = self._watch()
        rows = []
        with patch("tools.watch.runner.load_config_from_uri", return_value=self._gap_list()), \
             patch("tools.watch.runner.call_router"):
            result = self._run(watch, rows)
        assert result == []

    def test_open_problems_uri_load_failure_returns_empty(self):
        watch = self._watch()
        rows = [_paper_row()]
        with patch("tools.watch.runner.load_config_from_uri", side_effect=Exception("S3 unavailable")):
            result = self._run(watch, rows)
        assert result == []


# ---------------------------------------------------------------------------
# call_router grounding_mode parameter (#72 dependency)
# ---------------------------------------------------------------------------

class TestCallRouterGroundingMode:
    def test_strict_grounding_mode_included_in_router_body(self):
        """call_router passes grounding_mode to the router request body when not default."""
        captured = {}

        class _MockResp:
            def read(self): return json.dumps({"content": "research result"}).encode()
            def __enter__(self): return self
            def __exit__(self, *a): pass

        def _fake_urlopen(req, timeout=30):
            if "/token" in req.full_url:
                class _Token:
                    def read(self): return json.dumps({"access_token": "tok"}).encode()
                    def __enter__(self): return self
                    def __exit__(self, *a): pass
                return _Token()
            captured["body"] = json.loads(req.data.decode())
            return _MockResp()

        import importlib
        import os

        import tools.shared as _shared

        env = {
            "ROUTER_ENDPOINT": "https://router.example.com",
            "ROUTER_TOKEN_URL": "https://cognito.example.com/oauth2/token",
            "ROUTER_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:test",
        }
        with patch.dict(os.environ, env), \
             patch("boto3.client") as mock_boto3, \
             patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            mock_sm = MagicMock()
            mock_sm.get_secret_value.return_value = {
                "SecretString": json.dumps({"client_id": "cid", "client_secret": "csec"})
            }
            mock_boto3.return_value = mock_sm
            importlib.reload(_shared)
            _shared.call_router("research", "test prompt", max_tokens=150, grounding_mode="strict")

        assert captured.get("body", {}).get("grounding_mode") == "strict"

    def test_default_grounding_mode_not_included_in_body(self):
        """call_router omits grounding_mode from request body when it is 'default'."""
        captured = {}

        class _MockResp:
            def read(self): return json.dumps({"content": "ok"}).encode()
            def __enter__(self): return self
            def __exit__(self, *a): pass

        def _fake_urlopen(req, timeout=30):
            if "/token" in req.full_url:
                class _Token:
                    def read(self): return json.dumps({"access_token": "tok"}).encode()
                    def __enter__(self): return self
                    def __exit__(self, *a): pass
                return _Token()
            captured["body"] = json.loads(req.data.decode())
            return _MockResp()

        import importlib
        import os

        import tools.shared as _shared

        env = {
            "ROUTER_ENDPOINT": "https://router.example.com",
            "ROUTER_TOKEN_URL": "https://cognito.example.com/oauth2/token",
            "ROUTER_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123:secret:test",
        }
        with patch.dict(os.environ, env), \
             patch("boto3.client") as mock_boto3, \
             patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            mock_sm = MagicMock()
            mock_sm.get_secret_value.return_value = {
                "SecretString": json.dumps({"client_id": "cid", "client_secret": "csec"})
            }
            mock_boto3.return_value = mock_sm
            importlib.reload(_shared)
            _shared.call_router("summarize", "test prompt", grounding_mode="default")

        assert "grounding_mode" not in captured.get("body", {})
