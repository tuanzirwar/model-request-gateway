"""确保慢日志报告不会公开凭据，也不会把未完成日志当成证据。"""

import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "slow_log", Path(__file__).resolve().parent.parent / "scripts/analyze_slow_log.py"
)
slow_log = importlib.util.module_from_spec(spec)
spec.loader.exec_module(slow_log)


def test_digest_redacts_literals_comments_and_identifiers():
    raw = """# User@Host: private-user
# Query_time: 0.100000 Lock_time: 0.001000 Rows_sent: 1 Rows_examined: 10
SET timestamp=123;
SELECT secret_column FROM private_table WHERE key_hash='secret-token' AND id=123;
# Query_time: 0.200000 Lock_time: 0.002000 Rows_sent: 1 Rows_examined: 20
SET timestamp=124;
SELECT secret_column FROM private_table WHERE key_hash='other-token' AND id=456;
"""
    result = slow_log.summarize(raw)
    assert result["logged_statements"] == 2
    assert len(result["fingerprints"]) == 1
    row = result["fingerprints"][0]
    assert row["total_ms"] == 300
    assert row["rows_examined"] == 30
    public = json.dumps(result)
    for secret in ("secret-token", "other-token", "private-user", "private_table", "secret_column"):
        assert secret not in public


def test_digest_excludes_incomplete_tail_and_handles_multiline():
    raw = """# Query_time: 0.010000 Lock_time: 0.000000 Rows_sent: 0 Rows_examined: 1
UPDATE requests
SET status='succeeded' WHERE id='uuid';
# Query_time: 0.100000 Lock_time: 0.000000 Rows_sent: 0 Rows_examined: 1
UPDATE requests SET status='unfinished'
"""
    result = slow_log.summarize(raw)
    assert result["logged_statements"] == 1
    assert result["fingerprints"][0]["operation"] == "UPDATE"


def test_commit_is_classified_with_semicolon():
    report = slow_log.summarize(
        "# Query_time: 0.009 Lock_time: 0.000 Rows_sent: 0 Rows_examined: 0\n"
        "SET timestamp=123;\ncommit;\n"
    )
    assert report["fingerprints"][0]["operation"] == "COMMIT"
