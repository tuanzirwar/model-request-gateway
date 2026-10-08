"""汇总MySQL FILE慢日志；公开报告只保留指纹和数值，不导出SQL文本。"""

import argparse
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path

METRICS = re.compile(
    r"^# Query_time: ([\d.]+)\s+Lock_time: ([\d.]+)\s+"
    r"Rows_sent: (\d+)\s+Rows_examined: (\d+)",
    re.MULTILINE,
)


def fingerprint(sql):
    # 字面量和注释只参与内部归一化，输出不包含任何SQL片段或用户标识。
    sql = re.sub(r"'(?:\\.|''|[^'\\])*'|\"(?:\\.|\"\"|[^\"\\])*\"", "?", sql)
    sql = re.sub(r"/\*.*?\*/|--[^\n]*|#[^\n]*", " ", sql, flags=re.S)
    sql = re.sub(r"\b\d+(?:\.\d+)?\b", "?", sql)
    sql = re.sub(r"\s+", " ", sql).strip().lower()
    sql = re.sub(r"\?(?:\s*,\s*\?)+", "?list", sql)
    operation = sql.split(" ", 1)[0].rstrip(";").upper()
    if operation not in {"SELECT", "INSERT", "UPDATE", "DELETE", "COMMIT", "ROLLBACK"}:
        operation = "OTHER"
    return hashlib.sha256(sql.encode()).hexdigest()[:20], operation


def summarize(raw):
    groups = defaultdict(list)
    matches = list(METRICS.finditer(raw))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(raw)
        lines = raw[match.end() : end].splitlines()
        sql = "\n".join(
            line for line in lines if not line.startswith(("#", "SET timestamp=", "use ", "Time "))
        ).strip()
        if not sql.endswith(";"):
            continue  # 正在写入的尾部不能算完成记录。
        digest, operation = fingerprint(sql)
        groups[(digest, operation)].append(tuple(map(float, match.groups())))
    rows = []
    for (digest, operation), samples in groups.items():
        times = sorted(row[0] * 1000 for row in samples)
        rows.append(
            {
                "fingerprint": digest,
                "operation": operation,
                "count": len(samples),
                "total_ms": round(sum(times), 3),
                "mean_ms": round(sum(times) / len(times), 3),
                "p95_ms": round(times[int((len(times) - 1) * 0.95)], 3),
                "max_ms": round(times[-1], 3),
                "lock_total_ms": round(sum(row[1] for row in samples) * 1000, 3),
                "rows_sent": int(sum(row[2] for row in samples)),
                "rows_examined": int(sum(row[3] for row in samples)),
            }
        )
    operations = {}
    for row in rows:
        item = operations.setdefault(
            row["operation"], {"count": 0, "total_ms": 0, "rows_examined": 0}
        )
        for field in item:
            item[field] += row[field]
    for item in operations.values():
        item["total_ms"] = round(item["total_ms"], 3)
    return {
        "logged_statements": sum(row["count"] for row in rows),
        "operations": operations,
        "fingerprints": sorted(rows, key=lambda row: row["total_ms"], reverse=True),
        "limitations": [
            "threshold-filtered log, not all SQL",
            "SQL text intentionally omitted",
            "batch cardinality may produce separate fingerprints",
        ],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path, default=Path("reports/mysql-slow-summary.json"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    output = args.output.resolve()
    if not output.is_relative_to(root / "reports"):
        parser.error("报告必须位于reports目录")
    report = summarize(args.input.read_text("utf-8", errors="replace"))
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), "utf-8")
    print(
        json.dumps(
            {
                "logged_statements": report["logged_statements"],
                "fingerprints": len(report["fingerprints"]),
            }
        )
    )


if __name__ == "__main__":
    main()
