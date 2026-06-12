"""Run the verification pipeline against the golden dataset and report parity.

Usage (from a Databricks notebook or cluster):

    from address_verify.eval.run_eval import run_eval
    run_eval(model_endpoint="address_verify_realtime", experiment="/Users/me@x/addr_eval")

The harness logs field-level accuracy, AV-status agreement, and a confusion matrix
to MLflow so the SA can diff runs across model, prompt, and reference-data changes.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from ..pipeline import VerifiedAddress
from ..scoring import AVStatus


GOLDEN_CSV = Path(__file__).parent / "golden_dataset.csv"

FIELDS = [
    "primary_number", "street_name", "street_suffix",
    "city", "state", "zipcode",
]


@dataclass
class EvalRow:
    id: str
    raw_input: str
    case: str
    expected: dict[str, str]
    expected_av_status: str


@dataclass
class EvalResult:
    total: int = 0
    field_matches: dict[str, int] = field(default_factory=lambda: {f: 0 for f in FIELDS})
    av_status_matches: int = 0
    av_status_confusion: dict[tuple[str, str], int] = field(default_factory=dict)
    per_case_correct: dict[str, tuple[int, int]] = field(default_factory=dict)

    def record(self, row: EvalRow, actual: VerifiedAddress) -> None:
        self.total += 1
        chosen = actual.chosen
        for f in FIELDS:
            exp = row.expected.get(f) or ""
            got = (getattr(chosen, f, None) or "") if chosen else ""
            if exp.strip().upper() == got.strip().upper():
                self.field_matches[f] += 1
        av_match = actual.av_status.value == row.expected_av_status
        self.av_status_matches += int(av_match)
        key = (row.expected_av_status, actual.av_status.value)
        self.av_status_confusion[key] = self.av_status_confusion.get(key, 0) + 1
        c = self.per_case_correct.setdefault(row.case, (0, 0))
        self.per_case_correct[row.case] = (c[0] + int(av_match), c[1] + 1)

    def summary(self) -> dict:
        n = max(self.total, 1)
        return {
            "rows": self.total,
            "field_accuracy": {f: self.field_matches[f] / n for f in FIELDS},
            "av_status_accuracy": self.av_status_matches / n,
            "by_case": {
                case: {"correct": c, "total": t, "accuracy": c / max(t, 1)}
                for case, (c, t) in self.per_case_correct.items()
            },
            "confusion": {f"{k[0]}->{k[1]}": v for k, v in sorted(self.av_status_confusion.items())},
        }


def load_golden() -> list[EvalRow]:
    rows: list[EvalRow] = []
    with GOLDEN_CSV.open() as f:
        for r in csv.DictReader(f):
            rows.append(EvalRow(
                id=r["id"],
                raw_input=r["raw_input"],
                case=r["case"],
                expected_av_status=r["expected_av_status"],
                expected={
                    "primary_number": r["expected_primary_number"],
                    "street_name": r["expected_street_name"],
                    "street_suffix": r["expected_street_suffix"],
                    "city": r["expected_city"],
                    "state": r["expected_state"],
                    "zipcode": r["expected_zipcode"],
                },
            ))
    return rows


def run_eval(
    verify_fn: Callable[[str], VerifiedAddress],
    *,
    mlflow_experiment: Optional[str] = None,
) -> dict:
    """Run eval. `verify_fn` is a callable that returns a VerifiedAddress for a raw input.
    In a notebook, pass a closure around the MLflow-loaded pyfunc model or an HTTP call
    to the serving endpoint."""
    result = EvalResult()
    for row in load_golden():
        result.record(row, verify_fn(row.raw_input))

    summary = result.summary()

    if mlflow_experiment:
        import mlflow
        mlflow.set_experiment(mlflow_experiment)
        with mlflow.start_run(run_name="address_verify_eval"):
            mlflow.log_metric("av_status_accuracy", summary["av_status_accuracy"])
            for f, acc in summary["field_accuracy"].items():
                mlflow.log_metric(f"field_accuracy_{f}", acc)
            for case, stats in summary["by_case"].items():
                mlflow.log_metric(f"case_acc_{case}", stats["accuracy"])
            mlflow.log_dict(summary, "eval_summary.json")

    return summary
