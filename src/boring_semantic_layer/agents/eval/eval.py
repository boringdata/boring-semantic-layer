"""Evaluation runner for BSL agent queries.

Usage:
    python -m boring_semantic_layer.agents.eval.eval
    python -m boring_semantic_layer.agents.eval.eval --llm gpt-4o
    python -m boring_semantic_layer.agents.eval.eval --max 3 -v
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


@dataclass
class EvalResult:
    """Result of evaluating a single question."""

    question_id: str
    question: str
    success: bool
    error: str | None = None
    duration_seconds: float = 0.0
    num_tool_calls: int = 0
    features_tested: list[str] = field(default_factory=list)


@dataclass
class EvalSummary:
    """Summary of all evaluation results."""

    total: int = 0
    passed: int = 0
    failed: int = 0
    errors: list[EvalResult] = field(default_factory=list)
    results: list[EvalResult] = field(default_factory=list)
    duration_seconds: float = 0.0
    llm_model: str = ""
    timestamp: str = ""

    @property
    def pass_rate(self) -> float:
        return (self.passed / self.total * 100) if self.total > 0 else 0.0

    def compare_to_baseline(self, baseline: dict) -> dict:
        """Compare current results to a baseline and identify regressions."""
        baseline_results = {r["id"]: r for r in baseline.get("results", [])}
        current_results = {r.question_id: r for r in self.results}

        regressions = []
        improvements = []

        for qid, result in current_results.items():
            baseline_result = baseline_results.get(qid)
            if baseline_result is None:
                continue
            if baseline_result["success"] and not result.success:
                regressions.append({"id": qid, "error": result.error})
            elif not baseline_result["success"] and result.success:
                improvements.append({"id": qid})

        baseline_pass_rate = float(baseline.get("summary", {}).get("pass_rate", "0%").rstrip("%"))

        return {
            "has_regressions": len(regressions) > 0,
            "regression_count": len(regressions),
            "improvement_count": len(improvements),
            "pass_rate_delta": round(self.pass_rate - baseline_pass_rate, 1),
            "regressions": regressions,
            "improvements": improvements,
        }

    def to_dict(self) -> dict:
        return {
            "summary": {
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "pass_rate": f"{self.pass_rate:.1f}%",
                "duration_seconds": round(self.duration_seconds, 2),
                "llm_model": self.llm_model,
                "timestamp": self.timestamp,
            },
            "errors": [
                {"id": e.question_id, "question": e.question, "error": e.error} for e in self.errors
            ],
            "results": [
                {
                    "id": r.question_id,
                    "success": r.success,
                    "duration": round(r.duration_seconds, 2),
                    "num_tool_calls": r.num_tool_calls,
                }
                for r in self.results
            ],
        }


def load_questions(questions_path: Path | None = None) -> list[dict]:
    """Load questions from YAML file."""
    if questions_path is None:
        questions_path = Path(__file__).parent / "questions.yaml"

    with open(questions_path) as f:
        data = yaml.safe_load(f)

    return data.get("questions", [])


def run_eval(
    llm_model: str = "gpt-4",
    model_path: str | None = None,
    verbose: bool = False,
    max_questions: int | None = None,
) -> EvalSummary:
    """Run evaluation on all questions and return summary."""
    from boring_semantic_layer.agents.backends.langchain import LangChainAgent

    # Resolve model path
    if model_path is None:
        current_file = Path(__file__).resolve()
        project_root = current_file.parent.parent.parent.parent.parent
        model_path = str(project_root / "examples" / "flights.yml")
        if not Path(model_path).exists():
            alt_path = Path.cwd() / "examples" / "flights.yml"
            if alt_path.exists():
                model_path = str(alt_path)

    questions = load_questions()
    if max_questions:
        questions = questions[:max_questions]

    print(f"\n{'=' * 60}")
    print(f"BSL Agent Evaluation - {llm_model}")
    print(f"Questions: {len(questions)}")
    print(f"{'=' * 60}\n")

    agent = LangChainAgent(
        model_path=Path(model_path),
        llm_model=llm_model,
        chart_backend="plotext",
    )

    summary = EvalSummary(
        total=len(questions),
        llm_model=llm_model,
        timestamp=datetime.now().isoformat(),
    )

    start_time = time.time()

    for i, q in enumerate(questions, 1):
        question_id = q.get("id", f"q{i}")
        question_text = q.get("question", "")

        print(f"[{i}/{len(questions)}] {question_id}: {question_text[:60]}...")

        tool_calls: list[dict] = []

        def on_tool_call(name: str, args: dict, _tc: list = tool_calls):
            _tc.append({"name": name, "args": args})
            if verbose:
                print(f"  -> Tool: {name}")

        errors_collected: list[str] = []

        def on_error(error: str, _errors: list = errors_collected):
            _errors.append(error)

        agent.reset_history()
        q_start = time.time()

        try:
            tool_output, response = agent.query(
                question_text, on_tool_call=on_tool_call, on_error=on_error
            )
            q_duration = time.time() - q_start

            has_error = False
            error_msg = None

            if errors_collected:
                has_error = True
                error_msg = errors_collected[0]
            elif tool_output and "❌" in tool_output:
                has_error = True
                error_lines = [
                    line for line in tool_output.split("\n") if "❌" in line or "Error" in line
                ]
                error_msg = error_lines[0] if error_lines else "Unknown error"

            result = EvalResult(
                question_id=question_id,
                question=question_text,
                success=not has_error,
                error=error_msg,
                duration_seconds=q_duration,
                num_tool_calls=len(tool_calls),
                features_tested=q.get("features", []),
            )

        except Exception as e:
            q_duration = time.time() - q_start
            result = EvalResult(
                question_id=question_id,
                question=question_text,
                success=False,
                error=str(e),
                duration_seconds=q_duration,
                num_tool_calls=len(tool_calls),
            )

        summary.results.append(result)

        if result.success:
            summary.passed += 1
            print(f"  ✅ PASS ({q_duration:.1f}s)")
        else:
            summary.failed += 1
            summary.errors.append(result)
            print(f"  ❌ FAIL ({q_duration:.1f}s)")
            if result.error:
                error_display = (
                    result.error[:100] + "..." if len(result.error) > 100 else result.error
                )
                print(f"     Error: {error_display}")

    summary.duration_seconds = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"SUMMARY: {summary.passed}/{summary.total} passed ({summary.pass_rate:.1f}%)")
    print(f"Duration: {summary.duration_seconds:.1f}s")
    print(f"{'=' * 60}")

    if summary.errors:
        print("\nFAILED:")
        for e in summary.errors:
            print(f"  {e.question_id}: {e.error}")

    return summary


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Run BSL agent evaluation")
    parser.add_argument("--llm", default="gpt-4", help="LLM model to use")
    parser.add_argument("--sm", dest="model_path", help="Path to semantic model YAML")
    parser.add_argument("--max", type=int, dest="max_questions", help="Max questions to run")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-o", "--output", type=Path, help="Save results to JSON")
    parser.add_argument("-b", "--baseline", type=Path, help="Baseline JSON for regression check")
    parser.add_argument("--fail-on-regression", action="store_true", help="Exit 2 if regressions")

    args = parser.parse_args()

    try:
        summary = run_eval(
            llm_model=args.llm,
            model_path=args.model_path,
            verbose=args.verbose,
            max_questions=args.max_questions,
        )

        has_regressions = False
        if args.baseline and args.baseline.exists():
            with open(args.baseline) as f:
                baseline = json.load(f)
            comparison = summary.compare_to_baseline(baseline)
            has_regressions = comparison["has_regressions"]
            if has_regressions:
                print(f"\n🔴 REGRESSIONS: {comparison['regression_count']}")
                for reg in comparison["regressions"]:
                    print(f"  - {reg['id']}")

        if args.output:
            with open(args.output, "w") as f:
                json.dump(summary.to_dict(), f, indent=2)
            print(f"\nSaved to: {args.output}")

        if args.fail_on_regression and has_regressions:
            sys.exit(2)
        sys.exit(0 if summary.failed == 0 else 1)

    except KeyboardInterrupt:
        print("\n👋 Interrupted")
        sys.exit(130)


if __name__ == "__main__":
    main()
