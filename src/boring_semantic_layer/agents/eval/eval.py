"""Evaluation runner for BSL agent queries.

Runs all questions from questions.yaml against the flights dataset
and checks for errors. Outputs results as a summary report.

Usage:
    # Run all questions
    python -m boring_semantic_layer.agents.eval.eval

    # Run specific categories
    python -m boring_semantic_layer.agents.eval.eval --category basic

    # Run with specific LLM
    python -m boring_semantic_layer.agents.eval.eval --llm gpt-4o

    # Verbose mode (show full responses)
    python -m boring_semantic_layer.agents.eval.eval -v
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
    category: str
    difficulty: str
    success: bool
    error: str | None = None
    tool_output: str | None = None
    response: str | None = None
    duration_seconds: float = 0.0
    tool_calls: list[dict] = field(default_factory=list)
    # Additional metrics
    num_tool_calls: int = 0
    num_query_model_calls: int = 0
    num_get_model_calls: int = 0
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

    @property
    def total_tool_calls(self) -> int:
        return sum(r.num_tool_calls for r in self.results)

    @property
    def avg_tool_calls_per_question(self) -> float:
        return self.total_tool_calls / self.total if self.total > 0 else 0.0

    @property
    def avg_duration_per_question(self) -> float:
        return self.duration_seconds / self.total if self.total > 0 else 0.0

    @property
    def pass_rate_by_category(self) -> dict[str, float]:
        """Calculate pass rate for each category."""
        category_stats: dict[str, dict[str, int]] = {}
        for r in self.results:
            if r.category not in category_stats:
                category_stats[r.category] = {"total": 0, "passed": 0}
            category_stats[r.category]["total"] += 1
            if r.success:
                category_stats[r.category]["passed"] += 1
        return {
            cat: (stats["passed"] / stats["total"] * 100) if stats["total"] > 0 else 0.0
            for cat, stats in category_stats.items()
        }

    @property
    def pass_rate_by_difficulty(self) -> dict[str, float]:
        """Calculate pass rate for each difficulty level."""
        difficulty_stats: dict[str, dict[str, int]] = {}
        for r in self.results:
            if r.difficulty not in difficulty_stats:
                difficulty_stats[r.difficulty] = {"total": 0, "passed": 0}
            difficulty_stats[r.difficulty]["total"] += 1
            if r.success:
                difficulty_stats[r.difficulty]["passed"] += 1
        return {
            diff: (stats["passed"] / stats["total"] * 100) if stats["total"] > 0 else 0.0
            for diff, stats in difficulty_stats.items()
        }

    def compare_to_baseline(self, baseline: dict) -> dict:
        """Compare current results to a baseline and identify regressions.

        Args:
            baseline: Previous eval results dict (from to_dict())

        Returns:
            Dict with regression analysis
        """
        baseline_results = {r["id"]: r for r in baseline.get("results", [])}
        current_results = {r.question_id: r for r in self.results}

        regressions: list[dict] = []  # Was passing, now failing
        improvements: list[dict] = []  # Was failing, now passing
        new_failures: list[dict] = []  # New questions that failed
        consistent_failures: list[dict] = []  # Still failing

        for qid, result in current_results.items():
            baseline_result = baseline_results.get(qid)

            if baseline_result is None:
                # New question not in baseline
                if not result.success:
                    new_failures.append({"id": qid, "error": result.error})
            elif baseline_result["success"] and not result.success:
                # REGRESSION: was passing, now failing
                regressions.append(
                    {
                        "id": qid,
                        "category": result.category,
                        "difficulty": result.difficulty,
                        "error": result.error,
                    }
                )
            elif not baseline_result["success"] and result.success:
                # IMPROVEMENT: was failing, now passing
                improvements.append(
                    {
                        "id": qid,
                        "category": result.category,
                        "difficulty": result.difficulty,
                    }
                )
            elif not baseline_result["success"] and not result.success:
                # Still failing
                consistent_failures.append({"id": qid})

        # Compare aggregate metrics
        baseline_summary = baseline.get("summary", {})
        baseline_pass_rate = float(baseline_summary.get("pass_rate", "0%").rstrip("%"))

        return {
            "has_regressions": len(regressions) > 0,
            "regression_count": len(regressions),
            "improvement_count": len(improvements),
            "pass_rate_delta": round(self.pass_rate - baseline_pass_rate, 1),
            "baseline_pass_rate": baseline_pass_rate,
            "current_pass_rate": round(self.pass_rate, 1),
            "regressions": regressions,
            "improvements": improvements,
            "new_failures": new_failures,
            "consistent_failures": consistent_failures,
        }

    def to_dict(self) -> dict:
        return {
            "summary": {
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "pass_rate": f"{self.pass_rate:.1f}%",
                "duration_seconds": round(self.duration_seconds, 2),
                "avg_duration_per_question": round(self.avg_duration_per_question, 2),
                "total_tool_calls": self.total_tool_calls,
                "avg_tool_calls_per_question": round(self.avg_tool_calls_per_question, 2),
                "llm_model": self.llm_model,
                "timestamp": self.timestamp,
            },
            "metrics": {
                "pass_rate_by_category": {
                    cat: f"{rate:.1f}%" for cat, rate in self.pass_rate_by_category.items()
                },
                "pass_rate_by_difficulty": {
                    diff: f"{rate:.1f}%" for diff, rate in self.pass_rate_by_difficulty.items()
                },
            },
            "errors": [
                {
                    "id": e.question_id,
                    "question": e.question,
                    "category": e.category,
                    "error": e.error,
                }
                for e in self.errors
            ],
            "results": [
                {
                    "id": r.question_id,
                    "success": r.success,
                    "category": r.category,
                    "difficulty": r.difficulty,
                    "duration": round(r.duration_seconds, 2),
                    "num_tool_calls": r.num_tool_calls,
                    "num_query_model_calls": r.num_query_model_calls,
                    "num_get_model_calls": r.num_get_model_calls,
                    "features_tested": r.features_tested,
                }
                for r in self.results
            ],
        }


def load_questions(
    questions_path: Path | None = None,
    category: str | None = None,
    difficulty: str | None = None,
) -> list[dict]:
    """Load questions from YAML file, optionally filtering by category/difficulty."""
    if questions_path is None:
        questions_path = Path(__file__).parent / "questions.yaml"

    with open(questions_path) as f:
        data = yaml.safe_load(f)

    questions = data.get("questions", [])

    if category:
        questions = [q for q in questions if q.get("category") == category]

    if difficulty:
        questions = [q for q in questions if q.get("difficulty") == difficulty]

    return questions


def run_eval(
    llm_model: str = "gpt-4",
    model_path: str | None = None,
    category: str | None = None,
    difficulty: str | None = None,
    verbose: bool = False,
    max_questions: int | None = None,
) -> EvalSummary:
    """Run evaluation on all questions and return summary.

    Args:
        llm_model: LLM model to use (e.g., gpt-4, gpt-4o, claude-3-5-sonnet-20241022)
        model_path: Path to semantic model YAML (default: examples/flights.yml)
        category: Filter to specific category (basic, time, filter, join, advanced, edge_case)
        difficulty: Filter to specific difficulty (easy, medium, hard)
        verbose: Print full responses
        max_questions: Maximum number of questions to run (for quick testing)

    Returns:
        EvalSummary with all results
    """
    from boring_semantic_layer.agents.backends.langchain import LangChainAgent

    # Resolve model path
    if model_path is None:
        # Navigate from eval.py to project root
        # eval.py is at: src/boring_semantic_layer/agents/eval/eval.py
        # project root is 5 levels up
        current_file = Path(__file__).resolve()
        project_root = current_file.parent.parent.parent.parent.parent
        model_path = str(project_root / "examples" / "flights.yml")
        if not Path(model_path).exists():
            # Try alternative path from working directory
            alt_path = Path.cwd() / "examples" / "flights.yml"
            if alt_path.exists():
                model_path = str(alt_path)

    # Load questions
    questions = load_questions(category=category, difficulty=difficulty)
    if max_questions:
        questions = questions[:max_questions]

    # Initialize agent
    print(f"\n{'=' * 60}")
    print("BSL Agent Evaluation")
    print(f"{'=' * 60}")
    print(f"LLM Model: {llm_model}")
    print(f"Semantic Model: {model_path}")
    print(f"Questions: {len(questions)}")
    if category:
        print(f"Category filter: {category}")
    if difficulty:
        print(f"Difficulty filter: {difficulty}")
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
        category_name = q.get("category", "unknown")
        difficulty_level = q.get("difficulty", "unknown")

        print(f"[{i}/{len(questions)}] {question_id}: {question_text[:60]}...")

        # Track tool calls for this question
        tool_calls: list[dict] = []

        def on_tool_call(name: str, args: dict, _tool_calls: list = tool_calls):
            _tool_calls.append({"name": name, "args": args})
            if verbose:
                print(f"  -> Tool: {name}")

        # Track errors
        errors_collected: list[str] = []

        def on_error(error: str, _errors: list = errors_collected):
            _errors.append(error)

        # Reset conversation history for each question
        agent.reset_history()

        # Run the query
        q_start = time.time()
        try:
            tool_output, response = agent.query(
                question_text,
                on_tool_call=on_tool_call,
                on_error=on_error,
            )
            q_duration = time.time() - q_start

            # Check for errors in output
            has_error = False
            error_msg = None

            # Check for error indicators
            if errors_collected:
                has_error = True
                error_msg = errors_collected[0]
            elif tool_output and "❌" in tool_output:
                has_error = True
                # Extract error message
                error_lines = [
                    line for line in tool_output.split("\n") if "❌" in line or "Error" in line
                ]
                error_msg = error_lines[0] if error_lines else "Unknown error in output"
            elif response and "error" in response.lower() and "no error" not in response.lower():
                # Check if response mentions an error (but not "no error")
                if "could not" in response.lower() or "failed" in response.lower():
                    has_error = True
                    error_msg = f"Response indicates failure: {response[:200]}"

            # Count tool calls by type
            num_query_model = sum(1 for tc in tool_calls if tc["name"] == "query_model")
            num_get_model = sum(1 for tc in tool_calls if tc["name"] == "get_model")

            result = EvalResult(
                question_id=question_id,
                question=question_text,
                category=category_name,
                difficulty=difficulty_level,
                success=not has_error,
                error=error_msg,
                tool_output=tool_output if verbose else None,
                response=response if verbose else None,
                duration_seconds=q_duration,
                tool_calls=tool_calls,
                num_tool_calls=len(tool_calls),
                num_query_model_calls=num_query_model,
                num_get_model_calls=num_get_model,
                features_tested=q.get("features", []),
            )

        except Exception as e:
            q_duration = time.time() - q_start
            result = EvalResult(
                question_id=question_id,
                question=question_text,
                category=category_name,
                difficulty=difficulty_level,
                success=False,
                error=str(e),
                duration_seconds=q_duration,
                tool_calls=tool_calls,
                num_tool_calls=len(tool_calls),
                features_tested=q.get("features", []),
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
                # Truncate error for display
                error_display = (
                    result.error[:100] + "..." if len(result.error) > 100 else result.error
                )
                print(f"     Error: {error_display}")

        if verbose and result.response:
            print(f"  Response: {result.response[:200]}...")

    summary.duration_seconds = time.time() - start_time

    # Print summary
    print(f"\n{'=' * 60}")
    print("EVALUATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total:     {summary.total}")
    print(f"Passed:    {summary.passed}")
    print(f"Failed:    {summary.failed}")
    print(f"Pass Rate: {summary.pass_rate:.1f}%")
    print(f"Duration:  {summary.duration_seconds:.1f}s")
    print(f"Avg Duration/Question: {summary.avg_duration_per_question:.1f}s")

    # Tool call metrics
    print(f"\n{'=' * 60}")
    print("TOOL CALL METRICS")
    print(f"{'=' * 60}")
    print(f"Total Tool Calls:      {summary.total_tool_calls}")
    print(f"Avg Tool Calls/Q:      {summary.avg_tool_calls_per_question:.1f}")

    # Breakdown by category
    print(f"\n{'=' * 60}")
    print("PASS RATE BY CATEGORY")
    print(f"{'=' * 60}")
    for cat, rate in sorted(summary.pass_rate_by_category.items()):
        # Count questions in this category
        cat_count = sum(1 for r in summary.results if r.category == cat)
        cat_passed = sum(1 for r in summary.results if r.category == cat and r.success)
        print(f"  {cat:15} {rate:5.1f}% ({cat_passed}/{cat_count})")

    # Breakdown by difficulty
    print(f"\n{'=' * 60}")
    print("PASS RATE BY DIFFICULTY")
    print(f"{'=' * 60}")
    difficulty_order = ["easy", "medium", "hard"]
    for diff in difficulty_order:
        if diff in summary.pass_rate_by_difficulty:
            rate = summary.pass_rate_by_difficulty[diff]
            diff_count = sum(1 for r in summary.results if r.difficulty == diff)
            diff_passed = sum(1 for r in summary.results if r.difficulty == diff and r.success)
            print(f"  {diff:15} {rate:5.1f}% ({diff_passed}/{diff_count})")

    if summary.errors:
        print(f"\n{'=' * 60}")
        print("FAILED QUESTIONS")
        print(f"{'=' * 60}")
        for e in summary.errors:
            print(f"\n{e.question_id} ({e.category}/{e.difficulty}):")
            print(f"  Q: {e.question}")
            print(f"  Error: {e.error}")

    return summary


def print_regression_report(comparison: dict) -> None:
    """Print a regression comparison report."""
    print(f"\n{'=' * 60}")
    print("REGRESSION ANALYSIS")
    print(f"{'=' * 60}")

    delta = comparison["pass_rate_delta"]
    delta_str = f"+{delta}" if delta > 0 else str(delta)
    print(
        f"Pass Rate: {comparison['baseline_pass_rate']:.1f}% -> {comparison['current_pass_rate']:.1f}% ({delta_str}%)"
    )

    if comparison["has_regressions"]:
        print(f"\n🔴 REGRESSIONS DETECTED: {comparison['regression_count']} question(s) regressed")
        for reg in comparison["regressions"]:
            print(f"  - {reg['id']} ({reg['category']}/{reg['difficulty']})")
            if reg.get("error"):
                error_short = reg["error"][:80] + "..." if len(reg["error"]) > 80 else reg["error"]
                print(f"    Error: {error_short}")
    else:
        print("\n✅ No regressions detected")

    if comparison["improvements"]:
        print(f"\n🟢 IMPROVEMENTS: {comparison['improvement_count']} question(s) now passing")
        for imp in comparison["improvements"]:
            print(f"  - {imp['id']} ({imp['category']}/{imp['difficulty']})")

    if comparison["new_failures"]:
        print(f"\n🟡 NEW FAILURES: {len(comparison['new_failures'])} new question(s) failing")
        for nf in comparison["new_failures"]:
            print(f"  - {nf['id']}")

    if comparison["consistent_failures"]:
        print(
            f"\n⚪ CONSISTENT FAILURES: {len(comparison['consistent_failures'])} question(s) still failing"
        )


def main():
    """CLI entry point for evaluation runner."""
    parser = argparse.ArgumentParser(
        description="Run BSL agent evaluation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run all questions with default LLM (gpt-4)
    python -m boring_semantic_layer.agents.eval.eval

    # Run with specific LLM
    python -m boring_semantic_layer.agents.eval.eval --llm gpt-4o

    # Run only basic questions
    python -m boring_semantic_layer.agents.eval.eval --category basic

    # Run only easy questions
    python -m boring_semantic_layer.agents.eval.eval --difficulty easy

    # Quick test with 3 questions
    python -m boring_semantic_layer.agents.eval.eval --max 3

    # Save results to JSON
    python -m boring_semantic_layer.agents.eval.eval --output results.json

    # Compare against baseline to detect regressions
    python -m boring_semantic_layer.agents.eval.eval --baseline baseline.json

    # CI mode: fail if regressions detected
    python -m boring_semantic_layer.agents.eval.eval --baseline baseline.json --fail-on-regression
        """,
    )
    parser.add_argument(
        "--llm",
        default="gpt-4",
        help="LLM model to use (default: gpt-4)",
    )
    parser.add_argument(
        "--sm",
        "--model",
        dest="model_path",
        help="Path to semantic model YAML (default: examples/flights.yml)",
    )
    parser.add_argument(
        "--category",
        "-c",
        choices=[
            "core",
            "join",
            "advanced",
            "multihop",
            "tool_interaction",
            "output_config",
            "ibis_syntax",
            "resolution_strategy",
            "edge_case",
        ],
        help="Filter to specific category",
    )
    parser.add_argument(
        "--difficulty",
        "-d",
        choices=["easy", "medium", "hard"],
        help="Filter to specific difficulty",
    )
    parser.add_argument(
        "--max",
        type=int,
        dest="max_questions",
        help="Maximum number of questions to run",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print full responses",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Save results to JSON file",
    )
    parser.add_argument(
        "-b",
        "--baseline",
        type=Path,
        help="Compare against baseline JSON file to detect regressions",
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit with error code if regressions detected (for CI)",
    )

    args = parser.parse_args()

    try:
        summary = run_eval(
            llm_model=args.llm,
            model_path=args.model_path,
            category=args.category,
            difficulty=args.difficulty,
            verbose=args.verbose,
            max_questions=args.max_questions,
        )

        # Compare against baseline if provided
        has_regressions = False
        if args.baseline:
            if not args.baseline.exists():
                print(f"\n⚠️  Baseline file not found: {args.baseline}")
            else:
                with open(args.baseline) as f:
                    baseline = json.load(f)
                comparison = summary.compare_to_baseline(baseline)
                has_regressions = comparison["has_regressions"]
                print_regression_report(comparison)

        if args.output:
            with open(args.output, "w") as f:
                json.dump(summary.to_dict(), f, indent=2)
            print(f"\nResults saved to: {args.output}")

        # Determine exit code
        if args.fail_on_regression and has_regressions:
            print("\n❌ Exiting with error due to regressions (--fail-on-regression)")
            sys.exit(2)
        sys.exit(0 if summary.failed == 0 else 1)

    except KeyboardInterrupt:
        print("\n\n👋 Evaluation interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Evaluation failed: {e}")
        raise


if __name__ == "__main__":
    main()
