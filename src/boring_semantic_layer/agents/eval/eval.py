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
class ToolCallDetail:
    """Details of a single tool call."""

    name: str
    args: dict
    input_tokens: int = 0
    output_tokens: int = 0
    status: str = "success"  # "success" or "error"
    error: str | None = None
    result: str | None = None  # Full tool result content

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "args": self.args,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "status": self.status,
            "error": self.error,
        }


@dataclass
class TranscriptEntry:
    """A single entry in the conversation transcript."""

    role: str  # "user", "assistant", "tool_call", "tool_result", "thinking"
    content: str
    tool_name: str | None = None
    tool_args: dict | None = None
    tokens: dict | None = None

    def to_dict(self) -> dict:
        d = {"role": self.role, "content": self.content}
        if self.tool_name:
            d["tool_name"] = self.tool_name
        if self.tool_args:
            d["tool_args"] = self.tool_args
        if self.tokens:
            d["tokens"] = self.tokens
        return d


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
    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: list[ToolCallDetail] = field(default_factory=list)
    transcript: list[TranscriptEntry] = field(default_factory=list)
    final_response: str = ""

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def transcript_to_dict(self) -> dict:
        """Return full transcript as a dictionary."""
        return {
            "question_id": self.question_id,
            "question": self.question,
            "success": self.success,
            "error": self.error,
            "duration_seconds": round(self.duration_seconds, 2),
            "total_tokens": self.total_tokens,
            "final_response": self.final_response,
            "transcript": [t.to_dict() for t in self.transcript],
        }


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
    total_input_tokens: int = 0
    total_output_tokens: int = 0

    @property
    def pass_rate(self) -> float:
        return (self.passed / self.total * 100) if self.total > 0 else 0.0

    @property
    def total_tokens(self) -> int:
        return self.total_input_tokens + self.total_output_tokens

    @property
    def total_tool_calls(self) -> int:
        return sum(r.num_tool_calls for r in self.results)

    @property
    def tool_call_stats(self) -> dict:
        """Aggregate statistics for tool calls across all questions."""
        all_tool_calls = [tc for r in self.results for tc in r.tool_calls]
        if not all_tool_calls:
            return {"total": 0, "by_tool": {}, "by_status": {}}

        # Count by tool name
        by_tool: dict[str, dict] = {}
        for tc in all_tool_calls:
            if tc.name not in by_tool:
                by_tool[tc.name] = {"count": 0, "success": 0, "error": 0, "tokens": 0}
            by_tool[tc.name]["count"] += 1
            by_tool[tc.name]["tokens"] += tc.total_tokens
            if tc.status == "success":
                by_tool[tc.name]["success"] += 1
            elif tc.status == "error":
                by_tool[tc.name]["error"] += 1

        # Count by status
        by_status = {"success": 0, "error": 0, "unknown": 0}
        for tc in all_tool_calls:
            by_status[tc.status] = by_status.get(tc.status, 0) + 1

        return {
            "total": len(all_tool_calls),
            "by_tool": by_tool,
            "by_status": by_status,
        }

    @property
    def avg_tokens_per_question(self) -> float:
        return self.total_tokens / self.total if self.total > 0 else 0.0

    @property
    def avg_tool_calls_per_question(self) -> float:
        return self.total_tool_calls / self.total if self.total > 0 else 0.0

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
                "total_input_tokens": self.total_input_tokens,
                "total_output_tokens": self.total_output_tokens,
                "total_tokens": self.total_tokens,
                "total_tool_calls": self.total_tool_calls,
                "avg_tokens_per_question": round(self.avg_tokens_per_question),
                "avg_tool_calls_per_question": round(self.avg_tool_calls_per_question, 1),
            },
            "tool_call_stats": self.tool_call_stats,
            "errors": [
                {"id": e.question_id, "question": e.question, "error": e.error} for e in self.errors
            ],
            "results": [
                {
                    "id": r.question_id,
                    "success": r.success,
                    "duration": round(r.duration_seconds, 2),
                    "num_tool_calls": r.num_tool_calls,
                    "input_tokens": r.input_tokens,
                    "output_tokens": r.output_tokens,
                    "total_tokens": r.total_tokens,
                    "tool_calls": [tc.to_dict() for tc in r.tool_calls],
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


def save_transcripts(summary: EvalSummary, eval_dir: Path) -> None:
    """Save individual transcripts for each question to the eval directory."""
    eval_dir.mkdir(parents=True, exist_ok=True)

    # Create a timestamped run directory
    timestamp = summary.timestamp.replace(":", "-").replace(".", "-")
    run_dir = eval_dir / f"run_{timestamp}"
    run_dir.mkdir(exist_ok=True)

    # Save individual transcripts
    for result in summary.results:
        transcript_file = run_dir / f"{result.question_id}.json"
        with open(transcript_file, "w") as f:
            json.dump(result.transcript_to_dict(), f, indent=2)

    # Save summary
    summary_file = run_dir / "summary.json"
    with open(summary_file, "w") as f:
        json.dump(summary.to_dict(), f, indent=2)

    print(f"\nTranscripts saved to: {run_dir}")
    return run_dir


def run_eval(
    llm_model: str = "anthropic:claude-opus-4-20250514",
    model_path: str | None = None,
    verbose: bool = False,
    max_questions: int | None = None,
    question_ids: list[str] | None = None,
    save_transcripts_dir: Path | None = None,
) -> EvalSummary:
    """Run evaluation on all questions and return summary.

    Args:
        llm_model: LLM model to use
        model_path: Path to semantic model YAML
        verbose: Show verbose output
        max_questions: Maximum number of questions to run
        question_ids: List of specific question IDs to run (e.g., ["core_001", "join_002"])
        save_transcripts_dir: Directory to save transcripts (default: .eval/)
    """
    from boring_semantic_layer.agents.backends import LangGraphBackend

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

    # Filter by specific question IDs if provided
    if question_ids:
        questions = [q for q in questions if q.get("id") in question_ids]
        if not questions:
            print(f"Warning: No questions found matching IDs: {question_ids}")
            print("Available IDs:", [q.get("id") for q in load_questions()])
    if max_questions:
        questions = questions[:max_questions]

    print(f"\n{'=' * 60}")
    print(f"BSL Agent Evaluation - {llm_model}")
    print(f"Questions: {len(questions)}")
    print(f"{'=' * 60}\n")

    agent = LangGraphBackend(
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

        print(f"[{i}/{len(questions)}] {question_id}: {question_text}")

        tool_calls_detail: list[ToolCallDetail] = []
        pending_tool_calls: list[dict] = []  # Track calls waiting for results
        tokens_collected: list[dict] = []
        transcript: list[TranscriptEntry] = []

        # Add user question to transcript
        transcript.append(TranscriptEntry(role="user", content=question_text))

        # Import CLI display function for verbose mode
        if verbose:
            from boring_semantic_layer.agents.chats.cli import display_tool_call

        def on_tool_call(
            name: str,
            args: dict,
            tokens: dict | None = None,
            _pending: list = pending_tool_calls,
            _tokens: list = tokens_collected,
            _transcript: list = transcript,
        ):
            input_toks = tokens.get("input_tokens", 0) if tokens else 0
            output_toks = tokens.get("output_tokens", 0) if tokens else 0
            _pending.append(
                {
                    "name": name,
                    "args": args,
                    "input_tokens": input_toks,
                    "output_tokens": output_toks,
                }
            )
            if tokens:
                _tokens.append(tokens)
            # Add to transcript
            _transcript.append(
                TranscriptEntry(
                    role="tool_call",
                    content=f"Calling {name}",
                    tool_name=name,
                    tool_args=args,
                    tokens=tokens,
                )
            )
            # Show tool calls like CLI does
            if verbose:
                display_tool_call(name, args, None, tokens)

        def on_tool_result(
            tool_call_id: str,
            status: str,
            error: str | None,
            content: str | None = None,
            _pending: list = pending_tool_calls,
            _details: list = tool_calls_detail,
            _transcript: list = transcript,
        ):
            # Match result with pending call (FIFO order)
            if _pending:
                call_info = _pending.pop(0)
                _details.append(
                    ToolCallDetail(
                        name=call_info["name"],
                        args=call_info["args"],
                        input_tokens=call_info["input_tokens"],
                        output_tokens=call_info["output_tokens"],
                        status=status,
                        error=error,
                        result=content,
                    )
                )
                # Add to transcript
                _transcript.append(
                    TranscriptEntry(
                        role="tool_result",
                        content=content or "",
                        tool_name=call_info["name"],
                    )
                )
                # Tool results already printed by the tool itself (table/chart)
                # No need to print again here

        def on_thinking(text: str, _transcript: list = transcript):
            _transcript.append(TranscriptEntry(role="thinking", content=text))
            # Thinking already shown in CLI output, no need to duplicate

        errors_collected: list[str] = []

        def on_error(error: str, _errors: list = errors_collected):
            _errors.append(error)

        agent.reset_history()
        q_start = time.time()

        try:
            tool_output, response = agent.query(
                question_text,
                on_tool_call=on_tool_call,
                on_error=on_error,
                on_tool_result=on_tool_result,
                on_thinking=on_thinking,
            )
            q_duration = time.time() - q_start

            # Add final response to transcript
            if response:
                transcript.append(TranscriptEntry(role="assistant", content=response))

            has_error = False
            error_msg = None

            # Check if this question tests error recovery
            features = q.get("features", [])
            tests_recovery = "recovery" in features or "error_handling" in features

            if errors_collected:
                has_error = True
                error_msg = errors_collected[0]
            else:
                # Check tool_calls for any errors reported via on_tool_result
                # (ToolException errors have status="error" from LangChain)
                for tc in tool_calls_detail:
                    if tc.status == "error" and tc.error:
                        has_error = True
                        error_msg = tc.error
                        break

            # For recovery tests: success if there was an error but agent recovered
            # (i.e., made a successful tool call after the error)
            if tests_recovery and has_error:
                error_indices = [
                    i for i, tc in enumerate(tool_calls_detail) if tc.status == "error"
                ]
                success_indices = [
                    i for i, tc in enumerate(tool_calls_detail) if tc.status == "success"
                ]
                # Check if any success came after an error
                if error_indices and success_indices:
                    last_error = max(error_indices)
                    has_recovery = any(s > last_error for s in success_indices)
                    if has_recovery:
                        has_error = False  # Agent recovered successfully
                        error_msg = None

            # Sum up token usage from all LLM calls
            q_input_tokens = sum(t.get("input_tokens", 0) for t in tokens_collected)
            q_output_tokens = sum(t.get("output_tokens", 0) for t in tokens_collected)

            # Add any remaining pending tool calls (those without results yet)
            for call_info in pending_tool_calls:
                tool_calls_detail.append(
                    ToolCallDetail(
                        name=call_info["name"],
                        args=call_info["args"],
                        input_tokens=call_info["input_tokens"],
                        output_tokens=call_info["output_tokens"],
                        status="unknown",
                    )
                )

            result = EvalResult(
                question_id=question_id,
                question=question_text,
                success=not has_error,
                error=error_msg,
                duration_seconds=q_duration,
                num_tool_calls=len(tool_calls_detail),
                features_tested=q.get("features", []),
                input_tokens=q_input_tokens,
                output_tokens=q_output_tokens,
                tool_calls=tool_calls_detail,
                transcript=transcript,
                final_response=response,
            )

        except Exception as e:
            q_duration = time.time() - q_start
            # Sum up token usage even for failed queries
            q_input_tokens = sum(t.get("input_tokens", 0) for t in tokens_collected)
            q_output_tokens = sum(t.get("output_tokens", 0) for t in tokens_collected)

            # Add error to transcript
            transcript.append(TranscriptEntry(role="error", content=str(e)))

            # Add any remaining pending tool calls
            for call_info in pending_tool_calls:
                tool_calls_detail.append(
                    ToolCallDetail(
                        name=call_info["name"],
                        args=call_info["args"],
                        input_tokens=call_info["input_tokens"],
                        output_tokens=call_info["output_tokens"],
                        status="unknown",
                    )
                )

            result = EvalResult(
                question_id=question_id,
                question=question_text,
                success=False,
                error=str(e),
                duration_seconds=q_duration,
                num_tool_calls=len(tool_calls_detail),
                input_tokens=q_input_tokens,
                output_tokens=q_output_tokens,
                tool_calls=tool_calls_detail,
                transcript=transcript,
                final_response="",
            )

        summary.results.append(result)
        summary.total_input_tokens += result.input_tokens
        summary.total_output_tokens += result.output_tokens

        if result.success:
            summary.passed += 1
            print(f"  ✅ PASS ({q_duration:.1f}s, {result.total_tokens:,} tokens)")
        else:
            summary.failed += 1
            summary.errors.append(result)
            print(f"  ❌ FAIL ({q_duration:.1f}s, {result.total_tokens:,} tokens)")
            if result.error:
                error_display = (
                    result.error[:100] + "..." if len(result.error) > 100 else result.error
                )
                print(f"     Error: {error_display}")

        # Show chat transcript for failures (verbose already printed in real-time)
        if not result.success and not verbose:
            print("\n  --- Chat Transcript ---")
            for entry in transcript:
                if entry.role == "user":
                    print(f"  👤 USER: {entry.content}")
                elif entry.role == "assistant":
                    print(f"  🤖 ASSISTANT: {entry.content}")
                elif entry.role == "thinking":
                    print(f"  💭 THINKING: {entry.content[:200]}...")
                elif entry.role == "tool_call":
                    args_str = json.dumps(entry.tool_args, indent=2) if entry.tool_args else "{}"
                    if len(args_str) > 300:
                        args_str = args_str[:300] + "..."
                    print(f"  🔧 TOOL CALL: {entry.tool_name}")
                    print(f"     Args: {args_str}")
                elif entry.role == "tool_result":
                    content = entry.content or ""
                    if len(content) > 500:
                        content = content[:500] + "..."
                    print(f"  📋 TOOL RESULT ({entry.tool_name}):")
                    print(f"     {content}")
                elif entry.role == "error":
                    print(f"  ❌ ERROR: {entry.content}")
            print("  -------------------------\n")

    summary.duration_seconds = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"SUMMARY: {summary.passed}/{summary.total} passed ({summary.pass_rate:.1f}%)")
    print(f"Duration: {summary.duration_seconds:.1f}s")
    print(f"Tokens: {summary.total_tokens:,} (avg {summary.avg_tokens_per_question:,.0f}/question)")
    print(
        f"Tool calls: {summary.total_tool_calls} (avg {summary.avg_tool_calls_per_question:.1f}/question)"
    )

    # Show tool call breakdown
    stats = summary.tool_call_stats
    if stats["total"] > 0:
        print("\nTool breakdown:")
        for tool_name, tool_stats in stats["by_tool"].items():
            success_rate = (
                (tool_stats["success"] / tool_stats["count"] * 100)
                if tool_stats["count"] > 0
                else 0
            )
            print(
                f"  {tool_name}: {tool_stats['count']} calls, {success_rate:.0f}% success, {tool_stats['tokens']:,} tokens"
            )

    print(f"{'=' * 60}")

    if summary.errors:
        print("\nFAILED:")
        for e in summary.errors:
            print(f"  {e.question_id}: {e.error}")

    # Save transcripts to .eval/ folder
    if save_transcripts_dir is None:
        save_transcripts_dir = Path.cwd() / ".eval"
    save_transcripts(summary, save_transcripts_dir)

    return summary


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Run BSL agent evaluation")
    parser.add_argument(
        "--llm",
        default="gpt-4",
        help="LLM model to use. Format: [provider:]model (e.g., gpt-4o, openai:gpt-4o, anthropic:claude-sonnet-4-20250514)",
    )
    parser.add_argument("--sm", dest="model_path", help="Path to semantic model YAML")
    parser.add_argument("--max", type=int, dest="max_questions", help="Max questions to run")
    parser.add_argument(
        "-q",
        "--question",
        dest="question_ids",
        action="append",
        help="Run specific question ID(s). Can be used multiple times (e.g., -q core_001 -q join_002)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-o", "--output", type=Path, help="Save results to JSON")
    parser.add_argument("-b", "--baseline", type=Path, help="Baseline JSON for regression check")
    parser.add_argument("--fail-on-regression", action="store_true", help="Exit 2 if regressions")
    parser.add_argument(
        "--list-questions", action="store_true", help="List all available question IDs and exit"
    )

    args = parser.parse_args()

    # List questions and exit if requested
    if args.list_questions:
        questions = load_questions()
        print("Available question IDs:")
        for q in questions:
            print(f"  {q.get('id')}: {q.get('question', '')[:60]}...")
        sys.exit(0)

    try:
        summary = run_eval(
            llm_model=args.llm,
            model_path=args.model_path,
            verbose=args.verbose,
            max_questions=args.max_questions,
            question_ids=args.question_ids,
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
