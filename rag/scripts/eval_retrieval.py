#!/usr/bin/env python3
import argparse
import json
import math
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import sys


def _ensure_project_root_on_path() -> None:
    project_root = Path(__file__).resolve().parents[1]
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


_ensure_project_root_on_path()

from RAG_v1 import build_engine


def _safe_int(value: object) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_int_list(value: object) -> List[int]:
    if value is None:
        return []
    if isinstance(value, list):
        out: List[int] = []
        for v in value:
            parsed = _safe_int(v)
            if parsed is not None:
                out.append(parsed)
        return out
    parsed = _safe_int(value)
    return [parsed] if parsed is not None else []


def _as_str_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    raw = str(value).strip()
    return [raw] if raw else []


def _parse_row_relevance(row: Dict) -> Tuple[Dict[int, float], Dict[str, float]]:
    """
    Supports:
    - expected_ticket_id / expected_ticket_ids
    - expected_conversation_id / expected_conversation_ids
    - optional relevance_judgments with graded relevance, e.g.:
      [{"ticket_id": 123, "relevance": 3}, {"conversation_id": "conv_001", "relevance": 2}]
    """
    ticket_rel: Dict[int, float] = {}
    conv_rel: Dict[str, float] = {}

    for tid in _as_int_list(row.get("expected_ticket_id")) + _as_int_list(row.get("expected_ticket_ids")):
        ticket_rel[tid] = max(ticket_rel.get(tid, 0.0), 1.0)

    for cid in _as_str_list(row.get("expected_conversation_id")) + _as_str_list(
        row.get("expected_conversation_ids")
    ):
        conv_rel[cid] = max(conv_rel.get(cid, 0.0), 1.0)

    judgments = row.get("relevance_judgments")
    if isinstance(judgments, list):
        for item in judgments:
            if not isinstance(item, dict):
                continue
            rel = item.get("relevance", 1.0)
            try:
                rel_score = float(rel)
            except (TypeError, ValueError):
                rel_score = 1.0
            rel_score = max(rel_score, 0.0)

            tid = _safe_int(item.get("ticket_id"))
            if tid is not None:
                ticket_rel[tid] = max(ticket_rel.get(tid, 0.0), rel_score)

            cid = str(item.get("conversation_id", "")).strip()
            if cid:
                conv_rel[cid] = max(conv_rel.get(cid, 0.0), rel_score)

    return ticket_rel, conv_rel


def relevance_score(item: Dict, ticket_rel: Dict[int, float], conv_rel: Dict[str, float]) -> float:
    doc = item["doc"]
    md = doc.metadata or {}
    score = 0.0

    tid = _safe_int(md.get("ticket_id"))
    if tid is not None:
        score = max(score, ticket_rel.get(tid, 0.0))

    cid = str(md.get("conversation_id", "")).strip()
    if cid:
        score = max(score, conv_rel.get(cid, 0.0))

    return score


def precision_at_k(binary_relevance: List[int], k: int) -> float:
    if k <= 0:
        return 0.0
    top = binary_relevance[:k]
    return sum(top) / float(k)


def recall_at_k(binary_relevance: List[int], total_relevant: int, k: int) -> float:
    if total_relevant <= 0:
        return 0.0
    return sum(binary_relevance[:k]) / float(total_relevant)


def mrr(binary_relevance: List[int]) -> float:
    for idx, rel in enumerate(binary_relevance, start=1):
        if rel:
            return 1.0 / float(idx)
    return 0.0


def dcg_at_k(relevance_scores: List[float], k: int) -> float:
    total = 0.0
    for idx, rel in enumerate(relevance_scores[:k], start=1):
        total += (2.0**rel - 1.0) / math.log2(idx + 1.0)
    return total


def ndcg_at_k(relevance_scores: List[float], k: int) -> float:
    if k <= 0:
        return 0.0
    dcg = dcg_at_k(relevance_scores, k)
    idcg = dcg_at_k(sorted(relevance_scores, reverse=True), k)
    return (dcg / idcg) if idcg > 0 else 0.0


def _canonical_result_identity(item: Dict) -> str:
    doc = item["doc"]
    md = doc.metadata or {}
    ticket_id = _safe_int(md.get("ticket_id"))
    if ticket_id is not None:
        return f"ticket:{ticket_id}"
    conversation_id = str(md.get("conversation_id", "")).strip()
    if conversation_id:
        return f"conversation:{conversation_id}"
    chunk_id = str(md.get("chunk_id", "")).strip()
    if chunk_id:
        return f"chunk:{chunk_id}"
    return ""


def _jaccard(set_a: Set[str], set_b: Set[str]) -> float:
    if not set_a and not set_b:
        return 1.0
    union = set_a | set_b
    if not union:
        return 1.0
    inter = set_a & set_b
    return len(inter) / float(len(union))


def evaluate(engine, eval_path: str, out_path: str, *, case_k: int = 8) -> Dict:
    rows = []
    with open(eval_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))

    total = len(rows)
    hit1 = 0
    hit3 = 0
    metric_sums: Dict[str, float] = {
        "mrr": 0.0,
        "precision_at_1": 0.0,
        "precision_at_3": 0.0,
        "precision_at_5": 0.0,
        "precision_at_10": 0.0,
        "recall_at_1": 0.0,
        "recall_at_3": 0.0,
        "recall_at_5": 0.0,
        "recall_at_10": 0.0,
        "ndcg_at_3": 0.0,
        "ndcg_at_5": 0.0,
        "ndcg_at_10": 0.0,
    }
    details = []
    case_groups: Dict[str, List[Dict[str, object]]] = {}

    for row in rows:
        query = row["query"]
        ticket_rel, conv_rel = _parse_row_relevance(row)
        results = engine.retrieve(query)

        graded_relevance = [relevance_score(item, ticket_rel=ticket_rel, conv_rel=conv_rel) for item in results]
        binary_relevance = [1 if score > 0 else 0 for score in graded_relevance]

        top1_match = bool(binary_relevance and binary_relevance[0] == 1)
        top3_match = bool(sum(binary_relevance[:3]) > 0)

        hit1 += int(top1_match)
        hit3 += int(top3_match)
        total_relevant = len(ticket_rel) + len(conv_rel)

        row_metrics = {
            "mrr": mrr(binary_relevance),
            "precision_at_1": precision_at_k(binary_relevance, 1),
            "precision_at_3": precision_at_k(binary_relevance, 3),
            "precision_at_5": precision_at_k(binary_relevance, 5),
            "precision_at_10": precision_at_k(binary_relevance, 10),
            "recall_at_1": recall_at_k(binary_relevance, total_relevant=total_relevant, k=1),
            "recall_at_3": recall_at_k(binary_relevance, total_relevant=total_relevant, k=3),
            "recall_at_5": recall_at_k(binary_relevance, total_relevant=total_relevant, k=5),
            "recall_at_10": recall_at_k(binary_relevance, total_relevant=total_relevant, k=10),
            "ndcg_at_3": ndcg_at_k(graded_relevance, 3),
            "ndcg_at_5": ndcg_at_k(graded_relevance, 5),
            "ndcg_at_10": ndcg_at_k(graded_relevance, 10),
        }
        for key, value in row_metrics.items():
            metric_sums[key] += value

        case_top_identities = [
            identity
            for identity in (
                _canonical_result_identity(item) for item in results[: max(1, int(case_k))]
            )
            if identity
        ]
        case_group = str(row.get("case_group") or "").strip().lower()
        if case_group:
            case_groups.setdefault(case_group, []).append(
                {
                    "query": query,
                    "top_identities": case_top_identities,
                }
            )

        details.append(
            {
                "query": query,
                "expected_ticket_ids": sorted(ticket_rel.keys()),
                "expected_conversation_ids": sorted(conv_rel.keys()),
                "top1_hit": top1_match,
                "top3_hit": top3_match,
                "metrics": row_metrics,
                "top_results": [
                    {
                        "rank": idx,
                        "conversation_id": (item["doc"].metadata or {}).get("conversation_id"),
                        "ticket_id": (item["doc"].metadata or {}).get("ticket_id"),
                        "chunk_id": (item["doc"].metadata or {}).get("chunk_id"),
                        "score": item["rerank_score"],
                        "relevance": graded_relevance[idx - 1] if idx - 1 < len(graded_relevance) else 0.0,
                    }
                    for idx, item in enumerate(results[:5], start=1)
                ],
                "case_top_identities": case_top_identities,
            }
        )

    metrics_avg = {k: (v / total if total else 0.0) for k, v in metric_sums.items()}

    case_pair_scores: List[float] = []
    case_pair_details: List[Dict[str, object]] = []
    for case_group, entries in sorted(case_groups.items()):
        if len(entries) < 2:
            continue
        for left, right in combinations(entries, 2):
            left_set = set(left.get("top_identities") or [])
            right_set = set(right.get("top_identities") or [])
            score = _jaccard(left_set, right_set)
            case_pair_scores.append(score)
            case_pair_details.append(
                {
                    "case_group": case_group,
                    "left_query": left.get("query"),
                    "right_query": right.get("query"),
                    "jaccard": score,
                }
            )
    case_invariance_avg = sum(case_pair_scores) / len(case_pair_scores) if case_pair_scores else None

    summary = {
        "total_queries": total,
        "top1": hit1 / total if total else 0.0,
        "top3": hit3 / total if total else 0.0,
        "metrics": metrics_avg,
        "case_k": int(case_k),
        "case_invariance_at_k": case_invariance_avg,
        "case_invariance_pairs": case_pair_details,
        "details": details,
    }

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/rag.yaml")
    ap.add_argument("--eval-set", required=True)
    ap.add_argument("--out", default="data/reports/retrieval_eval.json")
    ap.add_argument("--case-k", type=int, default=8)
    args = ap.parse_args()

    engine = build_engine(args.config)
    summary = evaluate(engine, args.eval_set, args.out, case_k=max(1, int(args.case_k)))
    print(f"total_queries={summary['total_queries']}")
    print(f"top1={summary['top1']:.4f}")
    print(f"top3={summary['top3']:.4f}")
    print(f"mrr={summary['metrics']['mrr']:.4f}")
    print(f"precision@5={summary['metrics']['precision_at_5']:.4f}")
    print(f"recall@5={summary['metrics']['recall_at_5']:.4f}")
    print(f"ndcg@5={summary['metrics']['ndcg_at_5']:.4f}")
    case_metric = summary.get("case_invariance_at_k")
    if case_metric is None:
        print(f"case_invariance@{summary['case_k']}=n/a")
    else:
        print(f"case_invariance@{summary['case_k']}={float(case_metric):.4f}")
    print(f"report={args.out}")


if __name__ == "__main__":
    main()
