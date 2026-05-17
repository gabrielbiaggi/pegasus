from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

from dotenv import load_dotenv

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore[assignment]

from config import load_config
from shadow_collect import _estimate_accu_barrier_percent


def _classify_future_result(future_max_move_percent: float, barrier_est_percent: float) -> str:
    return "LOSS" if future_max_move_percent >= barrier_est_percent else "WIN"


def _backfill_csv(path: Path, atr_multiplier: float, min_percent: float, max_percent: float) -> int:
    if not path.exists():
        return 0

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    for name in (
        "accu_barrier_est_percent",
        "real_high_barrier",
        "real_low_barrier",
        "real_barrier_percent",
        "barrier_source",
        "future_result_atr_est",
        "future_result_spot_005",
    ):
        if name not in fieldnames:
            fieldnames.append(name)

    updated = 0
    for row in rows:
        try:
            tick_atr_percent = float(row.get("tick_atr_percent", "") or "nan")
            future_max_move_percent = float(row.get("future_max_move_percent", "") or "nan")
        except ValueError:
            continue

        if tick_atr_percent != tick_atr_percent or future_max_move_percent != future_max_move_percent:
            continue

        barrier_est_percent = _estimate_accu_barrier_percent(
            indicator_row={"tick_atr_percent": tick_atr_percent},
            atr_multiplier=atr_multiplier,
            min_percent=min_percent,
            max_percent=max_percent,
        )
        row["accu_barrier_est_percent"] = f"{barrier_est_percent:.8f}"
        row["real_high_barrier"] = row.get("real_high_barrier", "")
        row["real_low_barrier"] = row.get("real_low_barrier", "")
        row["real_barrier_percent"] = row.get("real_barrier_percent", "")
        row["barrier_source"] = row.get("barrier_source") or "atr_estimate"
        row["future_result_atr_est"] = row.get("future_result_atr_est") or row.get("future_result", "")
        row["future_result_spot_005"] = row.get("future_result", "")
        row["future_result"] = _classify_future_result(future_max_move_percent, barrier_est_percent)
        updated += 1

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return updated


def _backfill_postgres(pg_dsn: str, atr_multiplier: float, min_percent: float, max_percent: float) -> int:
    if not pg_dsn or psycopg2 is None:
        return 0

    alter_sql = """
    ALTER TABLE shadow_ticks ADD COLUMN IF NOT EXISTS accu_barrier_est_percent DOUBLE PRECISION;
    ALTER TABLE shadow_ticks ADD COLUMN IF NOT EXISTS real_high_barrier DOUBLE PRECISION;
    ALTER TABLE shadow_ticks ADD COLUMN IF NOT EXISTS real_low_barrier DOUBLE PRECISION;
    ALTER TABLE shadow_ticks ADD COLUMN IF NOT EXISTS real_barrier_percent DOUBLE PRECISION;
    ALTER TABLE shadow_ticks ADD COLUMN IF NOT EXISTS barrier_source TEXT;
    ALTER TABLE shadow_ticks ADD COLUMN IF NOT EXISTS future_result_atr_est TEXT;
    ALTER TABLE shadow_ticks ADD COLUMN IF NOT EXISTS future_result_spot_005 TEXT;
    """
    select_sql = """
    SELECT id, tick_atr_percent, future_max_move_percent, future_result
    FROM shadow_ticks
    ORDER BY id
    """
    update_sql = """
    UPDATE shadow_ticks
       SET accu_barrier_est_percent = %s,
           barrier_source = COALESCE(barrier_source, 'atr_estimate'),
           future_result_atr_est = COALESCE(future_result_atr_est, future_result),
           future_result_spot_005 = COALESCE(future_result_spot_005, %s),
           future_result = %s
     WHERE id = %s
    """

    with psycopg2.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(alter_sql)
            cur.execute(select_sql)
            rows = cur.fetchall()

        batch = []
        for row_id, tick_atr_percent, future_max_move_percent, future_result in rows:
            if tick_atr_percent is None or future_max_move_percent is None:
                continue
            barrier_est_percent = _estimate_accu_barrier_percent(
                indicator_row={"tick_atr_percent": tick_atr_percent},
                atr_multiplier=atr_multiplier,
                min_percent=min_percent,
                max_percent=max_percent,
            )
            next_future_result = _classify_future_result(float(future_max_move_percent), barrier_est_percent)
            batch.append((barrier_est_percent, future_result, next_future_result, row_id))

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, update_sql, batch, page_size=1000)
        conn.commit()

    return len(batch)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Backfill do target economico do ACCU no CSV/PG.")
    parser.add_argument("--csv", type=Path, default=Path("data/shadow_ticks.csv"))
    parser.add_argument("--pg-dsn", type=str, default=os.getenv("PG_DSN", ""))
    args = parser.parse_args()

    config = load_config()
    updated_csv = _backfill_csv(
        path=args.csv,
        atr_multiplier=config.accumulator_shadow_barrier_atr_multiplier,
        min_percent=config.accumulator_shadow_barrier_min_percent,
        max_percent=config.accumulator_shadow_barrier_max_percent,
    )
    updated_pg = _backfill_postgres(
        pg_dsn=args.pg_dsn,
        atr_multiplier=config.accumulator_shadow_barrier_atr_multiplier,
        min_percent=config.accumulator_shadow_barrier_min_percent,
        max_percent=config.accumulator_shadow_barrier_max_percent,
    )
    print(f"CSV atualizado: {updated_csv}")
    print(f"PostgreSQL atualizado: {updated_pg}")


if __name__ == "__main__":
    main()
