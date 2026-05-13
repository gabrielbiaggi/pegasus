import csv
import tempfile
import unittest
from pathlib import Path

from journal import TradeJournal


class JournalTest(unittest.TestCase):
    def test_signal_journal_uses_tick_entry_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = TradeJournal(directory)
            journal.log_signal(
                symbol="1HZ100V",
                contract_mode="accumulator",
                entry_epoch=1_700_000_000,
                direction="ACCU",
                score=10,
                stake=1.0,
                dry_run=True,
                metrics={
                    "bb_width_percent": 0.01,
                    "tick_atr_percent": 0.002,
                    "recent_move_percent": 0.003,
                },
            )

            with (Path(directory) / "signals.csv").open(newline="", encoding="utf-8") as handle:
                row = next(csv.DictReader(handle))

        self.assertEqual(row["entry_epoch"], "1700000000")
        self.assertEqual(row["contract_mode"], "accumulator")
        self.assertEqual(row["direction"], "ACCU")
        self.assertNotIn("candle_epoch", row)


if __name__ == "__main__":
    unittest.main()
