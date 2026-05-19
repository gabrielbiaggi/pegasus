import unittest
from unittest.mock import MagicMock, patch

from journal import TradeJournal


class JournalTest(unittest.TestCase):
    def _make_journal(self):
        j = TradeJournal.__new__(TradeJournal)
        j._pg_dsn = "postgresql://fake/db"
        j._schema_ready = True
        return j

    def test_signal_inserts_correct_columns(self) -> None:
        journal = self._make_journal()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cur.__enter__ = MagicMock(return_value=mock_cur)
        mock_cur.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cur

        with patch("journal.psycopg2.connect", return_value=mock_conn):
            journal.log_signal(
                symbol="1HZ100V",
                contract_mode="rise_fall",
                entry_epoch=1_700_000_000,
                direction="CALL",
                score=8,
                stake=50.0,
                dry_run=False,
                metrics={
                    "bb_width_percent": 0.01,
                    "tick_atr_percent": 0.002,
                    "hurst_exponent": 0.42,
                },
            )

        mock_cur.execute.assert_called_once()
        sql, params = mock_cur.execute.call_args[0]
        self.assertIn("INSERT INTO signals", sql)
        self.assertEqual(params[1], "1HZ100V")           # symbol
        self.assertEqual(params[2], "rise_fall")          # contract_mode
        self.assertEqual(params[3], 1_700_000_000)        # entry_epoch
        self.assertEqual(params[4], "CALL")               # direction
        self.assertEqual(params[5], 8)                    # score
        self.assertAlmostEqual(params[8], 0.01)           # bb_width_percent

    def test_trade_inserts_win_loss_result(self) -> None:
        journal = self._make_journal()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cur.__enter__ = MagicMock(return_value=mock_cur)
        mock_cur.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cur

        with patch("journal.psycopg2.connect", return_value=mock_conn):
            journal.log_trade(
                symbol="1HZ25V",
                contract_mode="rise_fall",
                contract_id=123456,
                entry_epoch=1_700_000_001,
                direction="PUT",
                score=7,
                stake=50.0,
                buy_price=50.0,
                profit=47.5,
                exit_epoch=1_700_000_006,
                held_ticks=5,
            )

        mock_cur.execute.assert_called_once()
        sql, params = mock_cur.execute.call_args[0]
        self.assertIn("INSERT INTO trades", sql)
        self.assertEqual(params[14], "WIN")  # result

    def test_no_op_without_pg_dsn(self) -> None:
        journal = TradeJournal(pg_dsn="")
        # Should not raise even though no DB
        journal.log_signal(
            symbol="1HZ25V",
            contract_mode="rise_fall",
            entry_epoch=1_700_000_000,
            direction="CALL",
            score=5,
            stake=50.0,
            dry_run=True,
        )
        journal.log_trade(
            symbol="1HZ25V",
            contract_mode="rise_fall",
            contract_id=1,
            entry_epoch=1_700_000_000,
            direction="CALL",
            score=5,
            stake=50.0,
            buy_price=50.0,
            profit=-50.0,
        )


if __name__ == "__main__":
    unittest.main()
