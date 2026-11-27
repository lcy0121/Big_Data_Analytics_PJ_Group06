import unittest

from ghawtrace.p05.score_runs import FIELDS, score_partition


def run(run_id, when, seconds=10., attempts=1, status="completed", conclusion="success", repo=1):
    return (repo, "exact.yml", when, run_id, "01", status, conclusion, seconds, attempts)


def scored(rows):
    ordered = sorted(rows, key=lambda r: (r[0], r[1], r[2] or "", r[3]))
    fields = [name for name, _ in FIELDS]
    return {values[0]: dict(zip(fields, values, strict=True)) for values in score_partition(ordered)}


class RunScores(unittest.TestCase):
    def test_ties_strictly_prior_visibility_and_future_invariance(self):
        seed = [run(i, "02") for i in range(1, 6)]
        tied = [run(6, "03.000001", 11., 2), run(7, "03.000001", 200.)]
        initial = scored(seed + tied)
        later = scored(seed + tied + [run(8, "03.000002"), run(9, "04", 100000.)])
        self.assertEqual(initial, {k: later[k] for k in initial})
        self.assertTrue(all(initial[i]["n_history"] == 0 for i in range(1, 6)))
        for i in (6, 7):
            self.assertEqual(initial[i]["n_history"], 5)
            self.assertEqual(initial[i]["mad_seconds"], 0)
        self.assertEqual(later[8]["n_history"], 7)
        self.assertEqual(initial[6]["elapsed_score"], 0.5)
        self.assertAlmostEqual(initial[6]["retry_score"], 5 / 6)
        self.assertEqual(initial[6]["reason"], "unusual_retries")
        self.assertEqual(initial[7]["reason"], "longer_elapsed")

    def test_window_cap_and_equal_time_tiebreak(self):
        seed = [run(i, "02", float(i)) for i in range(1, 71)]
        rows = scored(seed + [run(71, "03", 1.)])
        self.assertEqual(rows[71]["n_history"], 64)
        self.assertEqual(rows[71]["median_seconds"], 38.5)
        self.assertEqual(rows[71]["mad_seconds"], 16)
        self.assertAlmostEqual(rows[71]["elapsed_score"], 37.5 / (37.5 + 1.4826 * 16))
        self.assertEqual(rows[71]["reason"], "shorter_elapsed")

    def test_eligibility_and_workflow_isolation(self):
        rows = scored([run(1, "02", conclusion="skipped"), run(2, "02", status="queued"),
                       run(3, "00"), run(4, None), run(5, "02", conclusion=None),
                       run(6, "03", conclusion="failure"), run(7, "04"), run(8, "05", repo=2)])
        expected = ["skipped", "not_completed", "invalid_update_time", "invalid_update_time", "missing_conclusion"]
        self.assertEqual([rows[i]["reason"] for i in range(1, 6)], expected)
        self.assertIsNone(rows[2]["score_at_utc"])
        self.assertEqual([rows[i]["n_history"] for i in (6, 7, 8)], [0, 1, 0])
        self.assertIsNone(rows[7]["score"])

    def test_component_support_partial_and_zero(self):
        seed = [run(i, "02", attempts=None) for i in range(1, 6)]
        rows = scored(seed + [run(6, "03", 10.), run(7, "03", None, 0)])
        self.assertEqual(rows[6]["score_status"], "partial")
        self.assertEqual(rows[6]["score"], 0)
        self.assertEqual(rows[6]["n_attempts"], 0)
        self.assertIsNone(rows[6]["retry_score"])
        self.assertEqual(rows[7]["reason"], "missing_metrics")
        retry_only = scored([run(i, "02", None) for i in range(1, 6)] + [run(6, "03", None, 3)])
        self.assertIsNone(retry_only[6]["elapsed_score"])
        self.assertEqual(retry_only[6]["tail_count"], 0)
        self.assertEqual(retry_only[6]["score_status"], "partial")
        self.assertAlmostEqual(retry_only[6]["score"], 5 / 6)
