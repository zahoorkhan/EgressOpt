#!/usr/bin/env python3
"""Offline sanity tests for TPC-H setup entry point (no DuckDB extension/download)."""
from __future__ import annotations
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "setup_tpch_data.py"


class TpchSetupCliTests(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), *args], cwd=ROOT,
                              text=True, capture_output=True, timeout=15)

    def test_sf1_dry_run(self):
        r = self.run_cli("--scale", "1", "--dry-run")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("CALL dbgen(sf = 1)", r.stdout)
        self.assertIn("parquet_sf1", r.stdout)
        self.assertIn("SQL files are never changed", r.stdout)

    def test_sf10_dry_run(self):
        r = self.run_cli("--scale", "10", "--dry-run")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("-v -s 10", r.stdout)
        self.assertIn("parquet_sf10", r.stdout)
        self.assertIn("raw_sf10", r.stdout)

    def test_scale_constraint(self):
        r = self.run_cli("--scale", "3", "--dry-run")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("invalid choice", r.stderr)

    def test_dbgen_option_requires_sf10(self):
        r = self.run_cli("--scale", "1", "--dbgen-dir", "/tmp/unused", "--dry-run")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("only to --scale 10", r.stderr)

    def test_check_and_dry_run_exclusive(self):
        r = self.run_cli("--scale", "10", "--check", "--dry-run")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("mutually exclusive", r.stderr)

    def test_no_generation_on_help(self):
        r = self.run_cli("--help")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("No cloud calls", r.stdout)


if __name__ == "__main__":
    unittest.main()
