
import sys
import platform
import duckdb

print("EgressOpt Environment Verification")
print("--------------------------------")
print("Python:", sys.version.split()[0])
print("Architecture:", platform.machine())
print("DuckDB:", duckdb.__version__)

con = duckdb.connect(":memory:")

result = con.execute("""
    SELECT
        'EgressOpt' AS project,
        100 AS orders,
        25 AS customers,
        100 * 25 AS test_value
""").fetchdf()

print(result.to_string(index=False))
con.close()

print("STATUS: SUCCESS")
