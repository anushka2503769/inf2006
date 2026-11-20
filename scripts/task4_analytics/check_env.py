"""
scripts/task4_analytics/check_env.py  --  Task 4 (Analytics & Visualisation)

Run this once on the EMR primary node BEFORE q1 to q4 and make_figures.py:

    python3 scripts/task4_analytics/check_env.py

If something is missing, one command installs everything (pip with pinned versions):

    python3 scripts/task4_analytics/check_env.py --install

q1 to q4 call toPandas() on small results, and make_figures.py draws the charts, so the
node's Python needs numpy, pandas and matplotlib. The EMR 7.10.0 image (Python 3.9) has
no numpy and its pandas and matplotlib are missing some dependencies. This script
imports each package, draws a tiny test figure, prints the versions and, if anything is
missing, the exact pip line to run. Exit status is 1 when something is missing.

numpy stays pinned at 1.26.4 because pandas 2.2.3 and matplotlib 3.9.4 on Python 3.9
are built against numpy 1.x. python-dateutil stays below 3 because the AWS command-line
tools on the node (botocore) do not accept version 3. pip installs the other dependencies
(tzdata, pillow, contourpy and so on) by itself. Nothing here touches Spark.
"""
import importlib
import platform
import subprocess
import sys

# What - install asks pip for. pip adds the dependencies of these four by itself.
INSTALL = ["numpy==1.26.4", "pandas==2.2.3", "matplotlib==3.9.4", "python-dateutil>=2.8.2,<=2.9.0"]

# (import name, pip requirement used if it is missing)
NEEDED = [
    ("numpy", "numpy==1.26.4"),
    ("dateutil", "python-dateutil>=2.8.2"),
    ("pytz", "pytz>=2020.1"),
    ("tzdata", "tzdata>=2022.7"),
    ("pandas", "pandas==2.2.3"),
    ("contourpy", "contourpy>=1.0.1"),
    ("cycler", "cycler>=0.10"),
    ("fontTools", "fonttools>=4.22.0"),
    ("kiwisolver", "kiwisolver>=1.3.1"),
    ("packaging", "packaging>=20.0"),
    ("PIL", "pillow>=8"),
    ("pyparsing", "pyparsing>=2.3.1"),
    ("importlib_resources", "importlib-resources>=3.2.0"),   # only needed below Python 3.10
    ("matplotlib", "matplotlib==3.9.4"),
]


def try_import(name):
    if name == "importlib_resources" and sys.version_info >= (3, 10):
        return True, "not needed on this Python"
    try:
        m = importlib.import_module(name)
        return True, getattr(m, "__version__", "ok")
    except Exception as e:   # ImportError, or a binary mismatch raising something else
        return False, f"{type(e).__name__}: {e}"


def draw_test_figure():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
    fig, ax = plt.subplots(figsize=(2, 1.5))
    pd.Series([1, 2, 3]).plot(ax=ax)
    fig.savefig("check_env_test.png", dpi=50)
    plt.close(fig)
    import os
    os.remove("check_env_test.png")


def install():
    cmd = [sys.executable, "-m", "pip", "install", "--user", *INSTALL]
    print("Running: " + " ".join(cmd) + "\n")
    return subprocess.call(cmd)


def main():
    print(f"Python {platform.python_version()} at {sys.executable}")
    if "--install" in sys.argv[1:]:
        if install() != 0:
            print("\npip failed. If the cluster has no route to PyPI, use a notebook session and")
            print("run sc.install_pypi_package(...) there. The error above says what went wrong.")
            return 1
        print("\nInstall finished. Checking the packages again.\n")
        importlib.invalidate_caches()
    missing = []
    for name, req in NEEDED:
        ok, info = try_import(name)
        print(f"  {'ok     ' if ok else 'MISSING'} {name:20s} {info}")
        if not ok:
            missing.append(req)
    try:
        import pyspark
        print(f"  ok      {'pyspark':20s} {pyspark.__version__}")
    except Exception as e:
        print(f"  note    pyspark not importable here ({e}). Run this script on the cluster.")
    if not missing:
        try:
            draw_test_figure()
            print("\nA test figure was drawn and saved. The environment is ready.")
            return 0
        except Exception as e:
            print(f"\nAll packages import but drawing failed: {type(e).__name__}: {e}")
            return 1
    print("\nInstall everything with one command (primary node, no sudo needed):\n")
    print("    python3 check_env.py --install")
    print("\nThe same by hand:\n")
    print("    python3 -m pip install --user " + " ".join(f'"{r}"' for r in INSTALL))
    print("\nThen run this script again. If a package cannot be downloaded, the cluster has no")
    print("route to PyPI: use a notebook session instead and run sc.install_pypi_package(...) there.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
