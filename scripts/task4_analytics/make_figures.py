"""
scripts/task4_analytics/make_figures.py  --  Task 4 (Analytics & Visualisation)

Draws the four report figures from the small CSV tables that q1 to q4 write to
analytics_output/tables/. It needs only pandas and matplotlib (no Spark), so it
can be rerun on any machine that has the CSV files.

    python3 scripts/task4_analytics/make_figures.py

Figure 1  Q1  weekly high-minus-normal spread over time, and same-day price move by volume bucket
Figure 2  Q2  market volatility by GDELT event-volume and tone group (box plots of weekly values)
Figure 3  Q3  accuracy vs baseline by quarter, difference with interval, confusion matrix
Figure 4  Q4  weekly accuracy with and without GDELT, and the accuracy gain with interval

Every panel shows its group sizes and a source line. Intervals are week-clustered and use
Student's t (never the row-level intervals, which are far too narrow). All figures are
associations on source-price ratios, not returns and not causal effects.

Environment: PROJ_LOCAL_OUT (default "analytics_output") holds tables/ and figures/.
"""
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")  # no display needed on the cluster
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

LOCAL_OUT = os.environ.get("PROJ_LOCAL_OUT", "analytics_output")
TABLE_DIR = os.path.join(LOCAL_OUT, "tables")
FIG_DIR = os.path.join(LOCAL_OUT, "figures")
RUN_LOG = os.path.join(LOCAL_OUT, "analytics_run_log.jsonl")

# Validated categorical slots 1 to 3 (blue, orange, aqua) and ink tokens.
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
BLUE_RAMP = ["#86b6ef", "#3987e5", "#1c5cab"]       # ordinal low -> high
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": INK2, "axes.labelcolor": INK2, "text.color": INK,
    "xtick.color": INK2, "ytick.color": INK2, "font.size": 9,
    "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "legend.frameon": False, "lines.linewidth": 1.5,
})


def table(name):
    path = os.path.join(TABLE_DIR, f"{name}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found. Run the q1 to q4 scripts first.")
    return pd.read_csv(path)


def run_id():
    try:
        with open(RUN_LOG) as f:
            last = [line for line in f if line.strip()][-1]
        return json.loads(last).get("run_id", "")
    except Exception:
        return ""


def finish(fig, name, sources, note=""):
    src = "Source: " + ", ".join(sources) + (f". Run {run_id()}." if run_id() else ".")
    fig.text(0.01, 0.005, src + (" " + note if note else ""), fontsize=7, color=INK2, ha="left", va="bottom")
    os.makedirs(FIG_DIR, exist_ok=True)
    path = os.path.join(FIG_DIR, f"{name}.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[make_figures] {path}")
    return path


# Figure 1: Q1

def figure1():
    wk = table("q1_weekly_spread_by_week")
    sm = table("q1_weekly_spread")
    sd = table("q1_sameday_abs_change")
    wk["d"] = pd.to_datetime(wk["d"])
    wk = wk.sort_values("d")
    all_row = sm[sm["split"] == "all"].iloc[0]

    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4.2), gridspec_kw={"width_ratios": [1.6, 1]})
    a.plot(wk["d"], wk["spread_pchg"] * 100, color=BLUE, linewidth=1.0, label="Weekly spread")
    a.axhline(0, color=INK2, linewidth=0.8)
    lo, hi, mean = (all_row["ci95_lo_spread_pchg"] * 100, all_row["ci95_hi_spread_pchg"] * 100,
                    all_row["mean_spread_pchg"] * 100)
    a.axhspan(lo, hi, color=ORANGE, alpha=0.18, linewidth=0)
    a.axhline(mean, color=ORANGE, linewidth=1.5, label="Mean spread, 95% interval (t, by week)")
    a.set_title("Q1  High-volume minus normal-volume series: next-week price change")
    a.set_ylabel("Difference in next-week change of the\nopening price (percentage points)")
    a.legend(loc="upper left", fontsize=8)
    a.set_xlabel(f"Friday of the decision week. {int(all_row['n_weeks'])} weeks, about "
                 f"{all_row['avg_series_high']:.0f} high-volume and {all_row['avg_series_normal']:.0f} "
                 "normal-volume series per week")

    d = sd[sd["year"] == 0].copy()
    order = ["1_low", "2_normal", "3_high", "4_very_high"]
    labels = {"1_low": "Low\n(z < -1)", "2_normal": "Normal\n(-1 to 1)", "3_high": "High\n(1 to 2)",
              "4_very_high": "Very high\n(z >= 2)"}
    d = d.set_index("z_bucket").reindex(order).reset_index()
    x = range(len(d))
    b.bar(list(x), d["mean_abs_chg"] * 100, color=BLUE, width=0.6)
    for i, r in d.iterrows():
        b.text(i, r["mean_abs_chg"] * 100, f"{r['mean_abs_chg'] * 100:.2f}", ha="center", va="bottom", fontsize=8)
    b.set_xticks(list(x))
    b.set_xticklabels([f"{labels[k]}\nn = {int(n):,}" for k, n in zip(d["z_bucket"], d["n"])], fontsize=7.5)
    b.set_title("Q1  Same-day price move by same-day volume")
    b.set_ylabel("Mean absolute close-to-close change (%)")
    b.grid(axis="x", visible=False)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    return finish(fig, "fig1_q1_volume",
                  ["q1_weekly_spread_by_week", "q1_weekly_spread", "q1_sameday_abs_change"],
                  "Associations on source-price ratios, not returns.")


# Figure 2: Q2

def boxpanel(ax, panel, group_col, title, xlabel_extra):
    groups = ["1_low", "2_mid", "3_high"]
    data, ns = [], []
    for g in groups:
        v = panel.loc[panel[group_col] == g, "market_vol"].dropna() * 100
        data.append(v.values)
        ns.append(len(v))
    bp = ax.boxplot(data, patch_artist=True, widths=0.55, showfliers=True,
                    medianprops={"color": INK, "linewidth": 1.5},
                    whiskerprops={"color": INK2}, capprops={"color": INK2},
                    flierprops={"marker": "o", "markersize": 3, "markeredgecolor": INK2, "markerfacecolor": "none"})
    for patch, c in zip(bp["boxes"], BLUE_RAMP):
        patch.set_facecolor(c)
        patch.set_edgecolor(INK2)
    ax.set_xticks([1, 2, 3])
    ax.set_xticklabels([f"{n}\nn = {k} weeks" for n, k in zip(["Low", "Mid", "High"], ns)])
    ax.set_title(title)
    ax.set_xlabel(xlabel_extra)
    ax.grid(axis="x", visible=False)


def figure2():
    panel = table("q2_weekly_panel")
    fig, (a, b, c) = plt.subplots(1, 3, figsize=(13.5, 4.4), sharey=True)
    boxpanel(a, panel, "g_events_level", "Q2  By GDELT event volume", "Terciles of mean events per day in the week")
    boxpanel(b, panel, "g_events_rel", "Q2  By event volume vs prior 8 weeks",
             "Terciles of events relative to the previous 8 weeks")
    boxpanel(c, panel, "g_tone", "Q2  By GDELT tone", "Terciles of average tone (Low = most negative)")
    a.set_ylabel("Market volatility in the same week\n(std-dev of daily market change, %)")
    fig.suptitle("Weeks grouped by GDELT signal, July 2015 to June 2017 (same-week association only)",
                 x=0.01, ha="left", fontsize=9, color=INK2)
    fig.tight_layout(rect=(0, 0.04, 1, 0.95))
    return finish(fig, "fig2_q2_news", ["q2_weekly_panel"],
                  "Cut-offs use the whole window and the test is exploratory. Association, not cause.")


# Figure 3: Q3

def figure3():
    met = table("q3_metrics")
    pair = table("q3_paired_vs_baseline")
    comps = [c for c in pair["comparison"].unique()]
    final = [c for c in comps if not str(c).startswith("gdelt_")]
    final = final[0] if final else comps[0]
    test = met[met["split"] == "test"]

    fig = plt.figure(figsize=(12.5, 4.6))
    gs = fig.add_gridspec(1, 3, width_ratios=[1.2, 1.4, 0.7])
    a, b, c = (fig.add_subplot(gs[0, i]) for i in range(3))

    # (a) accuracy by quarter: final model vs its baseline
    q = test[(test["comparison"] == final) & (test["dimension"] == "quarter")]
    models = [m for m in q["model"].unique()]
    base_name = [m for m in models if str(m).startswith("baseline")]
    base_name = base_name[0] if base_name else None
    mod_name = [m for m in models if m != base_name][0]
    qs = sorted(q["grp"].unique())
    w = 0.38
    for k, (name, colr, lab) in enumerate([(mod_name, BLUE, f"Model ({mod_name})"),
                                           (base_name, ORANGE, "Baseline (training majority class)")]):
        if name is None:
            continue
        vals = [q[(q["model"] == name) & (q["grp"] == g)]["accuracy"].iloc[0] for g in qs]
        a.bar([i + (k - 0.5) * w for i in range(len(qs))], vals, width=w - 0.04, color=colr, label=lab)
    ns = [int(q[(q["model"] == mod_name) & (q["grp"] == g)]["n"].iloc[0]) for g in qs]
    a.set_xticks(range(len(qs)))
    a.set_xticklabels([f"{g}\nn = {n:,}" for g, n in zip(qs, ns)], fontsize=8)
    a.set_ylim(0, 1)
    a.set_ylabel("Accuracy on 2017 test rows")
    a.set_title("Q3  Accuracy by quarter")
    a.legend(loc="upper center", fontsize=7.5, ncol=1)
    a.grid(axis="x", visible=False)

    # (b) accuracy difference vs its own baseline, by volatility group
    p = pair[(pair["split"] == "test") & (pair["dimension"].isin(["overall", "vol_group"]))].copy()
    marks = {final: ("o", BLUE), "gdelt_price_only": ("s", ORANGE), "gdelt_price_gdelt": ("^", AQUA)}
    grp_order = ["all", "1_low_vol", "2_mid_vol", "3_high_vol"]
    ylabels, y = [], 0
    for g in grp_order:
        for comp in [final, "gdelt_price_only", "gdelt_price_gdelt"]:
            r = p[(p["comparison"] == comp) & (p["grp"] == g)]
            if r.empty:
                continue
            r = r.iloc[0]
            mk, col = marks.get(comp, ("o", INK2))
            b.errorbar(r["acc_diff_vs_baseline"] * 100, y,
                       xerr=[[(r["acc_diff_vs_baseline"] - r["ci95_lo"]) * 100],
                             [(r["ci95_hi"] - r["acc_diff_vs_baseline"]) * 100]],
                       fmt=mk, color=col, markersize=6, capsize=3, linewidth=1.2)
            ylabels.append((y, f"{'overall' if g == 'all' else g.replace('_vol', '')} | {comp} "
                               f"({int(r['n_weeks'])} wk)"))
            y += 1
        y += 0.6
    b.axvline(0, color=INK2, linewidth=0.8)
    b.set_yticks([t[0] for t in ylabels])
    b.set_yticklabels([t[1] for t in ylabels], fontsize=7)
    b.invert_yaxis()
    b.set_xlabel("Accuracy minus its own baseline (percentage points), 95% interval\n(week-clustered, Student's t)")
    b.set_title("Q3  Difference from baseline by volatility group")
    b.grid(axis="y", visible=False)

    # (c) confusion matrix of the final model on all test rows
    cm = test[(test["comparison"] == final) & (test["dimension"] == "overall") & (test["model"] == mod_name)].iloc[0]
    mat = [[cm["tp"], cm["fn"]], [cm["fp"], cm["tn"]]]
    c.imshow([[cm["tp"], cm["fn"]], [cm["fp"], cm["tn"]]], cmap="Blues")
    for i in range(2):
        for j in range(2):
            c.text(j, i, f"{int(mat[i][j]):,}", ha="center", va="center", fontsize=11,
                   color="white" if mat[i][j] > max(cm["tp"], cm["fn"], cm["fp"], cm["tn"]) * 0.6 else INK)
    c.set_xticks([0, 1])
    c.set_xticklabels(["Predicted\nup", "Predicted\ndown"])
    c.set_yticks([0, 1])
    c.set_yticklabels(["Actually up", "Actually down"])
    c.set_title("Q3  Confusion matrix, final model")
    c.grid(False)
    for s in c.spines.values():
        s.set_visible(False)

    fig.tight_layout(rect=(0, 0.04, 1, 1))
    return finish(fig, "fig3_q3_reliability", ["q3_metrics", "q3_paired_vs_baseline"],
                  "Test period only. A difference of 0 means the model predicts what the baseline predicts.")


# Figure 4: Q4

def figure4():
    wk = table("q4_weekly")
    pr = table("q4_paired")
    wk["d"] = pd.to_datetime(wk["d"])
    wk = wk.sort_values("d")

    fig, (a, b) = plt.subplots(1, 2, figsize=(11.5, 4.4), gridspec_kw={"width_ratios": [1.3, 1]})
    a.plot(wk["d"], wk["acc_price_only"], color=BLUE, marker="o", markersize=3, label="Price-only model")
    a.plot(wk["d"], wk["acc_price_gdelt"], color=ORANGE, marker="s", markersize=3, label="Price + GDELT model")
    a.set_ylabel("Weekly accuracy (same rows for both models)")
    a.set_title(f"Q4  Weekly accuracy, Jan to Jun 2017 ({len(wk)} weeks)")
    a.legend(loc="lower left", fontsize=8)
    a.xaxis.set_major_locator(mdates.MonthLocator())
    a.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    a.set_xlabel("Friday of the decision week, 2017")

    rows = pr[pr["dimension"].isin(["overall", "quarter", "vol_group"])].copy()
    rows["label"] = rows.apply(lambda r: ("all rows" if r["dimension"] == "overall" else str(r["grp"]))
                               + f"  ({int(r['n_weeks'])} wk, {int(r['weeks_gdelt_better'])} better / "
                               f"{int(r['weeks_gdelt_worse'])} worse)", axis=1)
    for i, (_, r) in enumerate(rows.iterrows()):
        b.errorbar(r["acc_gain"] * 100, i,
                   xerr=[[(r["acc_gain"] - r["acc_gain_ci95_lo"]) * 100],
                         [(r["acc_gain_ci95_hi"] - r["acc_gain"]) * 100]],
                   fmt="o", color=BLUE, markersize=6, capsize=3, linewidth=1.2)
    b.axvline(0, color=INK2, linewidth=0.8)
    b.set_yticks(range(len(rows)))
    b.set_yticklabels(rows["label"], fontsize=7)
    b.invert_yaxis()
    b.set_xlabel("Accuracy gain of price + GDELT over price-only\n(percentage points), 95% interval,\nweek-clustered, Student's t")
    b.set_title("Q4  Gain from adding GDELT features")
    b.grid(axis="y", visible=False)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    return finish(fig, "fig4_q4_gdelt_gain", ["q4_weekly", "q4_paired"],
                  "GDELT features are market-wide, so the effective sample is the weeks. The two models may use different algorithms (q4_model_families).")


def main():
    made = []
    for fn in (figure1, figure2, figure3, figure4):
        try:
            made.append(fn())
        except FileNotFoundError as e:
            print(f"[make_figures] skipped {fn.__name__}: {e}")
    if not made:
        sys.exit(1)


if __name__ == "__main__":
    main()
