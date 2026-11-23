# Databricks notebook source
# MAGIC %md
# MAGIC # 02_1_explore — Genie Code writes this file
# MAGIC
# MAGIC Do not paste code into `02_Build_NBA_with_Genie_Code`. Ask Genie Code to implement **this** notebook, then re-run `%run ./genie/02_1_explore/code` in `02`.

# COMMAND ----------

# DBTITLE 1,Explore conversion drivers (pandas)
# 2.1 Exploratory analysis of conversion drivers (pandas only).
# Expects `history_pdf` from the parent notebook; reads it without modifying it.
import pandas as pd

if "history_pdf" not in globals():
    raise NameError("`history_pdf` not found - run the 2.0 load cell in the parent notebook first.")

_eda = history_pdf[["action_taken", "lead_source", "age_band", "converted"]]


def _conv_by(col):
    return (
        _eda.groupby(col)["converted"]
        .agg(leads="size", conversions="sum", conv_rate="mean")
        .sort_values("conv_rate", ascending=False)
        .round({"conv_rate": 3})
    )


print(f"Overall conversion rate: {_eda['converted'].mean():.1%} "
      f"({int(_eda['converted'].sum()):,} of {len(_eda):,} contacts)")

for _col in ["action_taken", "lead_source", "age_band"]:
    print(f"\n=== Conversion rate by {_col} ===")
    print(_conv_by(_col).to_string())

print("\n=== Conversion rate: action_taken (rows) x age_band (cols) ===")
action_age_conv = pd.crosstab(
    _eda["action_taken"], _eda["age_band"],
    values=_eda["converted"], aggfunc="mean",
).round(3)
print(action_age_conv.to_string())