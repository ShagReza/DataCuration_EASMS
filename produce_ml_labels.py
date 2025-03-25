import pandas as pd
import numpy as np

def generate_ml_labels(df):
    """
    Assigns LABEL based on EASMS_ENRICHMENT, PVALUE, and ISOMER values:

    - LABEL = 3: if EASMS_ENRICHMENT ≥ 5 and PVALUE ≤ 0.05 and ISOMER is not empty
    - LABEL = 2: if 5 ≤ EASMS_ENRICHMENT < 10 and PVALUE ≤ 0.05
    - LABEL = 1: if EASMS_ENRICHMENT ≥ 10 and PVALUE ≤ 0.05
    - LABEL = 0: if 0 ≤ EASMS_ENRICHMENT ≤ 1 or PVALUE < 0.05
    - LABEL = -1: if 1 < EASMS_ENRICHMENT < 5 and PVALUE > 0.05
    - LABEL = -2: if EASMS_ENRICHMENT is missing
    """

    required_columns = {"EASMS_ENRICHMENT", "PVALUE", "ISOMER"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")

    # Clean up values: replace empty strings with NaN
    df["EASMS_ENRICHMENT"].replace("", np.nan, inplace=True)
    df["PVALUE"].replace("", np.nan, inplace=True)

    # Convert columns to numeric where needed
    df["EASMS_ENRICHMENT"] = pd.to_numeric(df["EASMS_ENRICHMENT"], errors="coerce")
    df["PVALUE"] = pd.to_numeric(df["PVALUE"], errors="coerce")

    def assign_label(row):
        enrichment = row["EASMS_ENRICHMENT"]
        pvalue = row["PVALUE"]
        isomer = str(row["ISOMER"]).strip()

        if pd.isna(enrichment):
            return -2
        elif enrichment >= 5 and pvalue <= 0.05 and isomer != "":
            return 3
        elif 5 <= enrichment < 10 and pvalue <= 0.05:
            return 2
        elif enrichment >= 10 and pvalue <= 0.05:
            return 1
        elif 0 <= enrichment <= 1 or pvalue < 0.05:
            return 0
        elif 1 < enrichment < 5 and pvalue > 0.05:
            return -1
        else:
            return -2  # fallback

    df["LABEL"] = df.apply(assign_label, axis=1).astype("int8")

    return df
