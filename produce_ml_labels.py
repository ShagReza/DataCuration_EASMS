import pandas as pd
import numpy as np

def generate_ml_labels(df):
    """
    Assigns LABEL based on ENRICHMENT and PVALUE values:
    - LABEL = 1 if ENRICHMENT >= 5 and PVALUE <= 0.05
    - LABEL = 0 if ENRICHMENT <= 1 or PVALUE > 0.05
    - LABEL = -1 if 1 < ENRICHMENT < 5 and PVALUE <= 0.05
    - LABEL = -2 if ENRICHMENT or PVALUE is missing (NaN or empty)

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: Updated DataFrame with the LABEL column.
    """

    required_columns = {"ENRICHMENT", "PVALUE"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")

    # Convert empty strings to NaN
    df["ENRICHMENT"].replace("", np.nan, inplace=True)
    df["PVALUE"].replace("", np.nan, inplace=True)

    # Convert to numeric, invalid values become NaN
    df["ENRICHMENT"] = pd.to_numeric(df["ENRICHMENT"], errors="coerce")
    df["PVALUE"] = pd.to_numeric(df["PVALUE"], errors="coerce")

    def assign_label(row):
        enrichment = row["ENRICHMENT"]
        pvalue = row["PVALUE"]

        if pd.isna(enrichment) or pd.isna(pvalue):
            return -2
        elif enrichment >= 5 and pvalue <= 0.05:
            return 1
        elif 0 <= enrichment <= 1 or pvalue > 0.05:
            return 0
        elif 1 < enrichment < 5 and pvalue <= 0.05:
            return -1
        else:
            return -2  # fallback for unexpected cases

    df["LABEL"] = df.apply(assign_label, axis=1).astype("int8")

    return df
