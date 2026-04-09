import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------- CONFIG ----------------
FMR_YEAR = 2025

INPUT_FILENAME = "yellow_ribbon_schools_with_zip_with_bah.csv"
OUTPUT_FILENAME = "yellow_ribbon_schools_with_zip_with_bah_with_fmr.csv"
CROSSWALK_FILENAME = "ZIP_COUNTY_122025.xlsx"

TOKEN_ENV_VAR = "HUD_USER_TOKEN"
HUD_FMR_BASE_URL = "https://www.huduser.gov/hudapi/public/fmr/statedata"

REQUEST_TIMEOUT = 30

VALID_HUD_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC",
    "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY",
    "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT",
    "VT", "VA", "WA", "WV", "WI", "WY", "PR", "GU", "VI", "MP"
}
# ----------------------------------------


def project_root() -> Path:
    """
    Resolve the project root assuming this script lives in yellow_ribbon/scripts/.
    """
    return Path(__file__).resolve().parent.parent


def normalize_zip(value: Any) -> str | None:
    """
    Normalize ZIP code to a 5-digit string.
    Returns None if the value is missing/invalid.
    """
    if pd.isna(value):
        return None

    text = str(value).strip()

    if not text:
        return None

    # Handle Excel/pandas cases like 501 or 501.0
    if text.endswith(".0"):
        text = text[:-2]

    # Keep only digits if the value is numeric-ish
    if text.isdigit():
        text = text.zfill(5)
    else:
        return None

    return text if len(text) == 5 else None


def normalize_county_fips(value: Any) -> str | None:
    """
    Normalize county FIPS to a 5-digit string.
    Returns None if missing/invalid.
    """
    if pd.isna(value):
        return None

    text = str(value).strip()

    if not text:
        return None

    if text.endswith(".0"):
        text = text[:-2]

    if not text.isdigit():
        return None

    text = text.zfill(5)
    return text if len(text) == 5 else None


def normalize_hud_fips_to_county_fips_5(value: Any) -> str | None:
    """
    HUD county 'fips_code' appears as a 10-character string where the first
    5 digits represent the county FIPS and the last 5 are typically '99999'.

    Example:
        0600199999 -> 06001
    """
    if pd.isna(value):
        return None

    text = str(value).strip()

    if not text:
        return None

    if text.endswith(".0"):
        text = text[:-2]

    if not text.isdigit():
        return None

    text = text.zfill(10)
    county_fips_5 = text[:5]
    return county_fips_5 if len(county_fips_5) == 5 else None


def parse_bool(value: Any) -> bool:
    """
    Robust boolean parser for values like:
    TRUE, FALSE, True, False, 1, 0, yes, no
    """
    if pd.isna(value):
        return False

    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y"}


def get_hud_token() -> str:
    """
    Read the HUD API token from an environment variable.
    """
    token = os.getenv(TOKEN_ENV_VAR)
    if not token:
        raise RuntimeError(
            f"Missing HUD API token. Set environment variable {TOKEN_ENV_VAR} "
            f"before running this script."
        )
    return token


def load_input_csv(input_path: Path) -> pd.DataFrame:
    """
    Load the main school dataset.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path, dtype={"zip_code": "string", "state": "string"})
    return df


def load_crosswalk(crosswalk_path: Path) -> pd.DataFrame:
    """
    Load the HUD-USPS ZIP-COUNTY crosswalk and reduce it to one county per ZIP
    using the highest TOT_RATIO.
    """
    if not crosswalk_path.exists():
        raise FileNotFoundError(f"Crosswalk file not found: {crosswalk_path}")

    crosswalk = pd.read_excel(
        crosswalk_path,
        dtype={
            "ZIP": "string",
            "COUNTY": "string",
            "USPS_ZIP_PREF_STATE": "string",
        },
    )

    required_cols = {
        "ZIP",
        "COUNTY",
        "USPS_ZIP_PREF_STATE",
        "TOT_RATIO",
    }
    missing_cols = required_cols - set(crosswalk.columns)
    if missing_cols:
        raise ValueError(
            f"Crosswalk file is missing required columns: {sorted(missing_cols)}"
        )

    crosswalk["ZIP"] = crosswalk["ZIP"].apply(normalize_zip)
    crosswalk["COUNTY"] = crosswalk["COUNTY"].apply(normalize_county_fips)
    crosswalk["USPS_ZIP_PREF_STATE"] = crosswalk["USPS_ZIP_PREF_STATE"].astype(str).str.strip().str.upper()
    crosswalk["TOT_RATIO"] = pd.to_numeric(crosswalk["TOT_RATIO"], errors="coerce")

    crosswalk = crosswalk.dropna(subset=["ZIP", "COUNTY", "TOT_RATIO"]).copy()

    # Keep the county with the highest TOT_RATIO for each ZIP
    crosswalk = crosswalk.sort_values(
        by=["ZIP", "TOT_RATIO"],
        ascending=[True, False],
        kind="stable",
    )
    crosswalk_best = crosswalk.drop_duplicates(subset=["ZIP"], keep="first").copy()

    crosswalk_best = crosswalk_best.rename(
        columns={
            "ZIP": "zip_code",
            "COUNTY": "county_fips_5",
            "USPS_ZIP_PREF_STATE": "crosswalk_state",
            "TOT_RATIO": "crosswalk_tot_ratio",
        }
    )

    return crosswalk_best[
        ["zip_code", "county_fips_5", "crosswalk_state", "crosswalk_tot_ratio"]
    ]


def fetch_state_fmr(state_code: str, year: int, token: str) -> list[dict[str, Any]]:
    """
    Fetch county-level FMR data for one state from HUD.
    Returns the 'counties' list from the API response.
    """
    url = f"{HUD_FMR_BASE_URL}/{state_code}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    params = {"year": year}

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"HUD API request failed for state {state_code} "
            f"with status {response.status_code}: {response.text}"
        ) from exc

    payload = response.json()

    if "data" not in payload:
        raise RuntimeError(f"Unexpected HUD response structure for state {state_code}: {payload}")

    counties = payload["data"].get("counties", [])
    if counties is None:
        counties = []

    return counties


def build_fmr_lookup(states: list[str], year: int, token: str) -> pd.DataFrame:
    """
    Build a single county-level FMR lookup table across all needed states.
    """
    all_rows: list[dict[str, Any]] = []

    for state_code in sorted(states):
        counties = fetch_state_fmr(state_code=state_code, year=year, token=token)
        time.sleep(1.1)

        for row in counties:
            all_rows.append(
                {
                    "state": str(row.get("statecode", "")).strip().upper(),
                    "hud_statename": row.get("statename"),
                    "hud_county_name": row.get("county_name"),
                    "hud_town_name": row.get("town_name"),
                    "hud_metro_name": row.get("metro_name"),
                    "hud_fips_code": row.get("fips_code"),
                    "county_fips_5": normalize_hud_fips_to_county_fips_5(row.get("fips_code")),
                    "fmr_efficiency": row.get("Efficiency"),
                    "fmr_1br": row.get("One-Bedroom"),
                    "fmr_2br": row.get("Two-Bedroom"),
                    "fmr_3br": row.get("Three-Bedroom"),
                    "fmr_4br": row.get("Four-Bedroom"),
                    "fmr_percentile": row.get("FMR Percentile"),
                    "fmr_smallarea_status": row.get("smallarea_status"),
                    "fmr_year": str(year),
                }
            )

    fmr_df = pd.DataFrame(all_rows)

    if fmr_df.empty:
        return fmr_df

    # Clean/normalize
    fmr_df["state"] = fmr_df["state"].astype(str).str.strip().str.upper()
    fmr_df["county_fips_5"] = fmr_df["county_fips_5"].apply(normalize_county_fips)

    numeric_cols = [
        "fmr_efficiency",
        "fmr_1br",
        "fmr_2br",
        "fmr_3br",
        "fmr_4br",
        "fmr_percentile",
    ]
    for col in numeric_cols:
        fmr_df[col] = pd.to_numeric(fmr_df[col], errors="coerce")

    fmr_df = fmr_df.dropna(subset=["state", "county_fips_5"]).copy()

    # Defensive dedupe in case of repeated rows
    fmr_df = fmr_df.drop_duplicates(subset=["state", "county_fips_5"], keep="first")

    return fmr_df


def main() -> None:
    root = project_root()

    input_path = root / "data" / "processed" / INPUT_FILENAME
    output_path = root / "data" / "processed" / OUTPUT_FILENAME
    crosswalk_path = root / "data" / "reference" / "hud_usps_crosswalk" / CROSSWALK_FILENAME

    print("Loading school dataset...")
    df = load_input_csv(input_path)

    required_school_cols = {
        "institution_name",
        "state",
        "zip_code",
        "online_only",
    }
    missing_school_cols = required_school_cols - set(df.columns)
    if missing_school_cols:
        raise ValueError(
            f"Input CSV is missing required columns: {sorted(missing_school_cols)}"
        )

    # Preserve original row count/order
    df = df.copy()
    df["state"] = df["state"].astype(str).str.strip().str.upper()
    df["zip_code"] = df["zip_code"].apply(normalize_zip)
    df["online_only_bool"] = df["online_only"].apply(parse_bool)

    missing_zip_mask = df["zip_code"].isna()
    online_only_mask = df["online_only_bool"]

    eligible_mask = (~missing_zip_mask) & (~online_only_mask)

    print("Loading HUD-USPS ZIP-COUNTY crosswalk...")
    crosswalk_best = load_crosswalk(crosswalk_path)

    print("Merging county FIPS onto eligible schools...")
    eligible_df = df.loc[eligible_mask].copy()

    eligible_df = eligible_df.merge(
        crosswalk_best,
        on="zip_code",
        how="left",
    )

    # Optional consistency check: crosswalk state should usually match school state
    state_mismatch_mask = (
        eligible_df["crosswalk_state"].notna()
        & eligible_df["state"].notna()
        & (eligible_df["crosswalk_state"] != eligible_df["state"])
    )

    # Null out clearly mismatched crosswalk results to avoid bad joins
    eligible_df.loc[state_mismatch_mask, ["county_fips_5", "crosswalk_tot_ratio"]] = pd.NA

    states_needed = sorted(
        [
            s for s in eligible_df["state"].dropna().unique().tolist()
            if s in VALID_HUD_STATE_CODES
        ]
    )

    print(f"Fetching HUD FMR county data for {len(states_needed)} states (year={FMR_YEAR})...")
    token = get_hud_token()
    fmr_lookup = build_fmr_lookup(states=states_needed, year=FMR_YEAR, token=token)

    if fmr_lookup.empty:
        raise RuntimeError("No FMR data was returned from HUD.")

    print("Merging FMR data onto eligible schools...")
    eligible_df = eligible_df.merge(
        fmr_lookup,
        on=["state", "county_fips_5"],
        how="left",
    )

    # Prepare output by starting with original df and adding empty columns
    output_df = df.copy()

    new_cols = [
        "county_fips_5",
        "crosswalk_state",
        "crosswalk_tot_ratio",
        "hud_statename",
        "hud_county_name",
        "hud_town_name",
        "hud_metro_name",
        "hud_fips_code",
        "fmr_efficiency",
        "fmr_1br",
        "fmr_2br",
        "fmr_3br",
        "fmr_4br",
        "fmr_percentile",
        "fmr_smallarea_status",
        "fmr_year",
    ]
    for col in new_cols:
        output_df[col] = pd.NA

    # Write merged values back only for eligible rows
    output_df.loc[eligible_mask, new_cols] = eligible_df[new_cols].values

    # Add status columns for transparency/debugging
    output_df["fmr_row_status"] = pd.NA
    output_df.loc[online_only_mask, "fmr_row_status"] = "skipped_online_only"
    output_df.loc[missing_zip_mask, "fmr_row_status"] = "skipped_missing_zip"

    eligible_output_mask = eligible_mask
    no_county_match_mask = eligible_output_mask & output_df["county_fips_5"].isna()
    no_fmr_match_mask = eligible_output_mask & output_df["county_fips_5"].notna() & output_df["fmr_2br"].isna()
    matched_mask = eligible_output_mask & output_df["fmr_2br"].notna()

    output_df.loc[no_county_match_mask, "fmr_row_status"] = "eligible_no_county_match"
    output_df.loc[no_fmr_match_mask, "fmr_row_status"] = "eligible_no_fmr_match"
    output_df.loc[matched_mask, "fmr_row_status"] = "matched"

    # Drop helper column
    output_df = output_df.drop(columns=["online_only_bool"])

    print(f"Writing output to: {output_path}")
    output_df.to_csv(output_path, index=False)

    # Summary
    total_rows = len(output_df)
    skipped_online = int((output_df["fmr_row_status"] == "skipped_online_only").sum())
    skipped_missing_zip = int((output_df["fmr_row_status"] == "skipped_missing_zip").sum())
    no_county_match = int((output_df["fmr_row_status"] == "eligible_no_county_match").sum())
    no_fmr_match = int((output_df["fmr_row_status"] == "eligible_no_fmr_match").sum())
    matched = int((output_df["fmr_row_status"] == "matched").sum())

    print("\nDone.")
    print(f"Total rows: {total_rows}")
    print(f"Skipped (online_only=True): {skipped_online}")
    print(f"Skipped (missing zip_code): {skipped_missing_zip}")
    print(f"Eligible but no county match: {no_county_match}")
    print(f"Eligible but no FMR match: {no_fmr_match}")
    print(f"Matched rows: {matched}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)