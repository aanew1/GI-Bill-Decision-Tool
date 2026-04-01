from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


# ----------------------------
# Helpers
# ----------------------------

def normalize_zip(series: pd.Series) -> pd.Series:
    """
    Keep only digits; left-pad to 5.
    Handles ints/floats/strings and preserves leading zeros.
    """
    s = series.astype("string")
    s = s.str.replace(r"\D+", "", regex=True)  # keep digits only
    s = s.str.zfill(5)
    # If some are empty after stripping, keep as <NA>
    s = s.mask(s == "00000", pd.NA)
    return s


def normalize_paygrade(pg: str) -> str:
    """
    Normalize paygrade representations to something comparable.
    Examples:
      'E-5' -> 'E5'
      'E05' -> 'E5'
      ' e 5 ' -> 'E5'
    """
    if pg is None:
        return ""
    pg = str(pg).strip().upper().replace(" ", "").replace("-", "")
    # Remove leading zeros after the letter, e.g., E05 -> E5
    if len(pg) >= 2 and pg[0].isalpha():
        letter = pg[0]
        num = pg[1:]
        try:
            num_i = int(num)
            return f"{letter}{num_i}"
        except ValueError:
            return pg
    return pg


def try_read_delimited(path: Path, sep: str) -> pd.DataFrame | None:
    """
    Attempt to read as delimited with a given separator.
    """
    try:
        df = pd.read_csv(
            path,
            sep=sep,
            engine="python",
            header=None,
            dtype="string",
            on_bad_lines="skip",
        )
        # Require at least 3 columns to be useful (zip/mha or mha/grade/rate)
        if df.shape[1] >= 2:
            return df
    except Exception:
        return None
    return None


def read_unknown_ascii(path: Path) -> pd.DataFrame:
    """
    Read an unknown-format ASCII file:
    - tries common delimiters
    - falls back to whitespace splitting
    - falls back to fixed-width parsing
    """
    # Common separators in DoD ASCII datasets
    for sep in ["|", ",", "\t", ";"]:
        df = try_read_delimited(path, sep)
        if df is not None and df.shape[1] >= 2:
            return df

    # Whitespace-delimited
    try:
        df = pd.read_csv(
            path,
            sep=r"\s+",
            engine="python",
            header=None,
            dtype="string",
            on_bad_lines="skip",
        )
        if df.shape[1] >= 2:
            return df
    except Exception:
        pass

    # Fixed-width fallback
    try:
        df = pd.read_fwf(path, header=None, dtype="string")
        if df.shape[1] >= 2:
            return df
    except Exception as e:
        raise RuntimeError(f"Could not parse ASCII file: {path}\nLast error: {e}") from e

    raise RuntimeError(f"Could not parse ASCII file (no usable columns found): {path}")


def find_one(base_dir: Path, patterns: list[str]) -> Path:
    """
    Find the first matching file under base_dir for any of the given glob patterns.
    """
    for pat in patterns:
        matches = sorted(base_dir.glob(pat))
        if matches:
            return matches[0]
    raise FileNotFoundError(f"No file found in {base_dir} matching any of: {patterns}")


# ----------------------------
# Core loaders
# ----------------------------

def load_zip_to_mha(sorted_zipmha_path: Path) -> pd.DataFrame:
    """
    Load ZIP -> MHA crosswalk.

    attempt to identify:
      - a 5-digit zip column
      - an MHA code column (usually numeric-ish)
    """
    raw = read_unknown_ascii(sorted_zipmha_path)

    # Flatten to strings
    for c in raw.columns:
        raw[c] = raw[c].astype("string").str.strip()

    # Heuristics:
    # - ZIP column: looks like 5 digits
    # - MHA column: often 3-5 digits; not necessarily 5
    zip_col = None
    mha_col = None

    for c in raw.columns:
        # count how many values look like 5 digits
        looks_like_zip = raw[c].str.match(r"^\d{5}$", na=False).mean()
        if looks_like_zip > 0.5:
            zip_col = c
            break

    if zip_col is None:
        # maybe zip is missing leading zeros; accept 1-5 digits then zfill
        for c in raw.columns:
            looks_like_zipish = raw[c].str.match(r"^\d{1,5}$", na=False).mean()
            if looks_like_zipish > 0.5:
                zip_col = c
                break

    if zip_col is None:
        raise RuntimeError(
            f"Couldn't identify ZIP column in {sorted_zipmha_path}. "
            f"Parsed columns={raw.shape[1]} sample=\n{raw.head(5)}"
        )

    # Choose MHA column as "another mostly-numeric column that isn't ZIP"
    candidates = [c for c in raw.columns if c != zip_col]
    best = None
    best_score = -1.0
    for c in candidates:
        score = raw[c].str.match(r"^\d{2,6}$", na=False).mean()
        if score > best_score:
            best = c
            best_score = score
    mha_col = best

    if mha_col is None:
        raise RuntimeError(
            f"Couldn't identify MHA column in {sorted_zipmha_path}. "
            f"Parsed columns={raw.shape[1]} sample=\n{raw.head(5)}"
        )

    xwalk = raw[[zip_col, mha_col]].copy()
    xwalk.columns = ["zip_code", "mha_code"]
    xwalk["zip_code"] = normalize_zip(xwalk["zip_code"])
    xwalk["mha_code"] = xwalk["mha_code"].astype("string").str.strip()

    # drop blanks
    xwalk = xwalk.dropna(subset=["zip_code", "mha_code"]).drop_duplicates()

    return xwalk

def load_bah_with_dependents(bahw_dat_path: Path, paygrade: str) -> pd.DataFrame:
    """
    Load BAH with dependents table from bahwXX.dat where:
      - col 0 is the MHA (e.g., AK400)
      - remaining columns are rates for each pay grade in a fixed order

    We select ONLY the requested paygrade column and return:
      mha_code, BAH
    """

    # Map paygrade -> column index in the parsed file (0-based)
    # col 0 = MHA, so rates start at col 1
    # IMPORTANT: If your file uses a different order, adjust this mapping.
    pg_to_col = {
        "E1": 1,
        "E2": 2,
        "E3": 3,
        "E4": 4,
        "E5": 5,   # <-- THIS is what you want
        "E6": 6,
        "E7": 7,
        "E8": 8,
        "E9": 9,
        "W1": 10,
        "W2": 11,
        "W3": 12,
        "W4": 13,
        "W5": 14,
        "O1E": 15,
        "O2E": 16,
        "O3E": 17,
        "O1": 18,
        "O2": 19,
        "O3": 20,
        "O4": 21,
        "O5": 22,
        "O6": 23,
        "O7": 24,
        "O8": 25,
        "O9": 26,
        "O10": 27,
    }

    raw = read_unknown_ascii(bahw_dat_path)

    # Normalize paygrade like E-5 -> E5
    pg = normalize_paygrade(paygrade)

    if pg not in pg_to_col:
        raise RuntimeError(f"Paygrade '{paygrade}' normalized to '{pg}' not in mapping keys: {sorted(pg_to_col)}")

    rate_col_idx = pg_to_col[pg]

    if raw.shape[1] <= rate_col_idx:
        raise RuntimeError(
            f"BAH file {bahw_dat_path} has only {raw.shape[1]} columns, "
            f"but mapping expects at least {rate_col_idx + 1}. "
            f"Sample:\n{raw.head(5)}"
        )

    # col 0 = MHA code/name like AK400
    bah = raw[[0, rate_col_idx]].copy()
    bah.columns = ["mha_code", "BAH"]

    bah["mha_code"] = bah["mha_code"].astype("string").str.strip()
    bah["BAH"] = (
        bah["BAH"]
        .astype("string")
        .str.replace(",", "", regex=False)
        .astype("float")
    )

    bah = bah.dropna(subset=["mha_code", "BAH"]).drop_duplicates(subset=["mha_code"])
    return bah

# ----------------------------
# Main
# ----------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Append a BAH column (E-5 with dependents) to the Yellow Ribbon CSV.")
    parser.add_argument(
        "--input",
        type=str,
        default="data/processed/yellow_ribbon_schools_with_zip.csv",
        help="Path to input CSV (relative to project root).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed/yellow_ribbon_schools_with_zip_with_bah.csv",
        help="Path to write output CSV (relative to project root).",
    )
    parser.add_argument(
    "--bah_dir",
    type=str,
    default="data/BAH_ASCII_2025",
    help="Directory containing the unzipped DoD ASCII BAH files (relative to project root).",
    )
    parser.add_argument(
        "--paygrade",
        type=str,
        default="E-5",
        help="Paygrade to compute BAH for (default E-5).",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    input_csv = project_root / args.input
    output_csv = project_root / args.output
    bah_dir = project_root / args.bah_dir

    if not input_csv.exists():
        print(f"ERROR: Input CSV not found: {input_csv}", file=sys.stderr)
        return 2
    if not bah_dir.exists():
        print(f"ERROR: BAH directory not found: {bah_dir}", file=sys.stderr)
        return 2

    # Find required DoD files
    bahw_path = find_one(bah_dir, ["bahw*.dat", "bahw*"])
    zipmha_path = find_one(bah_dir, ["sorted_zipmha*", "zipmha*"])

    print(f"Using input CSV: {input_csv}")
    print(f"Using BAH with-dependents file: {bahw_path}")
    print(f"Using ZIP->MHA crosswalk file: {zipmha_path}")
    print(f"Target paygrade: {args.paygrade}")

    # Load your schools CSV
    schools = pd.read_csv(input_csv, dtype={"zip_code": "string"})
    if "zip_code" not in schools.columns:
        raise RuntimeError("Expected a 'zip_code' column in your input CSV, but it wasn't found.")

    schools["zip_code"] = normalize_zip(schools["zip_code"])

    # Load crosswalk + BAH table
    zip_to_mha = load_zip_to_mha(zipmha_path)
    bah = load_bah_with_dependents(bahw_path, args.paygrade)

    bah_target = bah  # already filtered to the requested paygrade column

    # Merge: schools -> MHA -> BAH
    merged = schools.merge(zip_to_mha, on="zip_code", how="left")
    merged = merged.merge(bah_target[["mha_code", "BAH"]], on="mha_code", how="left")

    # Keep column name exactly 'BAH' as requested
    # Drop helper column unless you want to keep it
    merged = merged.drop(columns=["mha_code"], errors="ignore")

    # Diagnostics
    total = len(merged)
    missing_zip = merged["zip_code"].isna().sum()
    missing_bah = merged["BAH"].isna().sum()

    print(f"Rows: {total}")
    print(f"Missing zip_code: {missing_zip}")
    print(f"Missing BAH after merge: {missing_bah} ({missing_bah/total:.1%})")

    # Write output
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_csv, index=False)
    print(f"Wrote: {output_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())