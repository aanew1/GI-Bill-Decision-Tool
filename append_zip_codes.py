from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# ---------------- CONFIG ----------------
ZIP_COL_NAME = "zip_code"

# If True: overwrite data/raw/yellow_ribbon_schools.csv (after making a backup)
# If False: write to data/processed/yellow_ribbon_schools_with_zip.csv
OUTPUT_IN_PLACE = False
# ----------------------------------------

def find_zcta_shapefile(zcta_dir: Path) -> Path:
    """
    Auto-find a tl_XXXX_us_zcta520.shp somewhere under data/zcta/.
    This avoids hard-coding the year.
    """
    candidates = sorted(zcta_dir.rglob("tl_*_us_zcta520.shp"))
    if not candidates:
        raise FileNotFoundError(
            f"Could not find a ZCTA shapefile under: {zcta_dir}\n"
            "Expected something like: data/zcta/tl_2025_us_zcta520/tl_2025_us_zcta520.shp"
        )
    # Choose the most recent-looking filename (sorted works well for tl_YYYY_...)
    return candidates[-1]

def main():
    # scripts/append_zip_codes.py -> project root is parents[1]
    project_root = Path(__file__).resolve().parents[1]

    raw_csv = project_root / "data" / "raw" / "yellow_ribbon_schools.csv"
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    zcta_dir = project_root / "data" / "zcta"
    zcta_shp = find_zcta_shapefile(zcta_dir)

    if not raw_csv.exists():
        raise FileNotFoundError(f"Raw CSV not found: {raw_csv}")

    print(f"Project root: {project_root}")
    print(f"Reading CSV:   {raw_csv}")
    print(f"Using ZCTA:    {zcta_shp}")

    # 1) Read raw CSV
    df = pd.read_csv(raw_csv)

    # 2) Validate expected columns
    required_cols = {"lat", "long"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}. "
            f"Found columns: {list(df.columns)}"
        )

    # 3) Coerce lat/long to numeric
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["long"] = pd.to_numeric(df["long"], errors="coerce")

    # Mask valid coordinates
    valid_mask = df["lat"].between(-90, 90) & df["long"].between(-180, 180)

    # 4) Build GeoDataFrame for valid rows
    df_valid = df.loc[valid_mask].copy()
    gdf_points = gpd.GeoDataFrame(
        df_valid,
        geometry=[Point(lon, lat) for lon, lat in zip(df_valid["long"], df_valid["lat"])],
        crs="EPSG:4326",
    )

    # 5) Load ZCTA polygons
    gdf_zips = gpd.read_file(zcta_shp).to_crs(gdf_points.crs)

    # TIGER vintage uses different column names; try common ones
    zip_id_col = next(
        (c for c in ["ZCTA5CE20", "ZCTA5CE10", "ZCTA5CE00"] if c in gdf_zips.columns),
        None,
    )
    if not zip_id_col:
        raise ValueError(
            f"Couldn't find ZCTA ID column in shapefile. "
            f"Available columns: {list(gdf_zips.columns)}"
        )

    # 6) Spatial join (point-in-polygon)
    joined = gpd.sjoin(
        gdf_points,
        gdf_zips[[zip_id_col, "geometry"]],
        how="left",
        predicate="within",
    )

    # 7) Append ZIP column back to original df
    s = joined[zip_id_col].astype("string")
    df.loc[valid_mask, ZIP_COL_NAME] = s.where(s.notna(), pd.NA).str.zfill(5)

    # 8) Write output
    if OUTPUT_IN_PLACE:
        # Backup the current raw file (once)
        backup_path = raw_csv.with_name(raw_csv.stem + ".backup.csv")
        if not backup_path.exists():
            raw_csv.replace(backup_path)  # move original to backup
            print(f"Backup created (moved original): {backup_path}")

        # Write updated CSV back to raw path
        df.to_csv(raw_csv, index=False)
        print(f"Updated in place: {raw_csv} (added '{ZIP_COL_NAME}')")
    else:
        out_csv = processed_dir / "yellow_ribbon_schools_with_zip.csv"
        df.to_csv(out_csv, index=False)
        print(f"Wrote processed file: {out_csv}")

if __name__ == "__main__":
    main()
