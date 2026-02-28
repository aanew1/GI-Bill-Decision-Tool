import time
import requests
import pandas as pd
from pathlib import Path

BASE_URL_TEMPLATE = "https://api.va.gov/v0/gi/yellow_ribbon_programs?contribution_amount=unlimited&number_of_students=unlimited&page={page}&per_page=10"

TOTAL_PAGES = 136

HEADERS = {
    "User-Agent": "YellowRibbonSchools/1.0 (educational, non-commercial)",
    "Accept": "application/json",
}

SLEEP_SECONDS = 1.0

def extract_row(item: dict) -> dict:
    attrs = item.get("attributes", {})
    return {
        # Core identifiers
        "yellow_ribbon_id":item.get("id"),
        "institution_id":attrs.get("institution_id"),
        # Additional attributes
        "institution_name": attrs.get("name_of_institution"),
        "city":attrs.get("city"),
        "state":attrs.get("state"),
        "country":attrs.get("country"),
        "lat":attrs.get("latitude"),
        "long":attrs.get("longitude"),
        "degree_level":attrs.get("degree_level"),
        "division_professional_school":attrs.get("division_professional_school"),
        "online_only":attrs.get("online_only"), 
        "distance_learning":attrs.get("distance_learning"),
        "correspondence":attrs.get("correspondence"),
    }

def fetch_page(page: int) -> dict:
    url = BASE_URL_TEMPLATE.format(page=page)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()

def main():
    rows = []

    for page in range(1, TOTAL_PAGES + 1):
        print(f"Fetching page {page}/{TOTAL_PAGES}...")
        payload = fetch_page(page)

        items = payload.get("data", [])
        if not items:
            print(f"No items found on page {page}. Stopping early.")
            break

        for item in items:
            row = extract_row(item)
            rows.append(row)

        time.sleep(SLEEP_SECONDS)

    df = pd.DataFrame(rows)

    project_dir = Path(__file__).resolve().parents[1]  # <-- project root (parent of scripts/)
    raw_dir = project_dir / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    output_path = raw_dir / "yellow_ribbon_schools.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")

if __name__ == "__main__":
    main()