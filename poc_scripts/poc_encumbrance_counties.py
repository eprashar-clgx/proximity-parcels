# Import libraries
import argparse
import pandas as pd
from poc_county_encumbrances import run_parallel_processing 

def run_encumbrance_across_counties(encumbrance: str, fips_list: list):
    all_results = []

    for fips in fips_list:
        print(f"Processing {encumbrance} for FIPS: {fips}")
        df = run_parallel_processing(fips_code=fips, encumbrances=[encumbrance])
        df['fips_code'] = fips  # Make sure each row carries its FIPS for traceability
        all_results.append(df)

    # Concatenating all results into a single DataFrame
    combined_df = pd.concat(all_results, ignore_index=True)
    filename = f"merged_all_{encumbrance[:4]}.parquet"
    combined_df.to_parquet(filename)
    print(f"\n Saved combined results to: {filename}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("encumbrance", help="Encumbrance type to run (e.g., wetlands)")
    parser.add_argument("--fips", nargs="+", required=True, help="List of FIPS codes")

    args = parser.parse_args()
    run_encumbrance_across_counties(args.encumbrance, args.fips)
