import concurrent.futures
import pandas as pd
import time
from functools import partial
import argparse

# Functions implemented in module
from poc_tested_modules import (
    load_encumbrance_data,
    load_parcel_data,
    get_proximity_score_and_intersection_metrics,
    calculate_intersection_score
)

# List of encumbrances and the FIPS codes
ENCUMBRANCES = ['railways', 'roadways', 'transmission_lines','wetlands', 'protected_lands']

# Mapping of fips and states
FIPS_TO_STATE = {
    '17031': 'IL',  # Cook County, IL
    '13121': 'GA',  # Fulton County, GA
    '53033': 'WA',  # King County, WA
    '48491': 'TX',  # Williamson County, TX
    '29181': 'MO',  # Warren County, MO
    '42011': 'PA',  # Berks County, PA
    '55107': 'WI',  # Rusk County, WI
    '35051': 'NM',  # Sierra County, NM
    '17127': 'IL',  # Massac County, IL
}

# FIPS code for county analysis
# FIPS = '55107'

# Parcel columns that need to be added only once
BASE_PARCEL_COLUMNS = [
    'clip', 'fips_code', 'owner', 'tot_val',
    'mkt_val', 'tax_amt', 'sale_price', 'mtg_amt', 'land_acres',
    'land_sq_ft', 'land_use', 'land_use_t', 'geometry', 'taxability',
    'centroid', 'vertices', 'perimeter', 'area', 'compactness',
    'convex_hull'
]

# Function to process end to end workflow for one encumbrance type per county
def process_encumbrance(fips_code: str, encumbrance: str):
    """Full pipeline for a single encumbrance and FIPS"""
    print(f"Processing {encumbrance} for {fips_code}...")

    # Step 1: Load encumbrance data
    encumbrance_data = load_encumbrance_data(
        fips_code,
        encumbrance=encumbrance
        )

    # Step 2: Load parcel data
    raw_parcels = load_parcel_data(fips_code)

    # Step 3: Compute proximity score and intersection metrics
    parcels_with_proximity = get_proximity_score_and_intersection_metrics(
        encumbrance=encumbrance,
        gdf_parcel = raw_parcels,
        gdf_encumbrance = encumbrance_data
        )
    
    # Step 4: Calculate intersection score only for specific encumbrances
    if encumbrance in ['wetlands', 'protected_lands']:
        final_parcels = calculate_intersection_score(
            encumbrance, 
            gdf_parcel=parcels_with_proximity
        )
    else:
        final_parcels = parcels_with_proximity

    print(f"Finished {encumbrance} for {fips_code} with {len(final_parcels)} parcels.")
    return final_parcels

# Function to run multiple encumbrances in parallel for the same county
def run_parallel_processing(fips_code: str, encumbrances: list):
    """Run encumbrance processing in parallel and merge results"""
    print(f"Running full workflow for {fips_code}...")

    # Prepare the worker function with fips
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for enc in encumbrances:
            futures.append(
                executor.submit(process_encumbrance, fips_code, enc)
            )

        # Collect results
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Merge all results on spatial_parcel_point_id_pp
    print("Merging results...")
    final_merged = results[0]

    # Drop base parcel fields from subsequent results to avoid merge conflict
    for df in results[1:]:
        df_clean = df.drop(columns=[col for col in BASE_PARCEL_COLUMNS if col in df.columns], errors='ignore')
        final_merged = final_merged.merge(
            df_clean,
            on='spatial_parcel_point_id_pp',
            how='outer'
        )

    print(f"All encumbrance data merged. Final shape: {final_merged.shape}")
    return final_merged

# Running the workflow with argparse to pass fips code and encumbrance names
if __name__ == '__main__':
    start_time = time.time()
    parser = argparse.ArgumentParser(description='Run encumbrance analysis for a given FIPS code.')
    parser.add_argument('fips', type=str, help='FIPS code of the county')
    parser.add_argument(
        '--encumbrances', 
        nargs='+', 
        default=['wetlands', 'protected_lands'],  # or whatever defaults you want
        help='List of encumbrances to run (space-separated)'
    )
    args = parser.parse_args()

    # Run the parallel processing
    merged_parcels = run_parallel_processing(args.fips, args.encumbrances)

    # Create a filename that reflects the encumbrances
    enc_str = '_'.join(enc[:4] for enc in args.encumbrances)
    output_filename = f"merged_{args.fips}_{enc_str}.parquet"
    merged_parcels.to_parquet(output_filename)

    print(f"Saved output to {output_filename}")
    end_time = time.time()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")