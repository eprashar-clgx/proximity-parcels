# Importing libraries
# Importing required libraries
import os
import subprocess
import time
import logging
from typing import Literal
from collections import defaultdict

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon
from shapely import wkt
import fiona
import matplotlib.pyplot as plt
import seaborn as sns
import nation_wide.utils as utils

# Setup logging
# TODO: Setup location to save logs and time functions
logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Define constants here
geo_crs = "EPSG:4326"
projected_crs = "EPSG:3857" 
POC_FINALIZED_COUNTIES = [
    # urban
    '17031',
    '13121',
    '53033',
    # sub-urban
    '48491',
    '29181',
    '42011',
    # rural 
    '55107',
    '35051',
    '17127'
]

ENCUMBRANCES = [
    'roadways',
    'railways',
    'protected_lands',
    'wetlands',
    'transmission_lines',
    ]
EncumbranceType = Literal[
    'roadways',
    'railways',
    'protected_lands',
    'wetlands',
    'transmission_lines',
]

# CHANGE PATH TO YOUR LOCAL BEFORE EXECUTING
# Files are saved as {fips_encumbrance.parquet} or {fips_parcels.parquet} 
PARQUET_FOLDER = r"C:\Users\eprashar\OneDrive - CoreLogic Solutions, LLC\github\feb_25_encumbered_parcels\encumbered-parcels\ingestion_parquets"

# Define function to get encumbrance parquet for the county
def load_encumbrance_data(
        fips_code:str,
        encumbrance:str):
    """
    """
    # Load encumbrance data saved in local
    # CHECK PATH FOR PARQUET FOLDER 
    # Construct path to parquet file
    parquet_path = os.path.join(PARQUET_FOLDER, f"{fips_code}_{encumbrance}.parquet")

    # Check if file exists before reading
    if not os.path.isfile(parquet_path):
        raise FileNotFoundError(f"Parquet file not found at: {parquet_path}. Please check the path!")

    # Proceed to load the file
    gdf_encumbrance = gpd.read_parquet(parquet_path)

    # Convert to EPSG:4326
    gdf_encumbrance = gdf_encumbrance.to_crs(geo_crs)
    # print(f'CRS of the {encumbrance} dataframe is {gdf_encumbrance.crs}')
    return gdf_encumbrance

# Define function to get parcel data for the defined county
def load_parcel_data(fips_code: str) -> gpd.GeoDataFrame:
    """
    Load parcel data from BigQuery and filter by FIPS code.
    """
    # Load parcel parquet file saved in local
    # CHECK PATH FOR PARQUET FOLDER
    # Construct path to parquet file
    parquet_path = os.path.join(PARQUET_FOLDER, f"{fips_code}_parcels.parquet")

    # Check if file exists before reading
    if not os.path.isfile(parquet_path):
        raise FileNotFoundError(f"Parquet file not found at: {parquet_path}. Please check the path!")

    # Proceed to load the file
    gdf_parcel = gpd.read_parquet(parquet_path)
    
    # Convert to EPSG:4326
    gdf_parcel = gdf_parcel.to_crs(geo_crs)
    # print(f'CRS of the parcel dataframe is {gdf_parcel.crs}')
    return gdf_parcel

# Define buffer distances and scores based on polygon or line geometry
def buffer_scores_and_labels(
        encumbrance: EncumbranceType):
    '''
    Define buffer distances and score labels based on encumbrance type.
    '''
    if encumbrance == 'railways' or encumbrance == 'transmission_lines':
        buffer_distances = [5, 150, 300, 750, 1000]
        score_labels = ['intersects', 'very high', 'high', 'medium', 'low']
    
    elif encumbrance == 'roadways':
        buffer_distances = [5, 10, 25, 50, 100]
        score_labels = ['intersects', 'very high', 'high', 'medium', 'low']
    
    elif encumbrance == 'wetlands' or encumbrance == 'protected_lands':
        buffer_distances = [0, 5, 10, 50, 100]
        score_labels = ['intersects', 'very high', 'high', 'medium', 'low']

    return buffer_distances, score_labels

# Function to calculate intersection metrics
def calculate_intersection_metrics(
        encumbrance:EncumbranceType,
        all_parcels,
        matched_parcels,
        buffered_encumbrance
        ):
    '''

    '''
    # Reset index to make sure merge works properly
    buffer_gdf = buffered_encumbrance.reset_index().rename(columns={'index': 'buffer_index'})

    # Merge buffer geometries into matched results using index_right from sjoin
    matched_with_geom = matched_parcels.reset_index().merge(
        buffer_gdf[['buffer_index', 'geometry']],
        left_on='index_right',
        right_on='buffer_index',
        how='left',
        suffixes=('', '_buffer')
    )
    # Ensure both geometries are in a projected CRS (3857) for accurate area
    # Setting projection for parcel geometry
    matched_with_geom = matched_with_geom.set_geometry('geometry')
    matched_with_geom = matched_with_geom.to_crs(projected_crs)
    
    # Setting projection for buffered value of encumbrance geometry 
    matched_with_geom = matched_with_geom.set_geometry('geometry_buffer', drop=False)
    matched_with_geom[f'geometry_buffer_{encumbrance}'] = matched_with_geom['geometry_buffer'].to_crs(projected_crs)

    # Calculate intersection geometry 
    matched_with_geom[f'intersection_geom_{encumbrance}'] = matched_with_geom['geometry'].intersection(matched_with_geom[f'geometry_buffer_{encumbrance}'])

    # Group by parcel index -- this will help later to pick parcel row in case of multiple intersections
    matched_with_geom['orig_index'] = matched_with_geom['index']  # preserve original index for reassigning

    # Calculate intersection metrics for lines
    if encumbrance in ['railways', 'roadways', 'transmission_lines']:
        
        # Calculate intersection perimeter
        matched_with_geom[f'intersec_perim_{encumbrance}'] = matched_with_geom[f'intersection_geom_{encumbrance}'].length

        # Approximate true line length (perimeter / 2)
        matched_with_geom[f'approx_line_len_{encumbrance}'] = round(
            matched_with_geom[f'intersec_perim_{encumbrance}'] / 2, 2
        )

        # Retain row with max approximate line length
        max_length_idx = matched_with_geom.groupby('orig_index')[f'approx_line_len_{encumbrance}'].idxmax()
        matched_max_area = matched_with_geom.loc[max_length_idx]

        # Store this back into parcels dataframe
        all_parcels.loc[matched_max_area['orig_index'], f'approx_line_len_{encumbrance}'] = matched_max_area[f'approx_line_len_{encumbrance}'].values

    elif encumbrance in ['wetlands', 'protected_lands']:

        # Calculate parcel intersection ratio
        matched_with_geom[f'intersec_area_{encumbrance}'] = round(matched_with_geom[f'intersection_geom_{encumbrance}'].area,2)
        matched_with_geom['parcel_area'] = matched_with_geom['geometry'].area
        matched_with_geom[f'area_ratio_{encumbrance}'] = round((
            matched_with_geom[f'intersec_area_{encumbrance}'] / matched_with_geom['parcel_area']
        ),2)
        
        # Calculate parcel centroid to wetland distance
        # Convert parcel centroid column to GeoSeries
        # Convert WKT strings to actual shapely geometries
        matched_with_geom['centroid'] = matched_with_geom['centroid'].apply(wkt.loads)
        matched_with_geom['centroid'] = gpd.GeoSeries(matched_with_geom['centroid'], crs=geo_crs)
        
        # Reproject centroid to projected CRS (e.g. 3857)
        matched_with_geom['centroid'] = matched_with_geom['centroid'].to_crs(projected_crs)

        # parcel centroid to encumbered geometry distance
        matched_with_geom[f'parcel_dist_to_{encumbrance}'] = round(matched_with_geom['centroid'].distance(
            matched_with_geom[f'geometry_buffer_{encumbrance}']
            ),2)
        
        # Chunk to retain row with the max area ratio
        max_area_idx = matched_with_geom.groupby('orig_index')[f'area_ratio_{encumbrance}'].idxmax()
        matched_max_area = matched_with_geom.loc[max_area_idx]

        # Store metrics back in parcels_mod for max intersection row
        for col in ['intersec_area', 'area_ratio', 'parcel_dist_to']:
            all_parcels.loc[matched_max_area['orig_index'], f'{col}_{encumbrance}'] = matched_max_area[f'{col}_{encumbrance}'].values

    # Calculate number of intersecting encumbrances per parcel (for all encumbrance types)
    intersection_counts = matched_with_geom.groupby('orig_index').size()
    
    # Store the number of intersections as a separate metric
    all_parcels.loc[intersection_counts.index, f'n_{encumbrance}_intersections'] = intersection_counts.values
    
    logger.info(f"Finished calculating intersection metrics for encumbrance {encumbrance}!")
    return all_parcels

# Calculate proximity score and intersection metrics based on encumbrance type
# @log_time
def get_proximity_score_and_intersection_metrics(
        encumbrance: EncumbranceType,
        gdf_parcel: gpd.GeoDataFrame, 
        gdf_encumbrance: gpd.GeoDataFrame, 
        ) -> gpd.GeoDataFrame:
    """
    Assign proximity scores to parcels based on their distance to encumbrance features.
    
    Parameters:
    parcels (GeoDataFrame): The parcels to be scored.
    encumbrance (GeoDataFrame): The encumbrance features to score against.
    
    Returns:
    GeoDataFrame: Parcels with assigned proximity scores.
    """
    # Start logging process
    logger.info("Starting proximity scoring...")

    # Derive scores based on nature of encumbrance
    buffer_distances, score_labels = buffer_scores_and_labels(encumbrance)
    logger.info(f"Buffer distances: {buffer_distances}")
    logger.info(f"Score labels: {score_labels}")
    
    # Ensure the CRS of both GeoDataFrames match
    gdf_encumbrance = gdf_encumbrance.to_crs(gdf_parcel.crs)

    # Create a copy of the parcels GeoDataFrame to avoid modifying the original
    parcels_mod = gdf_parcel.copy()

    # Initialize a new column for proximity score
    # Explicitly defining dtype object to avoid SettingWithCopyWarning
    parcels_mod[f'proximity_score_{encumbrance}'] = pd.Series([None]*len(parcels_mod), dtype='object')

    # Calculate proximity scores based on distance to encumbrance features
    # Initialize i 
    i = 0
    for distance, label in zip(buffer_distances, score_labels):
        
        # Buffer individual geometries by specified distance
        # Project to a projected CRS for buffering
        buffer_gdf = gdf_encumbrance.to_crs(projected_crs)

        # Create buffered geometries and assign to a new column
        buffer_gdf['buffered_geometry'] = buffer_gdf.geometry.buffer(distance) # This geometry holds the buffered version

        # Drop the original geometry column
        buffer_gdf = buffer_gdf.drop(columns=['geometry'])

        # Rename the buffered geometry column to 'geometry'
        buffer_gdf = buffer_gdf.rename(columns={'buffered_geometry': 'geometry'})

        # Set CRS and reproject back to geo_crs
        buffer_gdf = buffer_gdf.set_geometry('geometry').to_crs(geo_crs)
        logger.info(f'Created buffered geometry with distance {distance} meters and CRS {buffer_gdf.crs}')
        
        # Use spatial join to find parcels within the buffer distance
        matched = gpd.sjoin(parcels_mod[parcels_mod[f'proximity_score_{encumbrance}'].isna()],
                             buffer_gdf,
                             predicate='intersects',
                             how='inner')

        # Assign proximity scores
        parcels_mod.loc[matched.index, f'proximity_score_{encumbrance}'] = label
        
        # Add encumbrance column values to main dataframe
        # TODO: Find more elegant solution
        cols_to_add = buffer_gdf.columns.difference(['geometry'])
        for col in cols_to_add:
            parcels_mod.loc[matched.index, f"{col.lower()}_{encumbrance}"] = matched[col].values
        logger.info(f"Assigned proximity score {label} to {len(matched)} parcels...")

        # Add intersecton metrics when buffer is smallest
        if i == 0:
            logger.info('Now adding intersection metrics...')
            parcels_mod = calculate_intersection_metrics(
                encumbrance=encumbrance,
                all_parcels=parcels_mod,
                matched_parcels=matched,
                buffered_encumbrance=buffer_gdf
            )
        # Increment i
        i += 1 
            
    # Fill remaining as no encumbrance and change to geographic CRS
    parcels_mod.fillna({f'proximity_score_{encumbrance}':'no_encumbrance'}, inplace=True)
    
    # Re-project everything to geographic CRS
    for column in parcels_mod.select_dtypes(include=['geometry']).columns:
        parcels_mod[column] = parcels_mod[column].to_crs(geo_crs)
    # print(f'CRS of output dataframe is {parcels_mod.crs}')
    print('Proximity scoring complete! Counts of proximity scores are...')

    # Print value counts of proximity scores
    print(parcels_mod[f'proximity_score_{encumbrance}'].value_counts())
    return parcels_mod

# Function to calculate intersection strength score
def calculate_intersection_score(
        encumbrance:EncumbranceType,
        gdf_parcel: gpd.GeoDataFrame,
        *,
        area_ratio_weight: float = 0.5,
        dist_weight: float = 0.3,
        n_intersections_weight: float = 0.2,
        area_ratio_thresholds: tuple = (0.4, 0.9),
        dist_thresholds: tuple = (100, 50, 10, 0),
        n_intersections_thresholds: tuple = (2 ,3),
        score_thresholds: tuple = (0.35, 0.7)
    ) -> gpd.GeoDataFrame:
    '''
    Calculates intersection strength score for a given encumbrance using:
    - Area ratio
    - Centroid distance
    - Number of intersections
    
    Accepts tunable weights and thresholds via kwargs.

    Returns:
    GeoDataFrame with a new 'intersection_score_{encumbrance}' column.
    '''
    # Check score is only asked for polygon encumbrances
    if encumbrance not in ['wetlands', 'protected_lands']:
        raise ValueError(
            f"Intersection scoring is only applicable for polygon encumbrances. "
            f"Valid options are: 'wetlands', 'protected_lands'."
        )

    # Create dataframe copy to avoid modifying the original
    parcels_mod = gdf_parcel.copy()

    # Unpack thresholds
    ar_low, ar_high = area_ratio_thresholds
    dist_low, dist_med, dist_high, dist_overwrite = dist_thresholds
    nint_med, nint_high = n_intersections_thresholds

    # Score area_ratio
    ar_col = f'area_ratio_{encumbrance}'
    parcels_mod[f'score_ar_{encumbrance}'] = parcels_mod[ar_col].apply(
        lambda x: 1 if x >= ar_high else (0.5 if x >= ar_low else 0.25 if x > 0 else 0)
    )

    # Score parcel_dist_to
    dist_col = f'parcel_dist_to_{encumbrance}'
    parcels_mod[f'score_dist_{encumbrance}'] = parcels_mod[dist_col].apply(
        lambda x: 1 if x <= dist_high else (0.5 if x <= dist_med else 0.25 if x <= dist_low else 0.15 if x > 0 else 0)
    )

    # Score number of intersections
    nint_col = f'n_{encumbrance}_intersections'
    parcels_mod[f'score_nint_{encumbrance}'] = parcels_mod[nint_col].apply(
        lambda x: 1 if x >= nint_high else (0.5 if x == nint_med else 0.25 if x > 0 else 0)
    )

    # Final weighted score
    score_col = f'intersection_score_{encumbrance}'
    parcels_mod[score_col] = (
        parcels_mod[f'score_ar_{encumbrance}'] * area_ratio_weight +
        parcels_mod[f'score_dist_{encumbrance}'] * dist_weight +
        parcels_mod[f'score_nint_{encumbrance}'] * n_intersections_weight
    )

    # TODO: Once testing is over, drop intermediate columns
    # parcels_mod = parcels_mod.drop(columns=[
    # f'parcel_dist_to_{encumbrance}',
    # f'area_ratio_{encumbrance}',
    # f'n_{encumbrance}_intersections'])

    # Add low, medium, high labels
    # Labeling with override for high-impact flags
    # TODO: Make this more efficient
    low_thres, high_thres = score_thresholds
    label_col = f'intersection_label_{encumbrance}'

    parcels_mod[label_col] = parcels_mod.apply(
    lambda row: None if row[score_col] == 0 else (
        'high' if (
            row[ar_col] >= ar_high 
            or row[dist_col] == dist_overwrite
        ) else (
            'low' if row[score_col] < low_thres else
            'medium' if row[score_col] < high_thres else
            'high'
        )
    ),
    axis=1
)

    print(f'Intersection scoring completed for {encumbrance}!')
    return parcels_mod