# Utility functions to calculate encumbrance scores

# Importing required libraries
import os
import subprocess
from datetime import datetime, timedelta
import logging
import time

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon
from shapely import wkt
import matplotlib.pyplot as plt
import seaborn as sns

from google.cloud import bigquery
from pandas_gbq import to_gbq


# BIGQ Importing modules
# CONSTANTS
PROJECT = 'clgx-gis-app-dev-06e3'
DATASET = 'property'
TABLE =  'spatialrecord_polygon'
CREDENTIALS_PATH =  r"C:\Users\eprashar\AppData\Roaming\gcloud\application_default_credentials.json"
# A smaller number creates more, smaller polygons. 0.1 is a good starting point.
GRID_CELL_SIZE = 0.1


# Functions to check authentication key
# Function to check google authentication token and re-generate if it is expired/doesn't exist
def check_and_authenticate(json_path):
    '''
    Function to check google authentication token and re-generate if it is expired/doesn't exist
    '''
    try:
        if not os.path.exists(json_path):
            raise FileNotFoundError("Credentials file not found")
        # Get modification time of the file
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(json_path))
        current_time = datetime.now()

        # Check if the file is older than 24 hours
        if current_time - file_mod_time > timedelta(hours=24):
            print("Credentials file is older than 24 hours. Re-authenticating...")

            # Re-authenticate
            try:
                print(f"Trying reauthentication on gcloud server using shell command...")
                subprocess.run("start cmd /c gcloud auth application-default login", shell=True, check=True)
                print('Login window opened...please complete authentication')
                
                # Poll for file modification
                print("Waiting for credentials file to update...")
                max_wait = 300  # seconds
                check_interval = 2  # seconds
                start_time = datetime.now()

                while (datetime.now() - start_time).total_seconds() < max_wait:
                    new_mod_time = datetime.fromtimestamp(os.path.getmtime(json_path))
                    if new_mod_time > file_mod_time:
                        print("Authentication confirmed! Credentials file updated.")
                        break
                    time.sleep(check_interval)
                else:
                    print("Timed out waiting for credentials file update.")

            except subprocess.CalledProcessError as e:
                print(f"Error during re-authentication: {e}")
            except Exception as e:
                print(f'Authentication failed because of {e}')
        else:
            print("Credentials file is valid.")
    except Exception as e:
        print(f"Error: {e}")

# Read parcel data from BigQuery
def read_bigquery_to_gdf(project, dataset, table, query=None, output= 'df', geometry_col=None):
    try:
        client = bigquery.Client(project=project)
    except Exception as e:
        print
    if query:
        query_job = client.query(query)
        df = query_job.to_dataframe()
    else:
        table_ref = client.dataset(dataset).table(table)
        table = client.get_table(table_ref)
        df = client.list_rows(table).to_dataframe()
    
    # Convert geometry to shapely objects
    if output == 'gpd' or output =='gdf' :
        df['geometry'] = df[geometry_col].apply(wkt.loads)
        # Create geopandas dataframe
        gdf = gpd.GeoDataFrame(df, geometry='geometry')
        gdf.crs = 'EPSG:4326'
        return gdf
    else:
        return df 


#  SUBDIVISION FUNCTION

def subdivide_large_geometries(file_path):
    """
    Reads a GeoParquet file, subdivides its geometries using a grid,
    and saves the result to a new file.
    """
    print(f"--- Starting Subdivision for: {os.path.basename(file_path)} ---")
    
    # --- 1. Load the County-Level Parquet File ---
    try:
        print("Step 1: Loading data into GeoDataFrame...")
        # Read the Parquet file into a pandas DataFrame first
        df = pd.read_parquet(file_path)
        
        # Convert the WKB 'geometry' column into actual shapely objects
        # This is the step that will require the most memory.
        gdf = gpd.GeoDataFrame(
            df, 
            geometry=gpd.GeoSeries.from_wkb(df['geometry']), 
            crs="EPSG:4326"
        )
        print(f"-> Successfully loaded {len(gdf):,} features.")
    except Exception as e:
        print(f"-> ERROR: Failed to load file. It might be too large for memory. Error: {e}")
        return

    # --- 2. Create a Subdivision Grid ---
    print(f"Step 2: Creating a subdivision grid with cell size {GRID_CELL_SIZE} degrees...")
    
    # Get the total geographic extent of all features in the file
    minx, miny, maxx, maxy = gdf.total_bounds
    
    # Create a grid of square polygons that covers the entire extent
    grid_cells = []
    for x in np.arange(minx, maxx, GRID_CELL_SIZE):
        for y in np.arange(miny, maxy, GRID_CELL_SIZE):
            grid_cells.append(box(x, y, x + GRID_CELL_SIZE, y + GRID_CELL_SIZE))
    
    grid_gdf = gpd.GeoDataFrame(geometry=grid_cells, crs="EPSG:4326")
    print(f"-> Created a grid with {len(grid_gdf)} cells.")

    # --- 3. Perform the Intersection (Clipping) ---
    print("Step 3: Clipping original geometries against the grid...")
    # The 'overlay' function with 'intersection' is the core of this process.
    # It finds the overlapping area between the wetlands and the grid cells.
    subdivided_gdf = gdf.overlay(grid_gdf, how='intersection')
    print(f"-> Original {len(gdf):,} features were subdivided into {len(subdivided_gdf):,} smaller features.")

    # --- 4. Save the Subdivided Data ---
    # Convert the geometry back to WKB for efficient storage
    subdivided_gdf['geometry'] = subdivided_gdf['geometry'].apply(lambda geom: geom.wkb)
    
    # Define the output path
    output_filename = os.path.basename(file_path).replace(".parquet", "_subdivided.parquet")
    output_path = os.path.join(WETLAND_COUNTY_FILES, output_filename)
    
    print(f"Step 4: Saving subdivided data to: {output_path}")
    # Save the final result as a standard pandas DataFrame
    pd.DataFrame(subdivided_gdf).to_parquet(output_path, index=False)
    print("-> Save complete.")