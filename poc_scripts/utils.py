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

                while datetime.now() - start_time < max_wait:
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