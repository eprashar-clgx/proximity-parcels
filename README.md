# Encumbered-parcels
### Description updated on: April 23, 2025
Sample data for testing is in [this Drive folder](https://drive.google.com/drive/folders/1Q0sSf-hAkc6ddCoGbM362aZyqzWDu1f2?usp=sharing)

### Summary
Through this initiative, we can calculate proximity zones and intersection strength for parcels with *features of interest*. Specific examples include:

1. Parcels that are 5, 50, 100, 500 meters away from a railway or a transmission line. In this case, the *feature of interest* would be a railway or a transmission line.
2. Determining how *strongly* parcels intersect a wetland or a protected land. In this case, the *feature of interest* would be a wetland or a protected land. More tangibly, a parcel that barely intersects with a long and thin riverine will have a *low intersection* whereas the same value for a parcel on a protected forest will be *high*. 

The hypothesized application of this product is in *site selection*. Proximity and intersection to certain *features of interest* can be useful signals to prioritize or discard regions while exploring potential sites. 

### POC Scope
Scope for this POC can be defined along three dimensions: *features of interest*, *geospatial analysis* and *coverage*. Details for each are mentioned below:

1. **Features of interest**: We're using the following 5 features in the POC:
    * Railways: Sourced from [US DOT Bureau of Transportation Statistics' North American Rail Network Lines](https://geodata.bts.gov/datasets/usdot::north-american-rail-network-lines/about)
    * Roadways: Sourced from [US DOT Bureau of Transportation Statistics' North American Roads](https://geodata.bts.gov/datasets/usdot::north-american-roads/about)
    * Transmission Lines: Sourced from [Department of Homeland Security's HIFLD Database]()
    * Wetlands: Sourced from [US Department of Fisheries and Wildlife Services](https://www.fws.gov/program/national-wetlands-inventory/download-state-wetlands-data)
    * Protected Lands: Sourced from [US Geological Survey](https://www.sciencebase.gov/catalog/item/6759abcfd34edfeb8710a004) 

2. **Geospatial Analysis**: Out of the five features mentioned above, the first three are treated as buffered geometries for proximity analysis while the last two also have an intersection strength calculation on top. Key points:
    * Proximity thresholds can be defined separately for each feature of interest; for example, a proximity of 50 meteres may be very high for railways, but not so for wetlands. 
    * In addition, the number of thresholds can also be defined so long as a label is associated with the numeric threshold. For example, a new threshold of 200 meters can be added with a small tweak in the code, but the label corresponding to this distance also needs to be defined.
    * Intersection strength is currently calculated only for polygon geometries (wetlands and protected lands). This is because while testing, intersection strength for lines didn't seem to add any interpretable value addition on top of proximity. 
    * Intersection strength is calculated using three metrics: *intersected geometry area as a percentage of parcel area*, *distance of parcel centroid from nearest edge of intersected feature* and *number of intersections*. These are mentioned in descending order of their contribution to the intersection score. Weights and thresholds are static, and can be tweaked with a minor change in code.   

3. **Coverage**: This POC covers the following 9 counties:
    * '17031': 'IL',  # Cook County, IL
    * '13121': 'GA',  # Fulton County, GA
    * '53033': 'WA',  # King County, WA
    * '48491': 'TX',  # Williamson County, TX
    * '29181': 'MO',  # Warren County, MO
    * '42011': 'PA',  # Berks County, PA
    * '55107': 'WI',  # Rusk County, WI
    * '35051': 'NM',  # Sierra County, NM
    * '17127': 'IL',  # Massac County, IL

### Testing: Results, Workflow and Calculations

**Testing Results**
The folder [*sample_data*](https://drive.google.com/drive/folders/1Q0sSf-hAkc6ddCoGbM362aZyqzWDu1f2?usp=sharing) contains parquet files for 5 counties:

* '13121': 'GA',  # Fulton County, GA
* '48491': 'TX',  # Williamson County, TX
* '29181': 'MO',  # Warren County, MO
* '42011': 'PA',  # Berks County, PA
* '17127': 'IL',  # Massac County, IL

Each county has a merged parquet file and a parquet for each encumbrance type. These can be loaded into QGIS for validation and quick overview.

**Testing Workflow**
The curious mind can also test the workflow and/or play around with underlying parameters such as buffer distances or intersection weight calculations. This can be done for two counties: **Warren, MO (fips 29181) and Berks, PA (fips 42011)**, and needs interaction with just two files: *poc_tested_modules.py* and *poc_county_encumbrances.py*. Below are step by step instructions:

1. Clone the github repository
2. *Changing filepaths:* In the file *poc_tested_modules.py*, replace the path for the variable `PARQUET_FOLDER` to the path of `ingestion_parquets` in your local.
3. *Transferring data from Drive*: Copy the contents of `ingestion_parquets`in [this Drive link](https://drive.google.com/drive/folders/1DnO9B_0cxOUjdCB7LD0VObIKnt0b5ZV_?usp=sharing) to your local folder mentioned in step 1.
4. *Running county workflow*: Run the python script in the terminal with a specific *fips* and *encumberances separated by spaces*. For example, if I wanted to check roadways and wetlands results for Warren County (29181), I would do:
`...poc_county_encumbrances.py 29181 roadways wetlands` and press enter. This would save a parquet file in the repo that can be analyzed in QGIS along with encumbrance files saved in `ingestion_parquets`.

5. *Testing Calculations:* The majority of work in this workflow is happening in two functions in the file `poc_tested_modules.py`. The first uses buffer distances to assign proximity labels, and the second assigns weights and thresholds to intersection metrics to calculate an intersection/encumbrance score. 
```
# Define buffer distances and scores based on polygon or line geometry
def buffer_scores_and_labels(
        encumbrance: EncumbranceType):
    '''
    Define buffer distances and score labels based on encumbrance type.
    '''
```
and 

```
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
```
After playing around with buffer distances and/or intersection score thresholds, a user should save the script and follow steps 1-4 above.

#### Underlying Workflow
The overall workflow is summarized in this diagram:
![Encumbrance Workflow](docs\analysis_workflow.PNG)

