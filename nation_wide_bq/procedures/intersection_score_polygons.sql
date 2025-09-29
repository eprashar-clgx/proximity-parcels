-- Procedure to calculate intersection scores for parcels relative to polygon layers (protected areas, wetlands)
-- This version processes all data in a single batch without FIPS filtering

CREATE OR REPLACE PROCEDURE `clgx-gis-app-prd-364d.proximity_parcels.calculate_intersection_score_polygons_batch`(encumbrance STRING)
BEGIN
  -- === Define constants ===
  DECLARE table_name STRING;

  -- Thresholds and weights
  DECLARE area_ratio_weight FLOAT64 DEFAULT 0.5;
  DECLARE dist_weight FLOAT64 DEFAULT 0.3;
  DECLARE n_intersections_weight FLOAT64 DEFAULT 0.2;

  DECLARE area_ratio_low FLOAT64 DEFAULT 0.4;
  DECLARE area_ratio_high FLOAT64 DEFAULT 0.9;

  DECLARE dist_low FLOAT64 DEFAULT 100;
  DECLARE dist_med FLOAT64 DEFAULT 50;
  DECLARE dist_high FLOAT64 DEFAULT 10;
  DECLARE dist_overwrite FLOAT64 DEFAULT 0;

  DECLARE nint_med INT64 DEFAULT 2;
  DECLARE nint_high INT64 DEFAULT 3;

  DECLARE score_low_threshold FLOAT64 DEFAULT 0.35;
  DECLARE score_high_threshold FLOAT64 DEFAULT 0.7;

  -- Initialize proximity table that will be used as base table
  SET table_name = FORMAT("proximity_parcels.proximity_intersection_%s", encumbrance);

  -- === Step 1: Calculate scores for intersecting parcels in a Temp Table ===
  -- This single query calculates the scores for only the relevant rows and stores them.
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE scored_parcels AS
    WITH intersects_only AS (
      -- Select only the parcels that actually have intersections.
      SELECT *
      FROM `%s`
      WHERE proximity_label = 'intersects'
    ),
    scores_calculated AS (
      -- Calculate the intermediate weighted score.
      SELECT 
        *,
        (
          (
            CASE 
              WHEN intersect_area_perc >= %f THEN 1
              WHEN intersect_area_perc >= %f THEN 0.5
              WHEN intersect_area_perc > 0 THEN 0.25
              ELSE 0
            END
          ) * %f +
          (
            CASE 
              WHEN centroid_distance <= %f THEN 1
              WHEN centroid_distance <= %f THEN 0.5
              WHEN centroid_distance <= %f THEN 0.25
              WHEN centroid_distance > 0 THEN 0.15
              ELSE 0
            END
          ) * %f +
          (
            CASE 
              WHEN intersect_impact_count >= %d THEN 1
              WHEN intersect_impact_count = %d THEN 0.5
              WHEN intersect_impact_count > 0 THEN 0.25
              ELSE 0
            END
          ) * %f
        ) AS intersection_score_temp
      FROM intersects_only
    )
    -- Select the final parcel identifier, score, and label.
    SELECT
      parcelPTID,
      ROUND(intersection_score_temp, 2) AS intersection_score,
      CASE
        WHEN intersection_score_temp = 0 THEN NULL
        WHEN intersect_area_perc >= %f OR centroid_distance = %f THEN 'high'
        WHEN intersection_score_temp <= %f THEN 'low'
        WHEN intersection_score_temp < %f THEN 'medium'
        ELSE 'high'
      END AS intersection_label
    FROM scores_calculated;
  """, 
  table_name,
  area_ratio_high, area_ratio_low, area_ratio_weight,
  dist_high, dist_med, dist_low, dist_weight,
  nint_high, nint_med, n_intersections_weight,
  area_ratio_high, dist_overwrite, score_low_threshold, score_high_threshold
  );

  -- === Step 2: Atomically replace the final table with the updated scores ===
  -- This single statement reconstructs the entire table, joining the new scores.
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `%s`
    CLUSTER BY sourcedFips, geometry AS
    SELECT
      t.*,
      -- Join the new scores. Parcels that were not 'intersects' will get NULLs, which is correct.
      s.intersection_score,
      s.intersection_label
    FROM `%s` AS t
    LEFT JOIN scored_parcels AS s ON t.parcelPTID = s.parcelPTID;
  """,
  table_name,
  table_name
  );

END;

-- Example calls to the procedure for different encumbrance types
--CALL proximity_parcels.calculate_intersection_score_polygons_batch('protected_lands_national');
--CALL proximity_parcels.calculate_intersection_score_polygons_batch('wetlands');