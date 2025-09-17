-- Procedure to calculate proximity scores for parcels relative to polygon encumbrances (protected areas, wetlands)
-- This version processes all data in a single batch without FIPS filtering

CREATE OR REPLACE PROCEDURE `clgx-gis-app-uat-a0e0.proximity_parcels.calculate_proximity_score_polygons_batch`(encumbrance_table STRING, encumbrance_id_col STRING)
BEGIN 
  -- Define constants
  DECLARE final_table_name STRING;
  DECLARE buffer_tiers ARRAY<STRUCT<buffer_meters INT64, label STRING>>;
  DECLARE max_buffer_meters INT64;

  SET final_table_name = FORMAT("proximity_parcels.proximity_intersection_%s", encumbrance_table);

  -- Define the buffer tiers for polygons.
  SET buffer_tiers = [
    STRUCT(0 AS buffer_meters, 'intersects' AS label),
    STRUCT(10, 'very high'),
    STRUCT(25, 'high'),
    STRUCT(75, 'medium'),
    STRUCT(150, 'low')
  ];

  SET max_buffer_meters = (SELECT MAX(buffer_meters) FROM UNNEST(buffer_tiers));

  -- Step 1: Get all encumbrance geometries from the pre-computed materialized view.
    EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE encumbrance_in_scope AS
    SELECT *
    FROM `proximity_parcels.%s_mv`
  """, 
  encumbrance_table);

  -- Step 2: Prepare all parcels data.
  CREATE OR REPLACE TEMP TABLE parcels_in_scope AS
  SELECT 
    parcelPTID,
    clip,
    sourcedFips,
    geom, -- Use the alias 'geom' for consistency
    ST_AREA(geom) AS parcel_area,
    ST_CENTROID(geom) AS centroid
  FROM `proximity_parcels.parcels_mv`;

  -- Step 3: Pre-calculate aggregate metrics for intersecting parcels.
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE intersection_aggregate_metrics AS
    SELECT
        p.parcelPTID,
        -- Count unique encumbrances that directly intersect the parcel
        COUNT(DISTINCT IF(ST_INTERSECTS(p.geom, r.geom), r.%s, NULL)) AS intersect_impact_count,
        -- Count unique encumbrances within the 'very high' buffer
        COUNT(DISTINCT IF(ST_INTERSECTS(p.geom, r.buf_very_high), r.%s, NULL)) AS very_high_impact_count,
        -- Sum the total area of direct intersection for each parcel
        LEAST(
          IFNULL(
            ROUND(
              SAFE_DIVIDE(
                SUM(IF(ST_INTERSECTS(p.geom, r.geom), ST_AREA(ST_INTERSECTION(p.geom, r.geom)), 0)),
                ANY_VALUE(p.parcel_area)
              ) * 100, 4),
            0.0),
          100.0
        ) AS intersect_area_perc,
        -- Sum the total area of intersection with the 'very high' buffer for each parcel
        LEAST(
          IFNULL(
            ROUND(
              SAFE_DIVIDE(
                SUM(ST_AREA(ST_INTERSECTION(p.geom, r.buf_very_high))),
                ANY_VALUE(p.parcel_area)
              ) * 100, 4),
            0.0),
          100.0
        ) AS very_high_area_perc
    FROM parcels_in_scope AS p
    -- This join now uses the dynamically created buf_max column.
    JOIN encumbrance_in_scope AS r ON ST_DWithin(p.geom, r.geom, %d)
    GROUP BY p.parcelPTID;
  """, 
  encumbrance_id_col,
  encumbrance_id_col,
  max_buffer_meters);

  -- Step 4: Resolve all matches in a single, set-based query.
  EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TEMP TABLE resolved_matches AS
    WITH all_possible_matches AS (
      -- This CTE finds all potential parcel-encumbrance pairs within the max distance.
      SELECT
        p.parcelPTID,
        CAST(r.%s AS STRING) AS encumbrance_id,
        p.geom AS parcel_geom,
        p.centroid AS parcel_centroid,
        r.geom AS encumbrance_geom
      FROM parcels_in_scope AS p
      JOIN encumbrance_in_scope AS r ON ST_DWithin(p.geom, r.geom, %d)
    ),
    ranked_matches AS (
      -- This CTE calculates the raw intersection status, area, and distances for ranking.
      SELECT
        parcelPTID,
        encumbrance_id,
        ST_INTERSECTS(parcel_geom, encumbrance_geom) AS is_intersecting,
        -- USER CHANGE: Calculate intersection area to use for ranking.
        ROUND(IF(ST_INTERSECTS(parcel_geom, encumbrance_geom), ST_AREA(ST_INTERSECTION(parcel_geom, encumbrance_geom)), 0), 2) AS intersection_area,
        ROUND(ST_DISTANCE(parcel_geom, encumbrance_geom), 2) AS shortest_distance,
        ROUND(ST_DISTANCE(parcel_centroid, encumbrance_geom), 2) AS centroid_distance
      FROM all_possible_matches
    )
    -- This QUALIFY clause cleanly selects the single best encumbrance for each parcel.
    SELECT * EXCEPT (intersection_area) -- Exclude area after ranking
    FROM ranked_matches
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY parcelPTID
      -- USER CHANGE: The ranking logic now prioritizes intersections, then largest area, then closest distance.
      ORDER BY is_intersecting DESC, intersection_area DESC, shortest_distance ASC
    ) = 1
  """, encumbrance_id_col, max_buffer_meters);

  -- Step 5: Create the final result set by joining all pieces together.
  CREATE OR REPLACE TEMP TABLE final_results AS
  SELECT
    p.parcelPTID,
    p.clip,
    p.sourcedFips,
    -- Apply labeling logic here using a clean CASE statement.
    CASE
      WHEN r.is_intersecting THEN 'intersects'
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'very high') THEN 'very high'
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'high') THEN 'high'
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'medium') THEN 'medium'
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'low') THEN 'low'
      ELSE 'no encumbrance'
    END AS proximity_label,
    
    CASE
      WHEN r.is_intersecting THEN 0 -- Per original logic for intersects
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'very high') THEN (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'very high')
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'high') THEN (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'high')
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'medium') THEN (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'medium')
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'low') THEN (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'low')
      ELSE NULL
    END AS buffer_meters,
    
    IF(r.is_intersecting, 1, 0) AS intersect_status,
    r.shortest_distance,
    r.centroid_distance,
    -- Join the pre-calculated aggregate metrics. Use COALESCE to handle non-intersecting parcels.
    COALESCE(agg.intersect_impact_count, 0) AS intersect_impact_count,
    COALESCE(agg.intersect_area_perc, 0) AS intersect_area_perc,
    -- FIX: Add the missing very_high metrics to the final result set.
    COALESCE(agg.very_high_impact_count, 0) AS very_high_impact_count,
    COALESCE(agg.very_high_area_perc, 0) AS very_high_area_perc,
    r.encumbrance_id,
    p.geom AS geometry
  FROM parcels_in_scope AS p
  LEFT JOIN resolved_matches AS r ON p.parcelPTID = r.parcelPTID
  LEFT JOIN intersection_aggregate_metrics AS agg ON p.parcelPTID = agg.parcelPTID;

  -- Step 6: Atomically replace the final table with the new results.
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `%s`
    CLUSTER BY sourcedFips, geometry AS
    SELECT * FROM final_results;
  """, final_table_name);

END;

-- Procedure call
CALL proximity_parcels.calculate_proximity_score_polygons_batch('protected_lands_national','ID'); -- 1 hour 
CALL proximity_parcels.calculate_proximity_score_polygons_batch('wetlands','NWI_ID'); -- 7 hours 