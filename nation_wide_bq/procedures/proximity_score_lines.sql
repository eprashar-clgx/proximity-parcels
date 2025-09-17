-- Procedure to calculate proximity scores for parcels relative to linear encumbrances (roads, railways, transmission lines)
-- This version processes all data in a single batch without FIPS filtering
CREATE OR REPLACE PROCEDURE `clgx-gis-app-uat-a0e0.proximity_parcels.calculate_proximity_score_lines_batch`(encumbrance_table STRING, encumbrance_id_col STRING)
BEGIN
  -- Constants
  DECLARE final_table_name STRING;
  DECLARE buffer_tiers ARRAY<STRUCT<buffer_meters INT64, label STRING>>;

  SET final_table_name = FORMAT("proximity_parcels.proximity_intersection_%s", encumbrance_table);

  -- Define buffer tiers based on the encumbrance type
  --IF encumbrance_table = 'roadways' THEN
  --  SET buffer_tiers = [
  --    STRUCT(5 AS buffer_meters, 'intersects' AS label),
  --    STRUCT(10, 'very high'), 
  --    STRUCT(25, 'high'),
  --    STRUCT(50, 'medium'), 
  --    STRUCT(100, 'low')
  --  ];
  --ELSE
    SET buffer_tiers = [
      STRUCT(5, 'intersects'),
      STRUCT(150, 'very high'), 
      STRUCT(300, 'high'),
      STRUCT(750, 'medium'), 
      STRUCT(1000, 'low')
    ];
  --END IF;

  -- Step 1: Get all encumbrance geometries.
  -- The WHERE clause with fips has been removed to process all data in a single batch
  EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TEMP TABLE encumbrance_buffered AS
  SELECT *
  FROM `proximity_parcels.%s_mv`
  """, encumbrance_table);

  -- Step 2: Prepare all parcels data.
  -- The WHERE clause has been removed.
  CREATE OR REPLACE TEMP TABLE parcels_in_scope AS
  SELECT 
    parcelPTID, 
    clip, 
    sourcedFips, 
    geom, 
    ST_CENTROID(geom) AS centroid
  FROM `clgx-gis-app-uat-a0e0.proximity_parcels.parcels_mv`;
  
  -- Step 3: Pre-calculate aggregate impact metrics for each parcel.
  -- This logic scales perfectly to a full-table batch.
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE parcel_aggregate_metrics AS
    SELECT
        p.parcelPTID,
        COUNT(DISTINCT IF(ST_INTERSECTS(p.geom, r.buf_intersects), r.%s, NULL)) AS intersect_impact_count,
        COUNT(DISTINCT IF(ST_INTERSECTS(p.geom, r.buf_very_high), r.%s, NULL)) AS very_high_impact_count,
        LEAST(
          IFNULL(
            ROUND(
              SAFE_DIVIDE(
                SUM(ST_AREA(ST_INTERSECTION(p.geom, r.buf_very_high))),
                ANY_VALUE(ST_AREA(p.geom))
              ) * 100, 4),
            0.0),
          100.0
        ) AS very_high_area_perc
    FROM parcels_in_scope AS p
    JOIN encumbrance_buffered AS r ON ST_INTERSECTS(p.geom, r.buf_max)
    GROUP BY p.parcelPTID;
  """, encumbrance_id_col, encumbrance_id_col);

  -- Step 4: Resolve all matches in a single, set-based query.
  -- This logic also scales perfectly.
  EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TEMP TABLE resolved_matches AS
    WITH all_possible_matches AS (
      SELECT
        p.parcelPTID,
        CAST(r.%s AS STRING) AS encumbrance_id,
        p.geom AS parcel_geom,
        p.centroid AS parcel_centroid,
        r.geom AS encumbrance_geom,
        r.buf_intersects
      FROM parcels_in_scope AS p
      JOIN encumbrance_buffered AS r ON ST_INTERSECTS(p.geom, r.buf_max)
    ),
    ranked_matches AS (
      SELECT
        parcelPTID,
        encumbrance_id,
        ST_INTERSECTS(parcel_geom, buf_intersects) AS is_intersecting,
        ROUND(ST_DISTANCE(parcel_geom, encumbrance_geom), 2) AS shortest_distance,
        ROUND(ST_DISTANCE(parcel_centroid, encumbrance_geom), 2) AS centroid_distance,
        IF(ST_INTERSECTS(parcel_geom, buf_intersects),
           ROUND(ST_PERIMETER(ST_INTERSECTION(parcel_geom, buf_intersects)) / 2, 2),
           0
        ) AS len_inside
      FROM all_possible_matches
    )
    SELECT *
    FROM ranked_matches
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY parcelPTID
      ORDER BY is_intersecting DESC, shortest_distance ASC
    ) = 1
  """,
  encumbrance_id_col
  );

  -- Step 5: Create the final result set with simplified CASE statement labeling.
  CREATE OR REPLACE TEMP TABLE final_results AS
  SELECT
    p.parcelPTID,
    p.clip,
    p.sourcedFips,
    CASE
      WHEN r.is_intersecting THEN 'intersects'
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'very high') THEN 'very high'
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'high') THEN 'high'
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'medium') THEN 'medium'
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'low') THEN 'low'
      ELSE 'no encumbrance'
    END AS proximity_label,
    
    CASE
      WHEN r.is_intersecting THEN (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'intersects')
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'very high') THEN (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'very high')
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'high') THEN (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'high')
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'medium') THEN (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'medium')
      WHEN r.shortest_distance <= (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'low') THEN (SELECT buffer_meters FROM UNNEST(buffer_tiers) WHERE label = 'low')
      ELSE NULL
    END AS buffer_meters,
    
    IF(r.is_intersecting, 1, 0) AS intersect_status,
    r.shortest_distance,
    r.centroid_distance,
    agg.intersect_impact_count,
    r.len_inside,
    agg.very_high_area_perc,
    agg.very_high_impact_count,
    r.encumbrance_id,
    p.geom AS geometry
  FROM parcels_in_scope AS p
  LEFT JOIN resolved_matches AS r ON p.parcelPTID = r.parcelPTID
  LEFT JOIN parcel_aggregate_metrics AS agg ON p.parcelPTID = agg.parcelPTID;

  -- Step 6: Atomically replace the final table with the new results.
  -- This single statement replaces the DELETE/INSERT pattern and avoids DML quota issues.
  -- It also preserves the clustering of the destination table.
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `%s`
    CLUSTER BY sourcedFips, geometry AS
    SELECT * FROM final_results;
  """, final_table_name);

END;

-- Example procedure call
CALL proximity_parcels.calculate_proximity_score_lines_batch('roadways','ID');
CALL proximity_parcels.calculate_proximity_score_lines_batch('railways', 'FRAARCID');
CALL proximity_parcels.calculate_proximity_score_lines_batch('transmission_lines','ID');
