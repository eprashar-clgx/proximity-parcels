-- Procedure to consolidate all encumbrance scores into a single table
-- This version processes all data in a single batch without FIPS filtering
-- Last updated on Sep 16, 2025 with alias names that match the final schema shared with Ricardo's team

CREATE OR REPLACE PROCEDURE `clgx-gis-app-prd-364d.proximity_parcels.consolidate_all_scores`()
BEGIN

  CREATE OR REPLACE TABLE `clgx-gis-app-prd-364d.proximity_parcels.all_encumbrance_scores`
  -- Best Practice: Cluster by both fips and geometry for optimal performance in downstream queries.
  CLUSTER BY sourcedFips, geometry AS

  -- The query starts from the main parcels table (p) to ensure every parcel is included.
  -- All other score tables are then LEFT JOINed to it.
  SELECT 
    p.parcelPTID AS spatial_parcel_point_id,
    p.clip,
    p.state, 
    p.stateCode AS state_code,
    p.countyCode AS cnty_code,
    CONCAT(p.stateCode,p.countyCode) AS fips_code,
    p.sourcedFips,
    p.clipOwner1Name AS parcel_poly_owner1, -- this is not used in the final schema for Ricardo's team
    p.geom AS geometry, -- Use the original geometry from the master parcels table. -- this is not used in the final schema for Ricardo's team

   -- Railways
    CASE 
      WHEN p1.proximity_label IN ('no encumbrance') THEN 'beyond threshold'
      ELSE p1.proximity_label END AS rail_proximity_lbl,
    p1.intersect_status AS rail_intersect_status,
    p1.shortest_distance AS rail_shortest_dist,
    p1.centroid_distance AS rail_dist_centroid,
    p1.intersect_impact_count AS rail_num_direct_intersections,
    p1.len_inside AS rail_line_length,
    ROUND(p1.very_high_area_perc,2) AS rail_perc_area_with_adj_lines,
    p1.very_high_impact_count AS rail_num_adj_intersections,
    p1.encumbrance_id AS rail_nearest_id,
    l1.KM AS rail_length, -- Fetched from railways_mv

    -- Roadways
    CASE 
      WHEN p2.proximity_label IN ('no encumbrance') THEN 'beyond threshold'
      ELSE p2.proximity_label END AS road_proximity_lbl,
    p2.intersect_status AS road_intersect_status,
    p2.shortest_distance AS road_shortest_dist,
    p2.centroid_distance AS road_dist_centroid,
    p2.intersect_impact_count AS road_num_direct_intersections,
    p2.len_inside AS road_line_length,
    ROUND(p2.very_high_area_perc, 2) AS road_perc_area_with_adj_roads,
    p2.very_high_impact_count AS road_num_adj_intersections,
    p2.encumbrance_id AS road_nearest_id,
    l2.ROADNAME AS road_name, -- Fetched from roadways_mv

    -- Transmission Lines
    CASE 
      WHEN p3.proximity_label IN ('no encumbrance') THEN 'beyond threshold'
      ELSE p3.proximity_label END AS tline_proximity_lbl,
    p3.intersect_status AS tline_intersect_status,
    p3.shortest_distance AS tline_shortest_dist,
    p3.centroid_distance AS tline_dist_centroid,
    p3.intersect_impact_count AS tline_num_direct_intersections,
    p3.len_inside AS tline_line_length,
    ROUND(p3.very_high_area_perc, 2) AS tline_perc_area_with_adj_lines,
    p3.very_high_impact_count AS tline_num_adj_intersections,
    p3.encumbrance_id AS tline_nearest_id,
    l3.VOLT_CLASS AS tline_volt_class, -- Fetched from transmission_lines_mv

    -- Protected lands
    CASE 
      WHEN p4.proximity_label IN ('no encumbrance') THEN 'beyond threshold'
      ELSE p4.proximity_label END AS prot_area_proximity_lbl,
    p4.intersect_status AS prot_area_intersect_status,
    p4.shortest_distance AS prot_area_shortest_dist,
    ROUND(p4.intersect_area_perc,2) AS prot_area_area_intersect,
    p4.centroid_distance AS prot_area_dist_centroid,
    p4.intersect_impact_count AS prot_area_num_direct_intersections,
    ROUND(p4.very_high_area_perc, 2) AS prot_area_perc_area_with_adj_areas,
    p4.very_high_impact_count AS prot_area_num_adj_intersections,
    p4.encumbrance_id AS prot_area_nearest_id,
    p4.intersection_score AS prot_area_intersection_score,
    p4.intersection_label AS prot_area_intersection_label,
    l4.MngTp_Desc AS prot_land_mng_type, -- Fetched from protected_lands_national_mv
    
    -- Wetlands
    CASE 
      WHEN p5.proximity_label IN ('no encumbrance') THEN 'beyond threshold'
      ELSE p5.proximity_label END AS wetland_proximity_lbl,
    p5.intersect_status AS wetland_intersect_status,
    p5.shortest_distance AS wetland_shortest_dist,
    ROUND(p5.intersect_area_perc,2) AS wetland_area_intersect,
    p5.centroid_distance AS wetland_dist_centroid,
    p5.intersect_impact_count AS wetland_num_direct_intersections,
    ROUND(p5.very_high_area_perc, 2) AS wetland_perc_area_with_adj_lands,
    p5.very_high_impact_count AS wetland_num_adj_intersections,
    p5.encumbrance_id AS wetland_nearest_id,
    p5.intersection_score AS wetland_intersection_score,
    p5.intersection_label AS wetland_intersection_label,
    l5.WETLAND_TYPE AS wetland_type -- Fetched from wetlands_mv
    
  FROM `clgx-gis-app-prd-364d.proximity_parcels.parcels_mv` AS p

  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.proximity_intersection_railways` p1
    ON p.parcelPTID = p1.parcelPTID

  -- FIX: Cast the ID column to STRING to match the type of p1.encumbrance_id
  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.railways_mv` l1
    ON p1.encumbrance_id = CAST(l1.FRAARCID AS STRING)

  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.proximity_intersection_roadways` p2
    ON p.parcelPTID = p2.parcelPTID
  -- FIX: Cast the ID column to STRING to match the type of p2.encumbrance_id
  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.roadways_mv` l2
    ON p2.encumbrance_id = CAST(l2.ID AS STRING)

  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.proximity_intersection_transmission_lines` p3
    ON p.parcelPTID = p3.parcelPTID
  -- FIX: Cast the ID column to STRING to match the type of p3.encumbrance_id
  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.transmission_lines_mv` l3
    ON p3.encumbrance_id = CAST(l3.ID AS STRING)

  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.proximity_intersection_protected_lands_national` p4
    ON p.parcelPTID = p4.parcelPTID
  -- This join is likely okay since UUIDs are strings, but casting is a safe practice.
  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.protected_lands_national_mv` l4
    ON p4.encumbrance_id = CAST(l4.ID AS STRING)

  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.proximity_intersection_wetlands` p5
    ON p.parcelPTID = p5.parcelPTID
  -- FIX: Cast the ID column to STRING to match the type of p5.encumbrance_id
  LEFT JOIN `clgx-gis-app-prd-364d.proximity_parcels.wetlands_mv` l5
    ON p5.encumbrance_id = CAST(l5.NWI_ID AS STRING)
  
  WHERE ST_GEOMETRYTYPE(p.geom) NOT IN ('ST_Point', 'ST_MultiPoint');

END;

-- Procedure call
CALL proximity_parcels.consolidate_all_scores()