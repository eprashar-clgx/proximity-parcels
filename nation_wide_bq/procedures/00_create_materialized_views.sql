-- Create Materialized Views for Proximity Parcels
-- PARCELS should be extracted from production project

CREATE OR REPLACE PROCEDURE `clgx-gis-app-dev-06e3.proximity_parcels.create_materialized_view`(view_name STRING)
BEGIN

  -- Create the master materialized view for parcels
  IF view_name = 'parcels' THEN
    CREATE OR REPLACE MATERIALIZED VIEW `clgx-gis-app-dev-06e3.proximity_parcels.parcels_mv`
    CLUSTER BY sourcedFips, geom AS
    SELECT
      * EXCEPT(geometry),
      ST_SIMPLIFY(geometry, 1) AS geom
    FROM `clgx-idap-bigquery-dev-71f0.edr_ent_property_parcel_polygons.property_parcelpolygon`
    WHERE ST_GEOMETRYTYPE(geometry) NOT IN ('ST_Point', 'ST_MultiPoint');

  -- Create the materialized view for wetlands with conditional buffering
  ELSEIF view_name = 'wetlands' THEN
    CREATE OR REPLACE MATERIALIZED VIEW `clgx-gis-app-dev-06e3.proximity_parcels.wetlands_mv`
    CLUSTER BY fips, geom AS
    SELECT
      * EXCEPT(geometry),
      ST_SIMPLIFY(geometry, 1) AS geom,
      -- Conditional buffering based on vertex count to avoid resource exhaustion.
      CASE
        WHEN ST_NUMPOINTS(geometry) < 50000 THEN ST_BUFFER(ST_SIMPLIFY(geometry, 1), 5)
        ELSE ST_SIMPLIFY(geometry, 1)
      END AS buf_intersects,
      CASE
        WHEN ST_NUMPOINTS(geometry) < 50000 THEN ST_BUFFER(ST_SIMPLIFY(geometry, 1), 10)
        ELSE ST_SIMPLIFY(geometry, 1)
      END AS buf_very_high
    FROM `clgx-gis-app-dev-06e3.proximity_parcels.wetlands`;

  -- Create the materialized view for protected lands with conditional buffering
  ELSEIF view_name = 'protected_lands_national' THEN
    CREATE OR REPLACE MATERIALIZED VIEW `clgx-gis-app-dev-06e3.proximity_parcels.protected_lands_national_mv`
    CLUSTER BY fips, geom AS
    SELECT
      * EXCEPT(geometry),
      ST_SIMPLIFY(geometry, 1) AS geom,
      -- Conditional buffering based on vertex count.
      CASE
        WHEN ST_NUMPOINTS(geometry) < 50000 THEN ST_BUFFER(ST_SIMPLIFY(geometry, 1), 5)
        ELSE ST_SIMPLIFY(geometry, 1)
      END AS buf_intersects,
      CASE
        WHEN ST_NUMPOINTS(geometry) < 50000 THEN ST_BUFFER(ST_SIMPLIFY(geometry, 1), 10)
        ELSE ST_SIMPLIFY(geometry, 1)
      END AS buf_very_high
    FROM `clgx-gis-app-dev-06e3.proximity_parcels.protected_lands_national`;

  -- Create the materialized view for railways
  ELSEIF view_name = 'railways' THEN
    CREATE OR REPLACE MATERIALIZED VIEW `clgx-gis-app-dev-06e3.proximity_parcels.railways_mv`
    CLUSTER BY fips, geom AS
    SELECT
      * EXCEPT(geometry),
      ST_SIMPLIFY(geometry, 1) AS geom,
      ST_BUFFER(ST_SIMPLIFY(geometry, 1), 5) AS buf_intersects,
      ST_BUFFER(ST_SIMPLIFY(geometry, 1), 150) AS buf_very_high,
      ST_BUFFER(ST_SIMPLIFY(geometry, 1), 1000) AS buf_max
    FROM `clgx-gis-app-dev-06e3.proximity_parcels.railways`;

  -- Create the materialized view for transmission lines
  ELSEIF view_name = 'transmission_lines' THEN
    CREATE OR REPLACE MATERIALIZED VIEW `clgx-gis-app-dev-06e3.proximity_parcels.transmission_lines_mv`
    CLUSTER BY fips, geom AS
    SELECT
      * EXCEPT(geometry),
      ST_SIMPLIFY(geometry, 1) AS geom,
      ST_BUFFER(ST_SIMPLIFY(geometry, 1), 5) AS buf_intersects,
      ST_BUFFER(ST_SIMPLIFY(geometry, 1), 150) AS buf_very_high,
      ST_BUFFER(ST_SIMPLIFY(geometry, 1), 1000) AS buf_max
    FROM `clgx-gis-app-dev-06e3.proximity_parcels.transmission_lines`;

  -- Create the materialized view for roadways
  ELSEIF view_name = 'roadways' THEN
    CREATE OR REPLACE MATERIALIZED VIEW `clgx-gis-app-dev-06e3.proximity_parcels.roadways_mv`
    CLUSTER BY fips, geom AS
    SELECT
      * EXCEPT(geometry),
      ST_SIMPLIFY(geometry, 1) AS geom,
      ST_BUFFER(ST_SIMPLIFY(geometry, 1), 5) AS buf_intersects,
      ST_BUFFER(ST_SIMPLIFY(geometry, 1), 150) AS buf_very_high,
      ST_BUFFER(ST_SIMPLIFY(geometry, 1), 1000) AS buf_max
    FROM `clgx-gis-app-dev-06e3.proximity_parcels.roadways`;

  ELSE
    -- Raise an error if an unknown view name is provided.
    RAISE USING MESSAGE = FORMAT("Unknown or unsupported view name: '%s'", view_name);
  END IF;

END;

-- Procedure calls
CALL proximity_parcels.create_materialized_view('parcels');
CALL proximity_parcels.create_materialized_view('roadways');
CALL proximity_parcels.create_materialized_view('railways');
CALL proximity_parcels.create_materialized_view('transmission_lines');
CALL proximity_parcels.create_materialized_view('protected_lands_national');
CALL proximity_parcels.create_materialized_view('wetlands');