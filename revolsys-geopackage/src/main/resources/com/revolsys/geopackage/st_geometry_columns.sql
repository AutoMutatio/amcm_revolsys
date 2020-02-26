CREATE VIEW st_geometry_columns AS
  SELECT
    table_name,
    column_name,
    "ST_" || geometry_type_name,
    g.srs_id,
    srs_name
  FROM gpkg_geometry_columns as g JOIN gpkg_spatial_ref_sys AS s
  WHERE g.srs_id = s.srs_id;
