CREATE VIEW st_spatial_ref_sys AS
  SELECT
    srs_name,
    srs_id,
    organization,
    organization_coordsys_id,
    definition,
    description
  FROM gpkg_spatial_ref_sys;
