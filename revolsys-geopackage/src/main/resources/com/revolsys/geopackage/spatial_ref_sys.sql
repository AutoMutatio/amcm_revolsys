CREATE VIEW spatial_ref_sys AS
  SELECT
    srs_id AS srid,
    organization AS auth_name,
    organization_coordsys_id AS auth_srid,
    definition AS srtext
  FROM gpkg_spatial_ref_sys;
