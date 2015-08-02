--DROP FUNCTION update_mopublic(text, integer);
CREATE OR REPLACE FUNCTION update_mopublic(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
SELECT '||dbschema||'.sequence();
SELECT '||dbschema||'.metadata_metadata('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.control_points_control_point('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.land_cover_lcsurface('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.land_cover_lcsurfaceproj('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.single_objects_surface_element('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.single_objects_linear_element('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.single_objects_point_element('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.local_names_names('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.ownership_boundary_point('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.ownership_realestate('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.ownership_realestateproj('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.ownership_dpr_mine('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.ownership_dpr_mineproj('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.pipelines_linear_element('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.territorial_boundaries_boundary_terr_point('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.territorial_boundaries_municipal_boundary('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.territorial_boundaries_municipal_boundproj('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.territorial_boundaries_other_territ_boundary('''||dbschema||''', '||bfsnr||');
SELECT '||dbschema||'.building_addresses('''||dbschema||''', '||bfsnr||');
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
