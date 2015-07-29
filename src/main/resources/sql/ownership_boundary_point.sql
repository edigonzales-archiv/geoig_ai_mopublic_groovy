--DROP FUNCTION ownership_boundary_point(text, integer);
CREATE OR REPLACE FUNCTION ownership_boundary_point(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
WITH boundary_point AS (
SELECT nextval('''||dbschema||'.t_id'') AS t_id, a.gueltigkeit, b.lagegen, b.lagezuv, b.punktzeichen,
 CASE
   WHEN a.gueltigereintrag IS NULL THEN a.datum1
   ELSE a.gueltigereintrag
 END AS stand_am,
 b.gem_bfs, b.geometrie
FROM av_avdpool_ng.liegenschaften_lsnachfuehrung as a, av_avdpool_ng.liegenschaften_grenzpunkt as b
WHERE a.gem_bfs = '||bfsnr||'
AND b.gem_bfs = '||bfsnr||'
AND b.entstehung = a.t_id
)

INSERT INTO av_mopublic_export.ownership_boundary_point (t_id, validity, plan_accuracy, plan_reliability, mark, state_of, fosnr, geometry)
SELECT t_id, gueltigkeit, lagegen, lagezuv, punktzeichen, stand_am::timestamp without time zone, gem_bfs, geometrie
FROM boundary_point;
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
