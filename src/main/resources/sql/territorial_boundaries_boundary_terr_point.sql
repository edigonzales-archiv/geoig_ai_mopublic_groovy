--DROP FUNCTION territorial_boundaries_boundary_terr_point(text, integer);
CREATE OR REPLACE FUNCTION territorial_boundaries_boundary_terr_point(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
INSERT INTO '||dbschema||'.territorial_boundaries_boundary_terr_point (t_id, validity, plan_accuracy, plan_reliability, mark, state_of, fosnr, geometry)
SELECT nextval('''||dbschema||'.t_id'') as t_id, a.gueltigkeit, b.lagegen, b.lagezuv, b.punktzeichen,
 CASE
   WHEN a.gueltigereintrag IS NULL THEN a.datum1::timestamp without time zone
   ELSE a.gueltigereintrag::timestamp without time zone
 END AS stand_am,
 b.gem_bfs, b.geometrie
FROM av_avdpool_ng.gemeindegrenzen_gemnachfuehrung as a, av_avdpool_ng.gemeindegrenzen_hoheitsgrenzpunkt as b
WHERE a.gem_bfs = '||bfsnr||'
AND b.gem_bfs = '||bfsnr||'
AND b.entstehung = a.t_id;
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
