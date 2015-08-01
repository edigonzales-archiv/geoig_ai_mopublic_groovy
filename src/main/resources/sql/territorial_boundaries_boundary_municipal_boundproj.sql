--DROP FUNCTION territorial_boundaries_municipal_boundproj(text, integer);
CREATE OR REPLACE FUNCTION territorial_boundaries_municipal_boundproj(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
INSERT INTO '||dbschema||'.territorial_boundaries_municipal_boundproj (t_id, name, state_of, fosnr, geometry)
SELECT nextval('''||dbschema||'.t_id'') as t_id, a.name as "name",
 CASE
   WHEN c.gueltigereintrag IS NULL THEN c.datum1::timestamp without time zone
   ELSE c.gueltigereintrag::timestamp without time zone
 END AS stand_am,
 b.gem_bfs as bfsnr, b.geometrie as geometrie
FROM av_avdpool_ng.gemeindegrenzen_gemeinde as a, av_avdpool_ng.gemeindegrenzen_projgemeindegrenze as b, av_avdpool_ng.gemeindegrenzen_gemnachfuehrung as c
WHERE a.gem_bfs = '||bfsnr||'
AND b.gem_bfs = '||bfsnr||'
AND c.gem_bfs = '||bfsnr||'
AND b.projgemeindegrenze_von = a.t_id
AND c.t_id = b.entstehung;
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
