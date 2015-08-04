--DROP FUNCTION single_objects_point_element(text, integer);
CREATE OR REPLACE FUNCTION single_objects_point_element(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
WITH single_objects_point_element AS (
SELECT nextval('''||dbschema||'.t_id'') as t_id, b.t_id as eo_t_id, a.gueltigkeit, b.art,
 CASE
  WHEN b.qualitaet IS NULL THEN 0
  ELSE b.qualitaet
 END AS qualitaet,
 CASE
  WHEN a.gueltigereintrag IS NULL THEN a.datum1
  ELSE a.gueltigereintrag
 END AS stand_am,
 c.gem_bfs, c.geometrie
FROM av_avdpool_ng.einzelobjekte_eonachfuehrung as a, av_avdpool_ng.einzelobjekte_einzelobjekt as b, av_avdpool_ng.einzelobjekte_punktelement as c
WHERE a.gem_bfs = '||bfsnr||'
AND b.gem_bfs = '||bfsnr||'
AND c.gem_bfs = '||bfsnr||'
AND a.t_id = b.entstehung
AND b.t_id = c.punktelement_von
),

foo AS (
 INSERT INTO av_mopublic_export.single_objects_point_element (t_id, validity, type, quality, state_of, fosnr, geometry)
 SELECT t_id, gueltigkeit, art, qualitaet, stand_am::timestamp without time zone, gem_bfs, geometrie
 FROM single_objects_point_element
),

single_objects_point_element_postext AS (
 SELECT nextval('''||dbschema||'.t_id'') as t_id, 1::integer as typ, a.name,
  CASE
   WHEN b.ori IS NULL THEN 100::double precision
   ELSE b.ori
  END AS ori,
  CASE
   WHEN b.hali IS NULL THEN 1
   ELSE b.hali
  END as hali,
  CASE
   WHEN b.vali IS NULL THEN 2
   ELSE b.vali
  END as vali,
  b.gem_bfs, c.t_id as postext_of, b.pos
 FROM av_avdpool_ng.einzelobjekte_objektname as a, av_avdpool_ng.einzelobjekte_objektnamepos as b, single_objects_point_element as c
 WHERE a.gem_bfs = '||bfsnr||'
 AND b.gem_bfs = '||bfsnr||'
 AND a.t_id = b.objektnamepos_von
 AND a.objektname_von = c.eo_t_id
)

INSERT INTO '||dbschema||'.single_objects_point_element_postext (t_id, type, number_name, ori, hali, vali, fosnr, postext_of, pos)
SELECT t_id, typ, name, ori, hali, vali, gem_bfs, postext_of, pos
FROM single_objects_point_element_postext;
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
