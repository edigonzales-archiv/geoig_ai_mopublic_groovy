--DROP FUNCTION local_names_names(text, integer);
CREATE OR REPLACE FUNCTION local_names_names(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
WITH names AS (
 SELECT nextval('''||dbschema||'.t_id'') AS t_id, a.t_id as names_t_id, 0 as kategorie, name, geometrie, NULL::varchar as typ,
  CASE
   WHEN b.gueltigereintrag IS NULL THEN b.datum1
   ELSE b.gueltigereintrag
  END AS stand_am,
  a.gem_bfs
 FROM av_avdpool_ng.nomenklatur_flurname as a, av_avdpool_ng.nomenklatur_nknachfuehrung as b
 WHERE a.gem_bfs = '||bfsnr||' AND b.gem_bfs = '||bfsnr||'
 AND b.t_id = a.entstehung

UNION ALL

 SELECT nextval('''||dbschema||'.t_id'') AS t_id, a.t_id as names_t_id, 1 as kategorie, name, geometrie, a.typ,
  CASE
   WHEN c.gueltigereintrag IS NULL THEN c.datum1
   ELSE c.gueltigereintrag
  END AS stand_am,
  a.gem_bfs
 FROM av_avdpool_ng.nomenklatur_ortsname as a, av_avdpool_ng.nomenklatur_nknachfuehrung as c
 WHERE a.gem_bfs = '||bfsnr||' AND c.gem_bfs = '||bfsnr||'
 AND c.t_id = a.entstehung
),

foo AS (
INSERT INTO av_mopublic_export.local_names_names (t_id, category, name, type, state_of, fosnr, geometry)
SELECT t_id, kategorie, name, typ, stand_am, gem_bfs, geometrie
FROM names
),

names_posname AS (
 SELECT t_id, posname_of, kategorie, name, pos, ori, hali, vali, gem_bfs
 FROM
 (
  SELECT nextval('''||dbschema||'.t_id'') AS t_id, b.t_id as posname_of, 0 as kategorie, b.name, a.pos, CASE WHEN a.ori IS NULL THEN 100 ELSE a.ori END as ori, a.hali, a.vali, a.gem_bfs
  FROM av_avdpool_ng.nomenklatur_flurnamepos as a, names as b
  WHERE a.gem_bfs = '||bfsnr||' AND b.gem_bfs = '||bfsnr||'
  AND b.names_t_id = a.flurnamepos_von

  UNION ALL

  SELECT nextval('''||dbschema||'.t_id'') AS t_id, b.t_id as posname_of, 1 as kategorie, b.name, a.pos, CASE WHEN a.ori IS NULL THEN 100 ELSE a.ori END as ori, a.hali, a.vali, a.gem_bfs
  FROM av_avdpool_ng.nomenklatur_ortsnamepos as a, names as b
  WHERE a.gem_bfs = '||bfsnr||' AND b.gem_bfs = '||bfsnr||'
  AND b.names_t_id = a.ortsnamepos_von

  UNION ALL

  SELECT nextval('''||dbschema||'.t_id'') AS t_id, NULL::integer as posname_of, 2 as kategorie, b.name, a.pos, CASE WHEN a.ori IS NULL THEN 100 ELSE a.ori END as ori, a.hali, a.vali, a.gem_bfs
  FROM av_avdpool_ng.nomenklatur_gelaendenamepos as a, av_avdpool_ng.nomenklatur_gelaendename as b
  WHERE a.gem_bfs = '||bfsnr||' AND b.gem_bfs = '||bfsnr||' 
  AND b.t_id = a.gelaendenamepos_von
 ) as u
)

INSERT INTO av_mopublic_export.local_names_names_posname (t_id, category, name, ori, hali, vali, fosnr, posname_of, pos)
SELECT t_id, kategorie, name, ori, hali, vali, gem_bfs, posname_of, pos
FROM names_posname;
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
