--DROP FUNCTION pipelines_linear_element(text, integer);
CREATE OR REPLACE FUNCTION pipelines_linear_element(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
-- Please check carefully!!!!
WITH linear_element AS (
SELECT nextval('''||dbschema||'.t_id'') as t_id, a.t_id as obj_t_id, a.betreiber, a.art, b.geometrie as geometrie, c.gueltigkeit,
 CASE
   WHEN c.gueltigereintrag IS NULL THEN c.datum1
   ELSE c.gueltigereintrag
 END AS stand_am,
 b.gem_bfs
FROM av_avdpool_ng.rohrleitungen_leitungsobjekt as a, av_avdpool_ng.rohrleitungen_linienelement as b, av_avdpool_ng.rohrleitungen_rlnachfuehrung as c
WHERE a.gem_bfs = '||bfsnr||'
AND b.gem_bfs = '||bfsnr||'
AND c.gem_bfs = '||bfsnr||'
AND b.linienelement_von = a.t_id
AND a.entstehung = c.t_id
),

foo AS (
INSERT INTO '||dbschema||'.pipelines_linear_element (t_id, operating_company, fluid, validity, state_of, fosnr, geometry)
SELECT t_id, betreiber, art, gueltigkeit, stand_am::timestamp without time zone, gem_bfs, geometrie
FROM linear_element
),

linear_elementposname AS (
SELECT DISTINCT ON (d.t_id) nextval('''||dbschema||'.t_id'') as t_id, c.betreiber,
 CASE
  WHEN d.ori IS NULL THEN 100::double precision
  ELSE d.ori
 END AS ori,
 CASE
  WHEN d.hali IS NULL THEN 1
  ELSE d.hali
 END as hali,
 CASE
  WHEN d.vali IS NULL THEN 2
  ELSE d.vali
 END as vali,
 c.gem_bfs, c.t_id AS posname_of, d.pos
FROM
(
 SELECT DISTINCT b.t_id, b.obj_t_id, a.betreiber, a.gem_bfs
 FROM  av_avdpool_ng.rohrleitungen_leitungsobjekt as a, linear_element as b
 WHERE a.gem_bfs = '||bfsnr||'
 AND b.gem_bfs = '||bfsnr||'
 AND a.t_id = b.obj_t_id
) as c, av_avdpool_ng.rohrleitungen_leitungsobjektpos as d
WHERE d.leitungsobjektpos_von = c.obj_t_id
)

INSERT INTO '||dbschema||'.pipelines_linear_element_posname (t_id, operating_company, ori, hali, vali, fosnr, posname_of, pos)
SELECT t_id, betreiber, ori, hali, vali, gem_bfs, posname_of, pos
FROM linear_elementposname;


';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
