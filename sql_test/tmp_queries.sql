



WITH linear_element AS (
SELECT nextval('av_mopublic_export.t_id') as t_id, a.t_id as obj_t_id, a.betreiber, a.art, b.geometrie as geometrie, c.gueltigkeit, 
 CASE 
   WHEN c.gueltigereintrag IS NULL THEN c.datum1
   ELSE c.gueltigereintrag
 END AS stand_am, 
 b.gem_bfs
FROM av_avdpool_ng_arcs.rohrleitungen_leitungsobjekt as a, av_avdpool_ng_arcs.rohrleitungen_linienelement as b, av_avdpool_ng_arcs.rohrleitungen_rlnachfuehrung as c
WHERE a.gem_bfs = 2583 
AND b.gem_bfs = 2583 
AND c.gem_bfs = 2583 
AND b.linienelement_von = a.t_id
AND a.entstehung = c.t_id
),

foo AS (
INSERT INTO av_mopublic_export.pipelines_linear_element (t_id, operating_company, fluid, validity, state_of, fosnr, geometry)
SELECT t_id, betreiber, art, gueltigkeit, stand_am::timestamp without time zone, gem_bfs, geometrie
FROM linear_element
),

linear_elementposname AS (
SELECT DISTINCT ON (d.t_id) nextval('av_mopublic_export.t_id') as t_id, c.betreiber,
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
 FROM  av_avdpool_ng_arcs.rohrleitungen_leitungsobjekt as a, linear_element as b
 WHERE a.gem_bfs = 2583 
 AND b.gem_bfs = 2583 
 AND a.t_id = b.obj_t_id
) as c, av_avdpool_ng_arcs.rohrleitungen_leitungsobjektpos as d
WHERE d.leitungsobjektpos_von = c.obj_t_id
)

INSERT INTO av_mopublic_export.pipelines_linear_element_posname (t_id, operating_company, ori, hali, vali, fosnr, posname_of, pos)
SELECT t_id, betreiber, ori, hali, vali, gem_bfs, posname_of, pos
FROM linear_elementposname;


linear_element_posname AS (

SELECT DISTINCT ON (b.t_id) nextval('av_mopublic_export.t_id') as t_id, a.betreiber, 
FROM
(
 SELECT DISTINCT b.linienelement_von, b.t_id, a.betreiber, a.gem_bfs
 FROM  av_avdpool_ng_arcs.rohrleitungen_leitungsobjekt as a, linear_element as b
 --WHERE a.gem_bfs = 2583 
 --AND b.gem_bfs = 2583 
 WHERE a.t_id = b.obj_t_id
) as c, linear_element as d
WHERE c
)


﻿SELECT DISTINCT ON (b.ogc_fid) b.tid as tid, a.tid as linienelementnamepos_von, a.betreiber as betreiber, b.pos as pos, b.ori as ori, b.hali, b.vali, b.gem_bfs as bfsnr,  ST_X(pos) AS y, ST_Y(pos) AS x, (100::double precision - ori) * 0.9::double precision AS rot, hali_txt, vali_txt  
FROM
  (
    SELECT DISTINCT b.linienelement_von, b.tid, a.betreiber, a.gem_bfs
    FROM  av_avdpool_ch.rohrleitungen_leitungsobjekt as a, av_avdpool_ch.rohrleitungen_linienelement as b
    WHERE a.gem_bfs = ? AND b.gem_bfs = ? 
    AND a.tid = b.linienelement_von
  ) AS a, av_avdpool_ch.rohrleitungen_leitungsobjektpos as b
WHERE a.gem_bfs = ? AND b.gem_bfs = ?
AND a.linienelement_von = b.leitungsobjektpos_von;


WITH dpr AS (
SELECT nextval('av_mopublic_export.t_id') as t_id, a.t_id as dpr_t_id, a.nbident, a.nummer, a.egris_egrid, a.vollstaendigkeit, (a.art-1) as art, b.flaechenmass,
 CASE 
   WHEN d.gueltigereintrag IS NULL THEN d.datum1
   ELSE d.gueltigereintrag
 END AS stand_am, 
 b.gem_bfs, b.geometrie
FROM av_avdpool_ng.liegenschaften_lsnachfuehrung as d, av_avdpool_ng.liegenschaften_grundstueck as a, av_avdpool_ng.liegenschaften_selbstrecht as b
WHERE a.gem_bfs = 2583 
AND b.gem_bfs = 2583 
AND d.gem_bfs = 2583
AND b.selbstrecht_von = a.t_id
AND a.entstehung = d.t_id 
),

foo AS (
INSERT INTO av_mopublic_export.ownership_dpr_mine (t_id, identnd, anumber, egris_egrid, completeness, realestate_type, area, state_of, fosnr, geometry)
SELECT t_id, nbident, nummer, egris_egrid, vollstaendigkeit, art, flaechenmass, stand_am::timestamp without time zone, gem_bfs, geometrie
FROM dpr
),

dpr_posnumber AS (
SELECT nextval('av_mopublic_export.t_id') as t_id, a.nbident, a.nummer,
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
 a.gem_bfs, a.t_id as posnumber_of, b.pos
FROM dpr as a, av_avdpool_ng.liegenschaften_grundstueckpos as b
WHERE a.gem_bfs = 2583
AND b.gem_bfs = 2583
AND a.dpr_t_id = b.grundstueckpos_von
)

INSERT INTO av_mopublic_export.ownership_dpr_mine_posnumber (t_id, identnd, anumber, ori, hali, vali, fosnr, posnumber_of, pos)
SELECT t_id, nbident, nummer, ori, hali, vali, gem_bfs, posnumber_of, pos
FROM dpr_posnumber;
