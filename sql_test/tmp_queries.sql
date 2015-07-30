


﻿INSERT INTO av_mopublic.liegenschaften__selbstrecht_bergwerk (tid, nbident, nummer, egris_egrid, vollstaendigkeit, grundstuecksart, flaechenmass, geometrie, stand_am, bfsnr)
SELECT a.tid as tid, a.nbident as nbident, a.nummer as nummer, a.egris_egrid as egris_egrid, c.designation_d as vollstaendigkeit, e.designation_d as grundstuecksart, b.flaechenmass as flaechenmass, b.geometrie as geometrie, 
 CASE 
   WHEN d.gueltigereintrag IS NULL THEN to_date(d.datum1, 'YYYYMMDD')
   ELSE to_date(d.gueltigereintrag, 'YYYYMMDD')
 END AS stand_am, 
 b.gem_bfs as bfsnr
FROM av_avdpool_ch.liegenschaften_lsnachfuehrung as d, av_avdpool_ch.liegenschaften_grundstueck as a, av_avdpool_ch.liegenschaften_selbstrecht as b, av_mopublic_meta.completeness_type as c,av_mopublic_meta.realestate_type as e
WHERE a.gem_bfs = ? AND b.gem_bfs = ? AND d.gem_bfs = ? 
AND b.selbstrecht_von = a.tid
AND a.vollstaendigkeit = c.code
AND d.tid = a.entstehung
AND (a.art - 1) = e.code
   UNION ALL
SELECT a.tid as tid, a.nbident as nbident, a.nummer as nummer, a.egris_egrid as egris_egrid, c.designation_d as vollstaendigkeit, e.designation_d as grundstuecksart, b.flaechenmass as flaechenmass, b.geometrie as geometrie, 
 CASE 
   WHEN d.gueltigereintrag IS NULL THEN to_date(d.datum1, 'YYYYMMDD')
   ELSE to_date(d.gueltigereintrag, 'YYYYMMDD')
 END AS stand_am, 
 b.gem_bfs as bfsnr
FROM av_avdpool_ch.liegenschaften_lsnachfuehrung as d, av_avdpool_ch.liegenschaften_grundstueck as a, av_avdpool_ch.liegenschaften_bergwerk as b, av_mopublic_meta.completeness_type as c,av_mopublic_meta.realestate_type as e
WHERE a.gem_bfs = ? AND b.gem_bfs = ? AND d.gem_bfs = ? 
AND b.bergwerk_von = a.tid
AND a.vollstaendigkeit = c.code
AND d.tid = a.entstehung
AND (a.art - 1) = e.code;

WITH selbsrecht AS (
SELECT nextval('av_mopublic_export.t_id') as t_id, a.t_id as sdr_t_id, a.nbident, a.nummer, a.egris_egrid, a.vollstaendigkeit, b.flaechenmass,
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
)

-- Zuerst Selbstrecht komplett und erst dann Bauwerke.


/*
WITH realestateproj AS (
SELECT nextval('av_mopublic_export.t_id') as t_id, a.t_id as gs_t_id, a.nbident, a.nummer, a.egris_egrid, a.vollstaendigkeit, b.flaechenmass,
 CASE 
   WHEN d.gueltigereintrag IS NULL THEN d.datum1
   ELSE d.gueltigereintrag
 END AS stand_am, 
 b.gem_bfs, b.geometrie
FROM av_avdpool_ng.liegenschaften_lsnachfuehrung as d, av_avdpool_ng.liegenschaften_projgrundstueck as a, av_avdpool_ng.liegenschaften_projliegenschaft as b
WHERE a.gem_bfs = 2583 
AND b.gem_bfs = 2583 
AND d.gem_bfs = 2583
AND b.projliegenschaft_von = a.t_id
AND a.entstehung = d.t_id 
),

foo AS (
INSERT INTO av_mopublic_export.ownership_realestateproj (t_id, identnd, anumber, egris_egrid, completeness, area, state_of, fosnr, geometry)
SELECT t_id, nbident, nummer, egris_egrid, vollstaendigkeit, flaechenmass, stand_am::timestamp without time zone, gem_bfs, geometrie
FROM realestateproj
),

realestateproj_posnumber AS (
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
FROM realestateproj as a, av_avdpool_ng.liegenschaften_projgrundstueckpos as b
WHERE a.gem_bfs = 2583 
AND b.gem_bfs = 2583
AND a.gs_t_id = b.projgrundstueckpos_von
)

INSERT INTO av_mopublic_export.ownership_realestateproj_posnumber (t_id, identnd, anumber, ori, hali, vali, fosnr, posnumber_of, pos)
SELECT t_id, nbident, nummer, ori, hali, vali, gem_bfs, posnumber_of, pos
FROM realestateproj_posnumber;
*/



