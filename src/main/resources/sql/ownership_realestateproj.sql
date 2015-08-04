--DROP FUNCTION ownership_realestateproj(text, integer);
CREATE OR REPLACE FUNCTION ownership_realestateproj(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
WITH realestateproj AS (
SELECT nextval('''||dbschema||'.t_id'') as t_id, a.t_id as gs_t_id, a.nbident, a.nummer, a.egris_egrid, a.vollstaendigkeit, b.flaechenmass,
 CASE
   WHEN d.gueltigereintrag IS NULL THEN d.datum1
   ELSE d.gueltigereintrag
 END AS stand_am,
 b.gem_bfs, b.geometrie
FROM av_avdpool_ng.liegenschaften_lsnachfuehrung as d, av_avdpool_ng.liegenschaften_projgrundstueck as a, av_avdpool_ng.liegenschaften_projliegenschaft as b
WHERE a.gem_bfs = '||bfsnr||'
AND b.gem_bfs = '||bfsnr||'
AND d.gem_bfs = '||bfsnr||'
AND b.projliegenschaft_von = a.t_id
AND a.entstehung = d.t_id
),

foo AS (
INSERT INTO '||dbschema||'.ownership_realestateproj (t_id, identnd, anumber, egris_egrid, completeness, area, state_of, fosnr, geometry)
SELECT t_id, nbident, nummer, egris_egrid, vollstaendigkeit, flaechenmass, stand_am::timestamp without time zone, gem_bfs, geometrie
FROM realestateproj
),

realestateproj_posnumber AS (
SELECT nextval('''||dbschema||'.t_id'') as t_id, a.nbident, a.nummer,
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
WHERE a.gem_bfs = '||bfsnr||'
AND b.gem_bfs = '||bfsnr||'
AND a.gs_t_id = b.projgrundstueckpos_von
)

INSERT INTO '||dbschema||'.ownership_realestateproj_posnumber (t_id, identnd, anumber, ori, hali, vali, fosnr, posnumber_of, pos)
SELECT t_id, nbident, nummer, ori, hali, vali, gem_bfs, posnumber_of, pos
FROM realestateproj_posnumber;
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
