--DROP FUNCTION land_cover_lcsurface(text, integer);
CREATE OR REPLACE FUNCTION land_cover_lcsurface(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
WITH bb AS (
SELECT a.t_id, a.gem_bfs, a.art,
  CASE
    WHEN b.gueltigereintrag IS NULL THEN b.datum1
    ELSE b.gueltigereintrag
  END AS stand_am,
  CASE
    WHEN a.qualitaet IS NULL THEN 0
    ELSE a.qualitaet
  END AS qualitaet, a.geometrie
FROM av_avdpool_ng.bodenbedeckung_boflaeche a, av_avdpool_ng.bodenbedeckung_bbnachfuehrung b
WHERE a.gem_bfs = '||bfsnr||'
AND b.gem_bfs = '||bfsnr||'
AND a.entstehung = b.t_id
),

gebnr AS (
SELECT gebaeudenummer_von, gwr_egid, gem_bfs
FROM av_avdpool_ng.bodenbedeckung_gebaeudenummer
WHERE gem_bfs = '||bfsnr||'
),

-- "lc" entspricht Struktur und Inhalt lcsurface-Tabelle von MOpublic.
-- "lc" bekommt eine neue t_id (von der Sequenz).
-- bb_t_id ist eigentlich nicht notwendig. Aber es wird fuer das Herstellen der Relation
-- der lcsurface_postext-Tabelle zur lcsurface-Tabelle verwendet (da wir ja eine neue
-- t_id vergeben).
lc AS (
SELECT nextval('''||dbschema||'.t_id'') AS t_id, bb.t_id as bb_t_id, bb.qualitaet, bb.art, gebnr.gwr_egid,
       bb.stand_am::timestamp without time zone  as stand_am,
       bb.gem_bfs, bb.geometrie
FROM bb LEFT JOIN gebnr ON bb.t_id = gebnr.gebaeudenummer_von
),

-- The first INSERT INTO has to be also a CTE (?).
-- can be combined with lc-CTE, see: http://www.postgresql.org/docs/9.1/static/queries-with.html (7.8.2. Data-Modifying Statements in WITH)
foo AS (
INSERT INTO av_mopublic_export.land_cover_lcsurface (t_id, quality, type, regbl_egid, state_of, fosnr, geometry)
SELECT t_id, qualitaet, art, gwr_egid, stand_am, gem_bfs, geometrie
FROM lc
),

-- lcsurface_postext-Tabelle
-- lcpos.a.objektname_von = lc.bb.t_id
lcpostext AS (
SELECT nextval('''||dbschema||'.t_id'') AS t_id, c.t_id as postext_of, 1::integer as typ, a.name,
       b.pos, b.ori,
       CASE
         WHEN b.hali IS NULL
         THEN 1
         ELSE b.hali
       END as hali,
       CASE
         WHEN b.vali IS NULL
         THEN 2
          ELSE b.vali
       END as vali,
       a.gem_bfs
FROM av_avdpool_ng.bodenbedeckung_objektname as a, av_avdpool_ng.bodenbedeckung_objektnamepos as b, lc as c
WHERE a.gem_bfs = '||bfsnr||'
AND b.gem_bfs = '||bfsnr||'
AND a.t_id = b.objektnamepos_von
AND a.objektname_von = c.bb_t_id
)

INSERT INTO av_mopublic_export.land_cover_lcsurface_postext(t_id, type, number_name, ori, hali, vali, fosnr, postext_of, pos)
SELECT t_id, typ, name, ori, hali, vali, gem_bfs, postext_of, pos
FROM lcpostext;
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';