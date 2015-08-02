--DROP FUNCTION building_addresses(text, integer);
CREATE OR REPLACE FUNCTION building_addresses(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
WITH street_name AS (
-- Why DISTINCT?
 SELECT DISTINCT ON (d.t_id) c.t_id, c.lok_t_id, c.istoffiziellebezeichnung, c.stand_am, c.gem_bfs, d.atext
 FROM
 (
  SELECT nextval('''||dbschema||'.t_id'') as t_id, a.t_id as lok_t_id,
   CASE
    WHEN a.istoffiziellebezeichnung = 0 THEN TRUE
    ELSE FALSE
   END as istoffiziellebezeichnung,
   b.gueltigereintrag::timestamp without time zone as stand_am,
   a.gem_bfs
  FROM av_avdpool_ng.gebaeudeadressen_lokalisation as a, av_avdpool_ng.gebaeudeadressen_gebnachfuehrung as b
  WHERE a.gem_bfs = '||bfsnr||'
  AND b.gem_bfs = '||bfsnr||'
  AND b.t_id = a.entstehung
 ) as c, av_avdpool_ng.gebaeudeadressen_lokalisationsname as d
 WHERE c.lok_t_id = d.benannte
),

foo1 AS (
 INSERT INTO '||dbschema||'.building_addresses_street_name (t_id, street_name, is_official_designation, state_of, fosnr)
 SELECT t_id, atext, istoffiziellebezeichnung, stand_am, gem_bfs
 FROM street_name
),

street_name_pos AS (
 SELECT nextval('''||dbschema||'.t_id'') as t_id, c.t_id as street_name_of, a.atext, b.pos,
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
  b.gem_bfs
 FROM av_avdpool_ng.gebaeudeadressen_lokalisationsname as a, av_avdpool_ng.gebaeudeadressen_lokalisationsnamepos as b, street_name as c
 WHERE a.gem_bfs = '||bfsnr||'
 AND b.gem_bfs = '||bfsnr||'
 AND c.gem_bfs = '||bfsnr||'
 AND a.t_id = b.lokalisationsnamepos_von
 AND a.benannte =  c.lok_t_id
),

foo2 AS (
 INSERT INTO '||dbschema||'.building_addresses_street_name_pos (t_id, street_name, ori, hali, vali, fosnr, street_name_of, pos)
 SELECT t_id, atext, ori, hali, vali, gem_bfs, street_name_of, pos
 FROM street_name_pos
),

-- plzo_os: Geometrien (mit Ortschaftsnamen) der Ortschaftsnamen, die mit Gemeindegrenze intersecten.
-- Nicht noetig fuer PLZ, da dort alles in einer Tabelle.
plzo_os AS (
 SELECT b.t_id, b.atext, a.flaeche
 FROM av_plzortschaft.plzortschaft_ortschaft as a, av_plzortschaft.plzortschaft_ortschaftsname as b, av_avdpool_ng.gemeindegrenzen_gemeindegrenze as c
 WHERE c.gem_bfs = '||bfsnr||'
 AND a.flaeche && c.geometrie
 AND ST_Intersects(a.flaeche, c.geometrie)
 AND a.t_id = ortschaftsname_von
),

gebaeudeeingang AS (
 SELECT nextval('''||dbschema||'.t_id'') as t_id, s.t_id as street_of, g.gebaeudeeingang_von, g.t_id as geb_t_id,
  nf.gueltigereintrag::timestamp without time zone AS stand_am,
  CASE
   WHEN g.status IS NULL THEN 1
   WHEN g.status = 0 THEN 0
   ELSE 1
  END as status, g.inaenderung, g.attributeprovisorisch,
  CASE
   WHEN g.istoffiziellebezeichnung = 0 THEN TRUE
   ELSE FALSE
  END as istoffiziellebezeichnung, g.lage, g.hoehenlage, g.hausnummer, g.im_gebaeude, g.gwr_egid, g.gwr_edid, g.gem_bfs
 FROM av_avdpool_ng.gebaeudeadressen_gebaeudeeingang as g, street_name as s, av_avdpool_ng.gebaeudeadressen_gebnachfuehrung as nf
 WHERE g.gem_bfs = '||bfsnr||'
 AND nf.gem_bfs = '||bfsnr||'
 AND g.gebaeudeeingang_von = s.lok_t_id
 AND g.entstehung = nf.t_id
),

gebaeudeeingang_strassenname AS (
 SELECT g.*, s.atext as lokname
 FROM gebaeudeeingang as g, street_name as s
 WHERE s.lok_t_id = g.gebaeudeeingang_von
),

gebaeudeeingang_strassenname_plz_ortschaft AS (
 SELECT g.*, o.atext as ortname, p.plz, p.zusziff
 FROM gebaeudeeingang_strassenname as g, plzo_os as o, av_plzortschaft.plzo_plz as p
 WHERE g.lage && o.flaeche
 AND g.lage && p.geom
 AND ST_Distance(g.lage, o.flaeche) = 0
 AND ST_Distance(g.lage, p.geom) = 0
),

foo3 AS (
 INSERT INTO '||dbschema||'.building_addresses_building_entrance (t_id, validity, is_official_designation, alevel,
             house_number, name_of_house, regbl_egid, regbl_edid, street_name, postalcode, additional_number, city,
             state_of, fosnr, street_of, pos)
 SELECT t_id, status, istoffiziellebezeichnung, hoehenlage, hausnummer, NULL::varchar, gwr_egid, gwr_edid,
        lokname, plz, zusziff, ortname, stand_am, gem_bfs, street_of, lage
 FROM gebaeudeeingang_strassenname_plz_ortschaft
),

building_addresses_street_name_pos AS (
 SELECT nextval('''||dbschema||'.t_id'') as t_id, g.t_id as pos_of, 0::integer as typ, g.hausnummer,
  CASE
   WHEN h.ori IS NULL THEN 100::double precision
   ELSE h.ori
  END AS ori,
  CASE
   WHEN h.hali IS NULL THEN 1
   ELSE h.hali
  END as hali,
  CASE
   WHEN h.vali IS NULL THEN 2
   ELSE h.vali
  END as vali,
  h.gem_bfs, h.pos
 FROM av_avdpool_ng.gebaeudeadressen_hausnummerpos as h, gebaeudeeingang as g
 WHERE h.gem_bfs = '||bfsnr||'
 AND h.hausnummerpos_von = g.geb_t_id
)

INSERT INTO '||dbschema||'.building_addresses_building_entrance_pos (t_id, type, number_name, ori, hali, vali, fosnr, pos_of, pos)
SELECT t_id, typ, hausnummer, ori, hali, vali, gem_bfs, pos_of, pos
FROM building_addresses_street_name_pos;
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
