--DROP FUNCTION territorial_boundaries_other_territ_boundary(text, integer);
CREATE OR REPLACE FUNCTION territorial_boundaries_other_territ_boundary(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
INSERT INTO '||dbschema||'.territorial_boundaries_other_territ_boundary (t_id, type, bound_validity_type, geometry)
SELECT t_id, typ, gueltigkeit, geometrie
FROM
(
 SELECT nextval('''||dbschema||'.t_id'') as t_id, 0::integer as typ, geometrie, gueltigkeit, gem_bfs
 FROM av_avdpool_ng.bezirksgrenzen_bezirksgrenzabschnitt
 WHERE gem_bfs = '||bfsnr||'
UNION ALL
 SELECT nextval('''||dbschema||'.t_id'') as t_id, 1::integer as typ, geometrie, gueltigkeit, gem_bfs
 FROM av_avdpool_ch.kantonsgrenzen_kantonsgrenzabschnitt
 WHERE gem_bfs = '||bfsnr||'
UNION ALL
 SELECT nextval('''||dbschema||'.t_id'') as t_id, 1::integer as typ, geometrie, gueltigkeit, gem_bfs
 FROM av_avdpool_ch.landesgrenzen_landesgrenzabschnitt
 WHERE gem_bfs = '||bfsnr||'
) as a;
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
