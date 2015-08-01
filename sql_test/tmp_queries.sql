INSERT INTO av_mopublic_export.territorial_boundaries_other_territ_boundary (t_id, type, bound_validity_type, geometry)
SELECT t_id, typ, gueltigkeit, geometrie
FROM 
(
 SELECT nextval('av_mopublic_export.t_id') as t_id, 0::integer as typ, geometrie, gueltigkeit, gem_bfs
 FROM av_avdpool_ng.bezirksgrenzen_bezirksgrenzabschnitt
 WHERE gem_bfs = 2583
UNION ALL
 SELECT nextval('av_mopublic_export.t_id') as t_id, 1::integer as typ, geometrie, gueltigkeit, gem_bfs
 FROM av_avdpool_ch.kantonsgrenzen_kantonsgrenzabschnitt
 WHERE gem_bfs = 2583 
UNION ALL
 SELECT nextval('av_mopublic_export.t_id') as t_id, 1::integer as typ, geometrie, gueltigkeit, gem_bfs
 FROM av_avdpool_ch.landesgrenzen_landesgrenzabschnitt
 WHERE gem_bfs = 2583 
) as a;