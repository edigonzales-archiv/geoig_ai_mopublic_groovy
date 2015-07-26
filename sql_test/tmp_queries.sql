﻿DELETE FROM av_mopublic.liegenschaften__grenzpunkt WHERE bfsnr = ?;

INSERT INTO av_mopublic.liegenschaften__grenzpunkt (tid, geometrie, gueltigkeit, lagegen, lagezuv, punktzeichen, stand_am, bfsnr)
SELECT b.tid as tid, b.geometrie as geometrie, c.designation_d as gueltigkeit, b.lagegen as lagegen, d.designation_d as lagezuv, e.designation_d as punktzeichen,
 CASE 
   WHEN a.gueltigereintrag IS NULL THEN to_date(a.datum1, 'YYYYMMDD')
   ELSE to_date(a.gueltigereintrag, 'YYYYMMDD')
 END AS stand_am, 
 b.gem_bfs as bfsnr
FROM av_avdpool_ch.liegenschaften_lsnachfuehrung as a, av_avdpool_ch.liegenschaften_grenzpunkt as b, av_mopublic_meta.validity_type as c, av_mopublic_meta.reliability_type as d, av_mopublic_meta.mark_type as e
WHERE a.gem_bfs = ? AND b.gem_bfs = ?
AND b.entstehung = a.tid
AND a.gueltigkeit = c.code
AND b.lagezuv = d.code
AND b.punktzeichen = e.code;