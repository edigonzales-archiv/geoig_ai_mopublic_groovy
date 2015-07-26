--DROP FUNCTION metadata_metadata(text, integer);
CREATE OR REPLACE FUNCTION metadata_metadata(dbschema text, bfsnr integer)
  RETURNS boolean AS
$$
BEGIN
  EXECUTE
'
INSERT INTO av_mopublic_export.metadata_metadata (t_id, generated_from, generated_based_model, generated_date, other_metadata_information)
SELECT nextval('''||dbschema||'.t_id'') AS t_id, ''ili2pg'' as generated_from, ''DM01AVCH24D'' as generated_based_model, now()::timestamp without time zone as generated_date, '||bfsnr||'::text as other_metadata_information
';

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';
