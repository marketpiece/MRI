-- Function: client_cue.web_load_rc_similarity_object_flags()

-- DROP FUNCTION client_cue.web_load_rc_similarity_object_flags(INTEGER);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_similarity_object_flags(
    IN arg_file_id INTEGER,
    OUT artist BOOLEAN,
    OUT composer BOOLEAN,
    OUT publisher BOOLEAN
)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns RapidCue Object Flags
BEGIN
    
   RETURN QUERY 
   SELECT CASE WHEN EXISTS(select 1 from client_cue.rc_artist_similarity where file_id = arg_file_id and group_id is null) THEN true ELSE false END as artist,
          CASE WHEN EXISTS(select 1 from client_cue.rc_composer_similarity where file_id = arg_file_id and group_id is null) THEN true ELSE false END as composer,
          CASE WHEN EXISTS(select 1 from client_cue.rc_publisher_similarity where file_id = arg_file_id and group_id is null) THEN true ELSE false END as publisher;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;