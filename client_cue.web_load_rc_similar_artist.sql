-- Function: client_cue.web_load_rc_similar_artist()

-- DROP FUNCTION client_cue.web_load_rc_similar_artist(INTEGER);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_similar_artist(
    IN arg_file_id INTEGER,
    OUT artist_id BIGINT,
    OUT artist CHARACTER VARYING,
    OUT similar_artist TEXT
)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns RapidCue Artists with/without similarities
DECLARE
    v_cmd  VARCHAR;
BEGIN
    
    v_cmd := 'SELECT 
                     a.row_id,
                     a.artist_name,
                     array_to_string(array_agg(distinct coalesce(a1.artist_name,a2.artist_name)),'' :: '') similar_artist_name
                FROM client_cue.rc_artist a
                LEFT JOIN client_cue.rc_artist_similarity s ON s.file_id = a.file_id AND s.artist_id = a.row_id AND s.group_id is null
                LEFT JOIN client_cue.rc_artist a1 ON a1.file_id = s.file_id AND a1.row_id = s.similar_artist_id
                LEFT JOIN client_cue.artist a2 ON a2.client_id = a.client_id AND a2.row_id = s.similar_artist_id
               WHERE a.file_id = '||arg_file_id||'
                 AND a.invalid_dt is null
               GROUP BY 1,2
               ORDER BY 3 desc, 2';
    RETURN QUERY EXECUTE v_cmd;
    IF NOT FOUND THEN
      RETURN QUERY
      SELECT null::bigint, 
             null::varchar, 
             null::text;
    END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;