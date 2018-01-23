-- Function: client_cue.web_load_rc_similar_publisher()

-- DROP FUNCTION client_cue.web_load_rc_similar_publisher(INTEGER);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_similar_publisher(
    IN arg_file_id INTEGER,
    OUT publisher_id BIGINT,
    OUT publisher CHARACTER VARYING,
    OUT similar_publisher TEXT,
    OUT affiliation CHARACTER VARYING
)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns RapidCue Publishers with/without similarities
DECLARE
    v_cmd  VARCHAR;
BEGIN
    
    v_cmd := 'SELECT 
                     p.row_id,
                     p.publisher_name,
                     array_to_string(array_agg(distinct coalesce(p1.publisher_name,p2.publisher_name)),'' :: '') similar_publisher_name,
                     p.affiliation
                FROM client_cue.rc_publisher p
                LEFT JOIN client_cue.rc_publisher_similarity s ON s.file_id = p.file_id AND s.publisher_id = p.row_id AND s.group_id is null
                LEFT JOIN client_cue.rc_publisher p1 ON p1.file_id = s.file_id AND p1.row_id = s.similar_publisher_id
                LEFT JOIN client_cue.publisher p2 ON p2.client_id = p.client_id AND p2.row_id = s.similar_publisher_id
               WHERE p.file_id = '||arg_file_id||'
                 AND p.invalid_dt is null
               GROUP BY 1,2,4
               ORDER BY 3 desc,2';
    RETURN QUERY EXECUTE v_cmd;
    IF NOT FOUND THEN
      RETURN QUERY
      SELECT null::bigint, 
             null::varchar, 
             null::text,
             null::varchar;
    END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;