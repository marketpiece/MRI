-- Function: client_cue.web_load_rc_similar_composer()

-- DROP FUNCTION client_cue.web_load_rc_similar_composer(INTEGER);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_similar_composer(
    IN arg_file_id INTEGER,
    OUT composer_id BIGINT,
    OUT composer CHARACTER VARYING,
    OUT similar_composer TEXT,
    OUT affiliation CHARACTER VARYING
)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns RapidCue Composers with/without similarities
DECLARE
    v_cmd  VARCHAR;
BEGIN
    
    v_cmd := 'SELECT 
                     c.row_id,
                     c.composer_name,
                     array_to_string(array_agg(distinct coalesce(c1.composer_name,c2.composer_name)),'' :: '') similar_composer_name,
                     c.affiliation
                FROM client_cue.rc_composer c
                LEFT JOIN client_cue.rc_composer_similarity s ON s.file_id = c.file_id AND s.composer_id = c.row_id AND s.group_id is null
                LEFT JOIN client_cue.rc_composer c1 ON c1.file_id = s.file_id AND c1.row_id = s.similar_composer_id
                LEFT JOIN client_cue.composer c2 ON c2.client_id = c.client_id AND c2.row_id = s.similar_composer_id
               WHERE c.file_id = '||arg_file_id||'
                 AND c.invalid_dt is null
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