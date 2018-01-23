-- Function: client_cue.web_load_rc_similar_artist_groups()

-- DROP FUNCTION client_cue.web_load_rc_similar_artist_groups(INTEGER,BIGINT);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_similar_artist_groups(
    IN arg_file_id INTEGER,
    IN arg_artist_id BIGINT,
    OUT similar_artist_id BIGINT,
    OUT similar_artist_name CHARACTER VARYING,
    OUT source CHARACTER VARYING,
    OUT group_number INTEGER,
    OUT bin_artist_match CHARACTER VARYING,
    OUT main CHARACTER VARYING,
    OUT merge BOOLEAN
)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns RapidCue Similar Artist Groups with direct/indirect relation
DECLARE
    v_cmd  VARCHAR;
    v_client_id INTEGER;
BEGIN
    SELECT client_id FROM client_cue.rc_file WHERE file_id = arg_file_id INTO v_client_id;
    
    v_cmd := 'WITH similarity_groups(file_id, artist_id, similar_artist_id, bin, merge, main, client_id) AS (
                                               SELECT a.file_id, 
                                                      a.row_id, 
                                                      a.row_id, 
                                                      false, 
                                                      false, 
                                                      false, 
                                                      a.client_id
                                                 FROM client_cue.rc_artist a
                                                 JOIN client_cue.rc_artist_similarity s ON s.file_id = a.file_id AND s.artist_id = a.row_id AND s.group_id is null
                                                WHERE a.file_id = '||arg_file_id||'
                                                  AND a.row_id = '||arg_artist_id||'
                                               UNION
                                               SELECT coalesce(s.file_id,'||arg_file_id||'), 
                                                      coalesce(s.artist_id,'||arg_artist_id||'),
                                                      coalesce(s.similar_artist_id,'||arg_artist_id||'),
                                                      coalesce(s.bin,false), 
                                                      s.merge,
                                                      s.main,
                                                      a.client_id
                                                 FROM client_cue.rc_artist a
                                                 LEFT JOIN client_cue.rc_artist_similarity s ON s.file_id = a.file_id AND s.artist_id = a.row_id AND s.group_id is null
                                                WHERE a.file_id = '||arg_file_id||' 
                                                  AND a.row_id = '||arg_artist_id||'
                                               ),
                                hs_main AS    ( SELECT s.file_id,
                                                       s.similar_artist_id as main_artist_id,
                                                       coalesce(a.artist_name,a2.artist_name) as main_artist_name
                                                  FROM client_cue.rc_artist_similarity_history s
                                                  LEFT JOIN client_cue.rc_artist_history a on a.file_id = s.file_id and a.row_id = s.similar_artist_id and s.bin is false
                                                  LEFT JOIN client_cue.artist_'||coalesce(v_client_id::varchar,'')||' a2 on a2.row_id = s.similar_artist_id and s.bin is true
                                                 WHERE s.group_id is not null
                                                   AND s.main is true
                                                 ORDER BY s.group_id
                                               )
              SELECT 
                     sg.similar_artist_id, 
                     coalesce(a.artist_name,a1.artist_name) as similar_artist_name,
                     CASE WHEN sg.bin THEN ''Bin'' ELSE ''File'' END::varchar as source,
                    (row_number() OVER (PARTITION BY sg.artist_id ORDER BY a.artist_name))::int group_number,
                     CASE WHEN a.matched THEN oa.artist_name ELSE '''' END::varchar as match_bin_artist_name,
                     CASE WHEN h.main_artist_id IS NOT NULL THEN ''Yes'' ELSE '''' END::varchar as main,
                     sg.merge
                FROM similarity_groups sg
                LEFT JOIN client_cue.rc_artist a ON a.file_id = sg.file_id AND a.row_id = sg.similar_artist_id AND sg.bin is false
                LEFT JOIN client_cue.artist a1 ON a1.client_id = sg.client_id AND a1.row_id = sg.similar_artist_id AND sg.bin is true
                LEFT JOIN hs_main h ON h.main_artist_name = coalesce(a.artist_name,a1.artist_name)
                LEFT JOIN client_cue.artist oa ON oa.client_id = a.client_id AND oa.row_id = a.original_artist_id
               ORDER BY 1,6';
    RETURN QUERY EXECUTE v_cmd;
    IF NOT FOUND THEN
      RETURN QUERY
      SELECT null::bigint, 
             null::varchar,
             null::varchar,
             null::int,
             null::varchar,
             null::varchar,
             null::boolean;
    END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;