-- Function: client_cue.web_load_rc_similar_publisher_groups()

-- DROP FUNCTION client_cue.web_load_rc_similar_publisher_groups(INTEGER,BIGINT);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_similar_publisher_groups(
    IN arg_file_id INTEGER,
    IN arg_publisher_id BIGINT,
    OUT similar_publisher_id BIGINT,
    OUT similar_publisher_name CHARACTER VARYING,
    OUT affiliation CHARACTER VARYING,
    OUT role CHARACTER VARYING,
    OUT source CHARACTER VARYING,
    OUT group_number INTEGER,
    OUT bin_publisher_match CHARACTER VARYING,
    OUT main CHARACTER VARYING,
    OUT merge BOOLEAN
)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns RapidCue Similar Publisher Groups with direct/indirect relation
DECLARE
    v_cmd  VARCHAR;
    v_client_id INTEGER;
BEGIN
    SELECT client_id FROM client_cue.rc_file WHERE file_id = arg_file_id INTO v_client_id;
    
    v_cmd := 'WITH similarity_groups(file_id, publisher_id, similar_publisher_id, bin, merge, main, client_id, seq) AS (
                                               SELECT p.file_id, 
                                                      p.row_id, 
                                                      p.row_id, 
                                                      false, 
                                                      false, 
                                                      false, 
                                                      p.client_id,
                                                      1 seq
                                                 FROM client_cue.rc_publisher p
                                                 JOIN client_cue.rc_publisher_similarity s ON s.file_id = p.file_id AND s.publisher_id = p.row_id AND s.group_id is null
                                                WHERE p.file_id = '||arg_file_id||'
                                                  AND p.row_id = '||coalesce(arg_publisher_id,0)||'
                                               UNION
                                               SELECT coalesce(s.file_id,'||arg_file_id||'), 
                                                      coalesce(s.publisher_id,'||coalesce(arg_publisher_id,0)||'), 
                                                      coalesce(s.similar_publisher_id,'||coalesce(arg_publisher_id,0)||'), 
                                                      coalesce(s.bin,false), 
                                                      s.merge, 
                                                      s.main, 
                                                      p.client_id,
                                                      2 seq
                                                 FROM client_cue.rc_publisher p
                                                 LEFT JOIN client_cue.rc_publisher_similarity s ON s.file_id = p.file_id AND s.publisher_id = p.row_id AND s.group_id is null
                                                WHERE p.file_id = '||arg_file_id||' 
                                                  AND p.row_id = '||coalesce(arg_publisher_id,0)||'
                                               ),
                                hs_main AS    ( SELECT s.file_id,
                                                       s.similar_publisher_id as main_publisher_id,
                                                       coalesce(p.publisher_name,p2.publisher_name) as main_publisher_name,
                                                       coalesce(p.society_id,p2.society_id) as society_id
                                                  FROM client_cue.rc_publisher_similarity_history s
                                                  LEFT JOIN client_cue.rc_publisher_history p on p.file_id = s.file_id and p.row_id = s.similar_publisher_id and s.bin is false
                                                  LEFT JOIN client_cue.publisher_'||coalesce(v_client_id::varchar,'')||' p2 on p2.row_id = s.similar_publisher_id and s.bin is true
                                                 WHERE s.group_id is not null
                                                   AND s.main is true
                                                 ORDER BY s.group_id
                                               ),
                                pub_roles AS ( SELECT file_id,
                                                      upper(btrim(regexp_replace(company_name,''\s{2,}'',chr(32),''g''))) publisher_name,
                                                      array_to_string(array_agg(distinct upper(trim(role))),'','') as role
                                                 FROM client_cue.rc_file_data
                                                 JOIN client_cue.publisher_role pr ON upper(pr.role_name) = upper(trim(role))
                                                GROUP BY 1,2
                                             )
              SELECT sg.similar_publisher_id,
                     coalesce(p.publisher_name,p1.publisher_name) as similar_publisher_name,
                     coalesce(p.affiliation,upper(s.acronym)) as affiliation,
                     pr.role::varchar,
                     CASE WHEN sg.bin THEN ''Bin'' ELSE ''File'' END::varchar as source,
                    (row_number() OVER (PARTITION BY SG.PUBLISHER_ID ORDER BY seq))::int group_number,
                     CASE WHEN p.matched THEN op.publisher_name ELSE '''' END::varchar as match_bin_publisher_name,
                     CASE WHEN h.main_publisher_id IS NOT NULL THEN ''Yes'' ELSE '''' END::varchar as main,
                     sg.merge
                FROM similarity_groups sg
                LEFT JOIN client_cue.rc_publisher p ON p.file_id = sg.file_id AND p.row_id = sg.similar_publisher_id AND sg.bin is false
                LEFT JOIN client_cue.publisher p1 ON p1.client_id = sg.client_id AND p1.row_id = sg.similar_publisher_id AND sg.bin is true
                LEFT JOIN hs_main h ON h.main_publisher_name = coalesce(p.publisher_name,p1.publisher_name) AND h.society_id = coalesce(p.society_id,p1.society_id)
                LEFT JOIN songdex.societies s on s.society_id = p1.society_id AND s.society_flg is true
                LEFT JOIN pub_roles pr ON pr.file_id = sg.file_id AND pr.publisher_name = p.publisher_name
                LEFT JOIN client_cue.publisher op ON op.client_id = p.client_id AND op.row_id = p.original_publisher_id
               ORDER BY seq,6';
    RETURN QUERY EXECUTE v_cmd;
    IF NOT FOUND THEN
      RETURN QUERY
      SELECT null::bigint, 
             null::varchar,
             null::varchar,
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