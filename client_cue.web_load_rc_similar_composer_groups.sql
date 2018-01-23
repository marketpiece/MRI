-- Function: client_cue.web_load_rc_similar_composer_groups()

-- DROP FUNCTION client_cue.web_load_rc_similar_composer_groups(INTEGER,BIGINT);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_similar_composer_groups(
    IN arg_file_id INTEGER,
    IN arg_composer_id BIGINT,
    OUT similar_composer_id BIGINT,
    OUT similar_composer_name CHARACTER VARYING,
    OUT affiliation CHARACTER VARYING,
    OUT role CHARACTER VARYING,
    OUT source CHARACTER VARYING,
    OUT group_number INTEGER,
    OUT bin_composer_match CHARACTER VARYING,
    OUT main CHARACTER VARYING,
    OUT merge BOOLEAN
)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns RapidCue Similar Composer Groups with direct/indirect relation
DECLARE
    v_cmd  VARCHAR;
    v_client_id INTEGER;
BEGIN
    SELECT client_id FROM client_cue.rc_file WHERE file_id = arg_file_id INTO v_client_id;

    v_cmd := 'WITH similarity_groups(file_id, composer_id, similar_composer_id, bin, merge, main, client_id) AS (
                                               SELECT c.file_id, 
                                                      c.row_id, 
                                                      c.row_id, 
                                                      false, 
                                                      false, 
                                                      false, 
                                                      c.client_id
                                                 FROM client_cue.rc_composer c
                                                 JOIN client_cue.rc_composer_similarity s ON s.file_id = c.file_id AND s.composer_id = c.row_id AND s.group_id is null
                                                WHERE c.file_id = '||arg_file_id||'
                                                  AND c.row_id = '||coalesce(arg_composer_id,0)||'
                                               UNION
                                               SELECT coalesce(s.file_id,'||arg_file_id||'), 
                                                      coalesce(s.composer_id,'||coalesce(arg_composer_id,0)||'),
                                                      coalesce(s.similar_composer_id,'||coalesce(arg_composer_id,0)||'),
                                                      coalesce(s.bin,false),
                                                      s.merge,
                                                      s.main,
                                                      c.client_id
                                                 FROM client_cue.rc_composer c
                                                 LEFT JOIN client_cue.rc_composer_similarity s ON s.file_id = c.file_id AND s.composer_id = c.row_id AND s.group_id is null
                                                WHERE c.file_id = '||arg_file_id||' 
                                                  AND c.row_id = '||coalesce(arg_composer_id,0)||'
                                               ),
                                hs_main AS    ( SELECT s.file_id,
                                                       s.similar_composer_id as main_composer_id,
                                                       coalesce(c.composer_name,c2.composer_name) as main_composer_name,
                                                       coalesce(c.society_id,c2.society_id) as society_id
                                                  FROM client_cue.rc_composer_similarity_history s
                                                  LEFT JOIN client_cue.rc_composer_history c on c.file_id = s.file_id and c.row_id = s.similar_composer_id and s.bin is false
                                                  LEFT JOIN client_cue.composer_'||coalesce(v_client_id::varchar,'')||' c2 on c2.row_id = s.similar_composer_id and s.bin is true
                                                 WHERE s.group_id is not null
                                                   AND s.main is true
                                                 ORDER BY s.group_id
                                               ),
                                comp_roles AS (SELECT file_id,
                                                      upper(btrim(regexp_replace(coalesce(first_name,'''')||chr(32)||coalesce(middle_name,'''')||chr(32)||coalesce(last_name,''''),''\s{2,}'',chr(32),''g''))) as composer_name,
                                                      array_to_string(array_agg(distinct upper(trim(role))),'','') as role
                                                 FROM client_cue.rc_file_data
                                                 JOIN client_cue.composer_role pr ON upper(pr.role_name) = upper(trim(role))
                                                GROUP BY 1,2
                                              )
              SELECT sg.similar_composer_id, 
                     coalesce(c.composer_name,c1.composer_name) as similar_composer_name,
                     coalesce(c.affiliation,upper(s.acronym)) as affiliation,
                     cr.role::varchar,
                     CASE WHEN sg.bin THEN ''Bin'' ELSE ''File'' END::varchar as source,
                    (row_number() OVER (PARTITION BY sg.composer_id ORDER BY c.composer_name))::int group_number,
                     CASE WHEN c.matched THEN oc.composer_name ELSE '''' END::varchar as match_bin_composer_name,
                     CASE WHEN h.main_composer_id IS NOT NULL THEN ''Yes'' ELSE '''' END::varchar as main,
                     sg.merge
                FROM similarity_groups sg
                LEFT JOIN client_cue.rc_composer c ON c.file_id = sg.file_id AND c.row_id = sg.similar_composer_id AND sg.bin is false
                LEFT JOIN client_cue.composer c1 ON c1.client_id = sg.client_id AND c1.row_id = sg.similar_composer_id AND sg.bin is true
                LEFT JOIN hs_main h ON h.main_composer_name = coalesce(c.composer_name,c1.composer_name) AND h.society_id = coalesce(c.society_id,c1.society_id)
                LEFT JOIN songdex.societies s on s.society_id = c1.society_id AND s.society_flg is true
                LEFT JOIN comp_roles cr ON cr.file_id = sg.file_id AND cr.composer_name = c.composer_name
                LEFT JOIN client_cue.composer oc ON oc.client_id = c.client_id AND oc.row_id = c.original_composer_id
               ORDER BY 1,6';
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