-- Function: client_cue.web_load_rc_dynamic_matched_object(integer)

-- DROP FUNCTION client_cue.web_load_rc_dynamic_matched_object(integer);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_dynamic_matched_object(
    IN arg_file_id integer,
    OUT file_id integer,
    OUT program character varying,
    OUT episode character varying,
    OUT row_id bigint,
    OUT bin_row_id bigint,
    OUT object_type integer,
    OUT fullmatch boolean,
    OUT series boolean)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns rapidcue list of dynamic matched objects
DECLARE
    cmd    VARCHAR;
    rec    RECORD;
BEGIN
    -- if the series program is a match with no match episode then replace program_id with bin_id
    cmd := ' WITH 
                    ep AS (
                           SELECT 
                                  e.row_id,
                                  e1.episode_id as bin_row_id,
                                  e1.program_id as group_id,
                                  2 object_type
                             FROM client_cue.rc_episode e
                             JOIN client_cue.rc_program p ON p.row_id = e.program_id
                             LEFT JOIN client_cue.episode_alias e1 ON e1.client_id = e.client_id
                                                                  AND upper(e1.title) = upper(e.title)
                             JOIN client_cue.program p1 ON p1.row_id = e1.program_id
                                                       AND upper(p1.title) = upper(p.title)
                            WHERE e.file_id = '||arg_file_id||'
                            GROUP BY 1,2,3
                         ),
                    pr AS (SELECT
                                  p.row_id,
                                  coalesce(p1.row_id,p2.program_id) as bin_row_id,
                                  coalesce(p1.row_id,p2.program_id) as group_id,
                                  1 object_type,
                                  p1.series_flg as series
                            FROM client_cue.rc_program p
                            LEFT JOIN client_cue.program p1 ON p1.row_id != p.row_id
                                                           AND p1.client_id = p.client_id
                                                           AND upper(p1.title) = upper(p.title)
                            LEFT JOIN client_cue.program_alias p2 ON p2.client_id = p.client_id
                                                                 AND upper(p2.title) = upper(p.title)
                            LEFT JOIN ep ON ep.group_id = coalesce(p1.row_id,p2.program_id)
                           WHERE p.file_id = '||arg_file_id||'
                             AND p1.series_flg is true
                             AND ep.row_id is null
                           GROUP BY 1,2,3,5
                          )
               SELECT row_id, max(bin_row_id) bin_row_id 
                 FROM pr
                GROUP BY 1';
    FOR rec IN execute cmd LOOP
      delete from client_cue.rc_program p where p.file_id = arg_file_id and p.row_id = rec.row_id;
      update client_cue.rc_episode e set program_id = rec.bin_row_id where e.file_id = arg_file_id and e.program_id = rec.row_id;
      update client_cue.rc_cuesheet cs set program_id = rec.bin_row_id where cs.file_id = arg_file_id and cs.program_id = rec.row_id;
    END LOOP;
    
    
    RETURN QUERY
    WITH pr_ep AS (WITH 
                        ep AS (
                               SELECT e.file_id,
                                      p.title::varchar as program,
                                      e.title::varchar as episode,
                                      e.row_id,
                                      e1.episode_id as bin_row_id,
                                      e1.program_id as group_id,
                                      2 object_type,
                                      case when e1.row_id is null then false else true end matched
                                 FROM client_cue.rc_episode e
                                 JOIN client_cue.rc_program p ON p.row_id = e.program_id
                                 LEFT JOIN client_cue.episode_alias e1 ON e1.client_id = e.client_id
                                                                      AND upper(e1.title) = upper(e.title)
                                 JOIN client_cue.program p1 ON p1.row_id = e1.program_id
                                                           AND upper(p1.title) = upper(p.title)
                                WHERE e.file_id = arg_file_id
                                GROUP BY 1,2,3,4,5,6,
                                         case when e1.row_id is null then false else true end
                             ),
                        pr AS (SELECT p.file_id,
                                      p.title::varchar as program,
                                      null::varchar as episode,
                                      p.row_id,
                                      coalesce(p1.row_id,p2.program_id) as bin_row_id,
                                      coalesce(p1.row_id,p2.program_id) as group_id,
                                      1 object_type,
                                      case when p1.row_id is null then false else true end matched,
                                      p1.series_flg as series
                                FROM client_cue.rc_program p
                                LEFT JOIN client_cue.program p1 ON p1.row_id != p.row_id 
                                                               AND p1.client_id = p.client_id
                                                               AND upper(p1.title) = upper(p.title)
                                LEFT JOIN client_cue.program_alias p2 ON p2.client_id = p.client_id
                                                                     AND upper(p2.title) = upper(p.title)
                                                                     AND p2.primary_flg is false
                                LEFT JOIN ep ON ep.group_id = coalesce(p1.row_id,p2.program_id)
                               WHERE p.file_id = arg_file_id
                                 AND CASE WHEN p.series_flg THEN ep.row_id is not null ELSE ep.row_id is null END 
                               GROUP BY 1,2,3,4,5,
                                        case when p1.row_id is null then false else true end,
                                        p1.series_flg
                              )
                   SELECT pr.file_id,
                          pr.program,
                          pr.episode,
                          pr.row_id,
                          pr.bin_row_id,
                          pr.group_id,
                          pr.object_type,
                          pr.matched,
                          pr.series
                     FROM pr
                   UNION
                   SELECT ep.file_id,
                          ep.program,
                          ep.episode,
                          ep.row_id,
                          ep.bin_row_id,
                          ep.group_id,
                          ep.object_type,
                          ep.matched,
                          null
                     FROM ep
                    ORDER BY group_id, object_type, series 
                 )
    SELECT pe.file_id,
           pe.program,
           pe.episode,
           pe.row_id,
           pe.bin_row_id,
           pe.object_type,
           CASE WHEN pe.row_id is not null AND exists(select 1 from pr_ep where pr_ep.matched is false) THEN null ELSE true END fullmatch,
           pe.series
      FROM pr_ep pe
     WHERE pe.bin_row_id is not null
     ORDER BY group_id, object_type;
    IF NOT FOUND THEN
      RETURN QUERY
      SELECT null::int,
             null::varchar,
             null::varchar,
             null::bigint,
             null::bigint,
             null::int,
             null::boolean,
             null::boolean;
    END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;