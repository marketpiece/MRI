-- Function: client_cue.web_load_rc_similar_composer_groups_merge()

-- DROP FUNCTION client_cue.web_load_rc_similar_composer_groups_merge(INTEGER,CHARACTER VARYING,CHARACTER VARYING,CHARACTER VARYING);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_similar_composer_groups_merge(
    IN arg_file_id INTEGER,
    IN arg_merge_composer_ids CHARACTER VARYING,
    IN arg_primary_id CHARACTER VARYING,
    IN arg_user_id CHARACTER VARYING
)
  RETURNS CHARACTER VARYING AS
$BODY$
-- $DESCRIPTION = Merges RapidCue Similar Composers Into One
DECLARE
   v_cmd           VARCHAR;
   v_arr_ids       VARCHAR[][];
   v_arr_chk       VARCHAR[];
   v_group_id      BIGINT;
   cRows           INTEGER;
   -- v_primary_name  VARCHAR;
   -- v_merge_name    VARCHAR;
   v_song_titles   VARCHAR := NULL;
BEGIN
    
   v_arr_ids := arg_merge_composer_ids::varchar[][];
   v_arr_chk := string_to_array(array_to_string(v_arr_ids,',')||','||arg_primary_id,',');

   IF v_arr_ids is null or arg_primary_id is null THEN
      RAISE EXCEPTION 'There is no selected %.',case when v_arr_ids is null then 'composer for merge' 
                                                     when arg_primary_id is null then 'primary composer'
                                                end USING ERRCODE = 'MRI09';
   END IF;

   IF v_arr_ids IS NOT NULL THEN 
     -- The group_id of the selection
     SELECT nextval('client_cue.seq_party__group_id'::regclass) INTO v_group_id;
     RAISE INFO 'v_group_id = %',v_group_id;
 
     -- EXECUTE 'select composer_name from client_cue.rc_composer where row_id = '||arg_primary_id INTO v_primary_name;

     -- Add to similarities table nonexistent composer_id if chosen for a merge/primary
     FOR i IN 1..array_upper(v_arr_chk,1) LOOP

        -- Check for non similar
        /*EXECUTE 'select composer_name from client_cue.rc_composer where row_id = '||v_arr_chk[i]||'
                 union 
                 select composer_name from client_cue.composer where row_id = '||v_arr_chk[i]
           INTO v_merge_name;
        IF (similarity(v_primary_name,v_merge_name) < 0.94 OR
           (v_primary_name not like '%'||v_merge_name||'%' or abs(char_length(v_primary_name)-char_length(v_merge_name)) > 4)) THEN 
           RAISE EXCEPTION 'The selected composer [%] is not similar to the primary.',v_merge_name USING ERRCODE = 'MRI09';
        END IF;*/    

        IF NOT EXISTS(select 1 from client_cue.rc_composer_similarity where similar_composer_id = v_arr_chk[i]::bigint) THEN
           v_cmd := 'INSERT INTO client_cue.rc_composer_similarity(file_id, composer_id, similar_composer_id, bin, merge, main)
                     VALUES($1, $2, $2, false, false, false)';
           RAISE INFO 'v_cmd = %',v_cmd;
           EXECUTE v_cmd USING arg_file_id, v_arr_chk[i]::bigint;
        END IF;
     END LOOP;

     v_cmd = 'UPDATE client_cue.rc_composer_similarity
                 SET main = true,
                     merge = false,
                     group_id = '||v_group_id||',
                     modified_dt = now(), 
                     modified_by = '||arg_user_id||'
               WHERE file_id = '||arg_file_id||'
                 AND similar_composer_id = '||arg_primary_id;
     RAISE INFO 'v_cmd = %',v_cmd;
     EXECUTE v_cmd;

     FOR i IN 1..array_upper(v_arr_ids,2) LOOP
        v_cmd := 'UPDATE client_cue.rc_composer_similarity
                     SET main = false,
                         merge = true,
                         group_id = '||v_group_id||',
                         modified_dt = now(),
                         modified_by = '||arg_user_id||'
                   WHERE file_id = '||arg_file_id||'
                     AND similar_composer_id = '||v_arr_ids[1][i];
        EXECUTE v_cmd;
     END LOOP;
  
     -- Check for eventual duplicates, which may be made by the merge
     CREATE TEMP TABLE dupl_tbl(
     row_id bigint,
     cuesong_id bigint,
     composer_id bigint,
     rn int,
     share_pct numeric,
     title varchar
     ) ON COMMIT DROP;
  
     v_cmd := 'INSERT INTO dupl_tbl
               SELECT *
                 FROM (SELECT cc.row_id,
                              cc.cuesong_id,
                              case when cc.composer_id in ('||array_to_string(v_arr_ids,',')||') then '||arg_primary_id||' else cc.composer_id end composer_id, 
                              ROW_NUMBER() OVER(PARTITION BY cc.cuesong_id, 
                                                             case when cc.composer_id in ('||array_to_string(v_arr_ids,',')||') then '||arg_primary_id||' else cc.composer_id end
                                                    ORDER BY cc.row_id)::int rn,
                              cc.share_pct,
                              s.title
                         FROM client_cue.rc_cuesong_composer cc
                         JOIN client_cue.rc_cuesong s ON s.row_id = cc.cuesong_id
                        WHERE cc.composer_id in ('||array_to_string(v_arr_chk,',')||')
                    ) dpl ';
     RAISE INFO 'v_cmd = %',v_cmd;
     EXECUTE v_cmd;
     
     v_cmd := 'SELECT array_to_string(array_agg(DISTINCT title),'','') FROM dupl_tbl WHERE rn > 1';
     RAISE INFO 'v_cmd = %',v_cmd;
     EXECUTE v_cmd INTO v_song_titles;
     IF v_song_titles is not null THEN
        RAISE EXCEPTION 'If the selected % replaced by the main it will cause duplication with the following songs [%]. Do you still wish to merge them and the share percentage of the merged to be assigned to the main ?',
                        CASE WHEN array_length(v_arr_ids,2) > 1 THEN 'publishers are' ELSE 'publisher is' END, v_song_titles
                  USING ERRCODE = 'MRI09';

     -- Delete duplicates and assign the share_pct to the main
        v_cmd := 'WITH upd_share AS (UPDATE client_cue.rc_cuesong_composer cp
                                        SET share_pct = share_pct + dupl1.share_pct
                                       FROM dupl_tbl d
                                       JOIN (select cuesong_id, sum(share_pct) share_pct from dupl_tbl where rn > 1 group by cuesong_id) dupl ON dupl.cuesong_id = d.cuesong_id
                                      WHERE cp.row_id = d.row_id
                                        AND d.rn = 1
                                     )
                  DELETE FROM client_cue.rc_cuesong_composer cp USING dupl_tbl dupl WHERE cp.row_id = dupl.row_id AND dupl.rn > 1';
        RAISE INFO 'v_cmd = %',v_cmd;
        EXECUTE v_cmd;
        GET DIAGNOSTICS cRows = ROW_COUNT;
        RAISE INFO 'Deleted rows = [%]', cRows;
     END IF;
  
     -- Merge and invalidate the selected for merge composer ids
     v_cmd := 'WITH merge_comps AS (UPDATE client_cue.rc_cuesong_composer cc
                                       SET composer_id = '||arg_primary_id||'
                                     WHERE cc.file_id = '||arg_file_id||'
                                       AND cc.composer_id in (select distinct similar_composer_id from client_cue.rc_composer_similarity where group_id = '||v_group_id||' and merge is true)
                                   )
               UPDATE client_cue.rc_composer c
                  SET invalid_dt = now(),
                      invalid_by = '||arg_user_id||' 
                WHERE c.file_id = '||arg_file_id||'
                  AND c.row_id in (select distinct similar_composer_id from client_cue.rc_composer_similarity where group_id = '||v_group_id||' and merge is true)';
     RAISE INFO 'v_cmd = %',v_cmd;
     EXECUTE v_cmd;
     GET DIAGNOSTICS cRows = ROW_COUNT;
     RAISE INFO 'Invalidated composers = [%]', cRows;

   END IF;
   
   RETURN 'Complete';   

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;