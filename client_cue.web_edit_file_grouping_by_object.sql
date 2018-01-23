-- Function: client_cue.web_edit_file_grouping_by_object(integer, boolean)

-- DROP FUNCTION client_cue.web_edit_file_grouping_by_object(integer, boolean);

CREATE OR REPLACE FUNCTION client_cue.web_edit_file_grouping_by_object(
    arg_file_id integer,
    arg_proceed boolean DEFAULT false)
  RETURNS text AS
$BODY$
/*
   $DESCRIPTION = Process the Objects of RapidCue File Data
            
*/

DECLARE
   v_cmd                    VARCHAR;
   cRows                    INTEGER;
   v_file_row               CLIENT_CUE.RC_FILE%ROWTYPE;
   v_file_name              VARCHAR;
   v_file_format            VARCHAR;
   v_production_id          VARCHAR;
   v_episode_id             VARCHAR;
                            
   v_program                VARCHAR;
   v_episode                VARCHAR;
   v_artist                 VARCHAR;
   v_composer               VARCHAR;
   v_publisher              VARCHAR;
   v_cuesong                VARCHAR;
   v_cuesong_artist         VARCHAR;
   v_cuesong_composer       VARCHAR;
   v_cuesong_publisher      VARCHAR;
   v_cuesong_sub_pub        VARCHAR;
   v_cuesheet               VARCHAR;
   v_cuesheet_usage         VARCHAR;
   v_cuesheet_theme         VARCHAR;
   v_return_value           TEXT;
   v_errored                VARCHAR;   
   v_ready                  VARCHAR;
   v_create_by              INTEGER;
   v_rec                    RECORD;
   v_obj_rec                RECORD;
BEGIN
    v_errored := status_id from client_cue.rc_status where upper(status) = 'ERROR';
    v_ready := status_id from client_cue.rc_status where upper(status) = 'READY';
    SELECT user_id INTO v_create_by FROM client_cue.user WHERE username = current_user;
    SELECT * FROM CLIENT_CUE.RC_FILE WHERE file_id = arg_file_id INTO v_file_row;
    SELECT upper(format_name) FROM CLIENT_CUE.RC_FILE_CONFIG WHERE config_id = v_file_row.file_config_id INTO v_file_format;
    RAISE INFO 'File row = %', v_file_row;
   
    -- Preparing File IDs
    v_production_id := 'CASE WHEN ('||quote_literal(v_file_format)||' ~* ''V2'') AND (upper(f.cue_sheet_type) = ''SERIES'') THEN btrim(f.production_id)::bigint
                             WHEN ('||quote_literal(v_file_format)||' ~* ''V2'') AND upper(f.cue_sheet_type) != ''SERIES'' THEN btrim(f.program_id)::bigint
                        ELSE nextval(''client_cue.seq_rc_program__production_id''::regclass)
                        END';
    v_episode_id := 'CASE WHEN ('||quote_literal(v_file_format)||' ~* ''V2'') AND (upper(f.cue_sheet_type) = ''SERIES'') THEN btrim(f.program_id)::bigint
                          WHEN ('||quote_literal(v_file_format)||' ~* ''V1'') AND upper(f.cue_sheet_type) = ''SERIES'' THEN nextval(''client_cue.seq_rc_episode__episode_id''::regclass)
                     ELSE null::bigint
                     END';
   
    IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'client_cue' AND table_name = 'rc_program') THEN
       IF EXISTS(select 1 from client_cue.rc_program where file_id = arg_file_id and client_id = v_file_row.client_id) THEN
          RAISE EXCEPTION 'The selected file has been already processed.';
       END IF;
    END IF;

    DROP TABLE IF EXISTS tbl_errors;
    CREATE TEMP TABLE tbl_errors (
    message_id serial, 
    message text
    ) ON COMMIT DROP;
    
    -- Check for Cue Sheet Type (SERIES, NON SERIES)
    EXECUTE 'SELECT count(distinct cue_sheet_type) FROM client_cue.rc_file_data where file_id = '||arg_file_id||' GROUP BY upper(btrim(cue_sheet_type)) HAVING count(distinct cue_sheet_type) > 1';
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    IF cRows > 0 THEN 
       RAISE INFO 'Bad data found.';
       EXECUTE 'INSERT INTO tbl_errors(message)
                SELECT ''Incorrect Cue Sheet Type. The type must be either SERIES or NON SERIES''';
    END IF;

    -- PROGRAM
    v_program := 'INSERT INTO CLIENT_CUE.RC_PROGRAM
                  SELECT nextval(''client_cue.seq_program__row_id''::regclass) as row_id,
                         '||arg_file_id||' as file_id,
                         '||v_file_row.client_id||' as client_id,
                         MAX('||v_production_id||') as production_id,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN trim(f.series_name) ELSE f.program_name END as title,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN trim(f.series_production_number)::text ELSE coalesce(trim(f.production_number),f.episode_number)::text END as production_no,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN TRUE ELSE FALSE END as series_flg,
                         pc.row_id as category_id,
                         pv.row_id as version_id,
                         pl.row_id as program_language,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN null::int ELSE f.part_number::int END as part,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN null::varchar ELSE f.network_station END as station,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN false ELSE CASE WHEN upper(coalesce(f.animated_indicator,''No'')) = ''NO'' THEN false ELSE true END END as animated_flg,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN false ELSE CASE WHEN upper(coalesce(f.music_content_indicator,''No'')) = ''NO'' THEN true ELSE false END END as no_music_flg,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN COALESCE(f.series_duration_minute::int,0) ELSE COALESCE(f.program_duration_minute::int,0) END as duration_mm,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN COALESCE(f.series_duration_second::int,0) ELSE COALESCE(f.program_duration_second::int,0) END as duration_ss,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN null::varchar ELSE btrim(regexp_replace(f.prepared_by,''\s{2,}'',chr(32),''g'')) END as prepared_by,
                         max(CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN f.series_instructions ELSE f.program_instructions END) as notes,
                         max(CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN f.series_comments ELSE f.program_comments END) as instructions,
                         false::boolean as matched,
                         null::bigint as original_program_id
                    FROM client_cue.rc_file_data f
                    LEFT JOIN client_cue.program_category pc on upper(pc.category_name) = upper(btrim(regexp_replace(f.series_category,''\s{2,}'',chr(32),''g'')))
                    LEFT JOIN client_cue.program_version pv on upper(pv.version_name) = CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN upper(btrim(regexp_replace(f.series_version,''\s{2,}'',chr(32),''g''))) ELSE  upper(btrim(regexp_replace(f.program_version,''\s{2,}'',chr(32),''g''))) END
                    LEFT JOIN client_cue.program_language pl ON upper(pl.language_name) = upper(btrim(regexp_replace(f.series_language,''\s{2,}'',chr(32),''g'')))
                   WHERE file_id = '||arg_file_id||'
                   GROUP BY  
                         f.cue_sheet_type,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN trim(f.series_name) ELSE f.program_name END,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN trim(f.series_production_number)::text ELSE coalesce(trim(f.production_number),f.episode_number)::text END,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN TRUE ELSE FALSE END,
                         pc.row_id,
                         pv.row_id,
                         f.series_version,
                         pl.row_id,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN null::int ELSE f.part_number::int END,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN null::varchar ELSE f.network_station END,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN false ELSE CASE WHEN upper(coalesce(f.animated_indicator,''No'')) = ''NO'' THEN false ELSE true END END,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN false ELSE CASE WHEN upper(coalesce(f.music_content_indicator,''No'')) = ''NO'' THEN true ELSE false END END,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN COALESCE(f.series_duration_minute::int,0) ELSE COALESCE(f.program_duration_minute::int,0) END,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN COALESCE(f.series_duration_second::int,0) ELSE COALESCE(f.program_duration_second::int,0) END,
                         CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN null::varchar ELSE btrim(regexp_replace(f.prepared_by,''\s{2,}'',chr(32),''g'')) END
                         ';
       RAISE INFO 'v_program = %', v_program;
       EXECUTE v_program;
       -- Check for existent
       RAISE INFO 'Checking for existing program ...';
       v_program := 'SELECT rcp.row_id, p.row_id as original_program_id 
                       FROM client_cue.rc_program rcp 
                       JOIN client_cue.program_'||v_file_row.client_id||' p ON rcp.title = trim(p.title)
                      WHERE rcp.file_id = '||arg_file_id||'
                        AND rcp.client_id = p.client_id
                        AND coalesce(rcp.production_no,'''') = coalesce(p.production_no,'''')
                        AND coalesce(rcp.version_id,0) = coalesce(p.version_id,0)
                        AND coalesce(rcp.program_language,0) = coalesce(p.program_language,0)
                        AND coalesce(rcp.part,0) = coalesce(p.part,0)
                        AND coalesce(rcp.station,'''') = coalesce(p.station,'''')
                        AND coalesce(rcp.animated_flg,false) = coalesce(p.animated_flg,false)
                        AND coalesce(rcp.no_music_flg,false) = coalesce(p.no_music_flg,false)
                        AND coalesce(rcp.duration_mm,0) = coalesce(p.duration_mm,0)
                        AND coalesce(rcp.duration_ss,0) = coalesce(p.duration_ss,0)';
       RAISE INFO 'v_program = %', v_program;
       FOR v_obj_rec IN EXECUTE v_program LOOP
          UPDATE client_cue.rc_program SET original_program_id = v_obj_rec.original_program_id, matched = true WHERE row_id = v_obj_rec.row_id;
       END LOOP;
       -- Update file rc_production_id for easy later join only when FORMAT is V1
       IF v_file_format ~* 'V1' THEN
          RAISE INFO 'Updates file production_id for easy later join ...';
          v_cmd := 'UPDATE client_cue.rc_file_data f
                       SET rc_production_id = rcp.production_id::varchar
                      FROM client_cue.rc_program rcp
                      LEFT JOIN client_cue.program_category pc on pc.row_id = rcp.category_id
                      LEFT JOIN client_cue.program_version pv on pv.row_id = rcp.version_id
                      LEFT JOIN client_cue.program_language pl ON pl.row_id = rcp.program_language
                     WHERE f.file_id = '||arg_file_id||'
                       AND f.file_id = rcp.file_id
                       AND rcp.title = CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN trim(f.series_name) ELSE trim(f.program_name) END
                       AND COALESCE(rcp.production_no,'''') = COALESCE(CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN trim(f.series_production_number)::text ELSE coalesce(trim(f.production_number),f.episode_number)::text END,'''')
                       AND rcp.series_flg = CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN TRUE ELSE FALSE END
                       AND COALESCE(upper(pc.category_name),'''') = COALESCE(upper(btrim(regexp_replace(f.series_category,''\s{2,}'',chr(32),''g''))),'''')
                       AND COALESCE(upper(pv.version_name),'''') = COALESCE(CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN upper(btrim(regexp_replace(f.series_version,''\s{2,}'',chr(32),''g''))) ELSE upper(btrim(regexp_replace(f.program_version,''\s{2,}'',chr(32),''g''))) END,'''')
                       AND COALESCE(upper(pl.language_name),'''') = COALESCE(upper(btrim(regexp_replace(f.series_language,''\s{2,}'',chr(32),''g''))),'''')
                       AND COALESCE(rcp.part,0) = COALESCE(CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN null::int ELSE f.part_number::int END,0)
                       AND COALESCE(rcp.station,'''') = COALESCE(CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN null::varchar ELSE f.network_station END,'''')
                       AND rcp.animated_flg = CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN false ELSE CASE WHEN upper(coalesce(f.animated_indicator,''No'')) = ''NO'' THEN false ELSE true END END
                       AND rcp.no_music_flg = CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN false ELSE CASE WHEN upper(coalesce(f.music_content_indicator,''No'')) = ''NO'' THEN true ELSE false END END
                       AND rcp.duration_mm = CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN COALESCE(f.series_duration_minute::int,0) ELSE COALESCE(f.program_duration_minute::int,0) END
                       AND rcp.duration_ss = CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN COALESCE(f.series_duration_second::int,0) ELSE COALESCE(f.program_duration_second::int,0) END';
          RAISE INFO 'v_cmd = %', v_cmd;
          EXECUTE v_cmd;
       END IF;
             
    -- EPISODE
    v_episode  := 'INSERT INTO CLIENT_CUE.RC_EPISODE
                   SELECT nextval(''client_cue.seq_episode__row_id''::regclass) as row_id,
                          null as group_id, -- deprecated
                          '||arg_file_id||' as file_id,
                          '||v_file_row.client_id||' as client_id,
                         '||v_episode_id||' as episode_id,
                          rcp.row_id as program_id,
                          trim(f.program_name) as title,
                          coalesce(nullif(trim(f.production_number),''''),f.episode_number)::text as episode_no,
                          f.epn::text as epn_no,
                          pv.row_id as version_id,
                          null::int as episode_language,
                          f.season::int,
                          f.part_number::int as part,
                          f.network_station as station,
                          CASE WHEN upper(coalesce(f.animated_indicator,''No'')) = ''NO'' THEN false ELSE true END as animated_flg,
                          CASE WHEN upper(coalesce(f.music_content_indicator,''No'')) = ''NO'' THEN true ELSE false END as no_music_flg,
                          COALESCE(f.program_duration_minute::int,0) as duration_mm,
                          COALESCE(f.program_duration_second::int,0) as duration_ss,
                          CASE WHEN upper(cue_sheet_type) = ''SERIES'' THEN btrim(regexp_replace(f.prepared_by,''\s{2,}'',chr(32),''g'')) ELSE null::varchar END as prepared_by,
                          max(f.program_instructions) as notes,
                          max(f.program_comments) as instrunctions,
                          false::boolean as matched,
                          null::bigint as original_episode_id
                     FROM client_cue.rc_file_data f
                     LEFT JOIN client_cue.program_version pv on upper(pv.version_name) = upper(btrim(regexp_replace(f.program_version,''\s{2,}'',chr(32),''g'')))
                     -- LEFT JOIN client_cue.program_language pl ON upper(pl.language_name) = upper(btrim(regexp_replace(f.series_language,''\s{2,}'',chr(32),''g'')))
                     JOIN client_cue.rc_program rcp ON rcp.file_id = f.file_id AND rcp.production_id = coalesce(f.production_id::bigint, f.rc_production_id::bigint) 
                    WHERE f.file_id = '||arg_file_id||'
                    GROUP BY
                          f.program_id,
                          f.cue_sheet_type,
                          rcp.row_id,
                          f.program_name,
                          coalesce(nullif(trim(f.production_number),''''),f.episode_number)::text,
                          f.program_version,
                          f.animated_indicator,
                          f.music_content_indicator,
                          f.program_duration_second,
                          f.series_name,
                          f.program_duration_minute,
                          f.series_production_number,
                          f.episode_number,
                          f.epn,
                          pv.row_id,
                          f.season,
                          f.part_number,
                          f.network_station,
                          CASE WHEN upper(coalesce(f.animated_indicator,''No'')) = ''NO'' THEN false ELSE true END,
                          CASE WHEN upper(coalesce(f.music_content_indicator,''No'')) = ''NO'' THEN true ELSE false END,
                          f.series_duration_minute::int,
                          f.series_duration_second::int,
                          CASE WHEN upper(cue_sheet_type) = ''SERIES'' THEN btrim(regexp_replace(f.prepared_by,''\s{2,}'',chr(32),''g'')) ELSE null::varchar END
                          -- f.program_comments,
                          -- f.program_instructions
                         ';
       RAISE INFO 'v_episode = %', v_episode;
       EXECUTE v_episode;
       GET DIAGNOSTICS cRows = ROW_COUNT;
       -- Check for existent
       IF cRows > 0 THEN 
          RAISE INFO 'Checking for existing episode ...';
          v_episode := 'SELECT rce.row_id, e.row_id as original_episode_id
                          FROM client_cue.rc_episode rce 
                          JOIN client_cue.episode_'||v_file_row.client_id||' e ON rce.title = e.title
                         WHERE rce.file_id = '||arg_file_id||'
                           AND rce.client_id = e.client_id
                           AND coalesce(rce.episode_no,'''') = coalesce(e.episode_no,'''')
                           AND coalesce(rce.version_id,0) = coalesce(e.version_id,0)
                           AND coalesce(rce.season,0) = coalesce(e.season,0)
                           AND coalesce(rce.part,0) = coalesce(e.part,0)
                           AND coalesce(rce.station,'''') = coalesce(e.station,'''')
                           AND coalesce(rce.animated_flg,false) = coalesce(e.animated_flg,false)
                           AND coalesce(rce.no_music_flg,false) = coalesce(e.no_music_flg,false)';
         RAISE INFO 'v_episode = %', v_episode;
         FOR v_obj_rec IN EXECUTE v_episode LOOP
            UPDATE client_cue.rc_episode SET original_episode_id = v_obj_rec.original_episode_id, matched = true WHERE row_id = v_obj_rec.row_id;
         END LOOP;
       END IF;
       -- Update file rc_program_id for easy later join only when FORMAT is V1
       IF v_file_format ~* 'V1' THEN
          RAISE INFO 'Updates file rc_program_id for easy later join ...';
          v_cmd := 'UPDATE client_cue.rc_file_data f
                       SET rc_program_id = rce.episode_id::varchar
                      FROM client_cue.rc_episode rce
                      LEFT JOIN client_cue.program_version pv on pv.row_id = rce.version_id
                      JOIN client_cue.rc_program rcp ON rcp.file_id = rce.file_id AND rcp.row_id = rce.program_id
                     WHERE f.file_id = '||arg_file_id||'
                       AND rce.file_id = f.file_id
                       AND rce.title = trim(f.program_name) 
                       AND rce.title = CASE WHEN upper(f.cue_sheet_type) = ''SERIES'' THEN trim(f.program_name) ELSE null::varchar END
                       AND COALESCE(rce.episode_no,'''') = COALESCE(nullif(trim(f.production_number),''''),f.episode_number)
                       AND COALESCE(rce.epn_no,'''') = COALESCE(f.epn::text,'''')
                       AND COALESCE(upper(pv.version_name),'''') = COALESCE(upper(pv.version_name),'''')
                       AND COALESCE(rce.season,0) = COALESCE(f.season::int,0)
                       AND COALESCE(rce.part,0) = COALESCE(f.part_number::int,0)
                       AND COALESCE(rce.station,'''') = COALESCE(f.network_station,'''')
                       AND rce.animated_flg = CASE WHEN upper(coalesce(f.animated_indicator,''No'')) = ''NO'' THEN false ELSE true END
                       AND rce.no_music_flg = CASE WHEN upper(coalesce(f.music_content_indicator,''No'')) = ''NO'' THEN true ELSE false END
                       AND rce.duration_mm = COALESCE(f.program_duration_minute::int,0)
                       AND rce.duration_ss = COALESCE(f.program_duration_second::int,0)';
          RAISE INFO 'v_cmd = %', v_cmd;
          EXECUTE v_cmd;
       END IF;      

    -- ARTIST
    v_artist   := 'INSERT INTO CLIENT_CUE.RC_ARTIST
                   WITH art AS (SELECT '||arg_file_id||' as file_id,
                                       '||v_file_row.client_id||' as client_id,
                                       upper(btrim(regexp_replace(artist,''\s{2,}'',chr(32),''g''))) as artist_name
                             FROM client_cue.rc_file_data
                            WHERE file_id = '||arg_file_id||' 
                              AND coalesce(btrim(artist),'''') != ''''
                            GROUP BY 3
                           )
                   SELECT nextval(''client_cue.seq_artist__row_id''::regclass) as row_id,
                          a.file_id,
                          a.client_id,
                          a.artist_name,
                          null::varchar as similar_artist_name, -- deprecated
                          false::boolean as matched,
                          null::bigint as original_artist_id
                     FROM art a
                    GROUP BY 2,3,4';
       RAISE INFO 'v_artist = %', v_artist;
       EXECUTE v_artist;
       GET DIAGNOSTICS cRows = ROW_COUNT;
       -- Check for existent
       IF cRows > 0 THEN   
          RAISE INFO 'Checking for existing artist ...';
          v_artist := 'SELECT rca.row_id, a.row_id as original_artist_id
                         FROM client_cue.rc_artist rca
                         JOIN client_cue.artist a ON rca.artist_name = a.artist_name
                        WHERE rca.file_id = '||arg_file_id||'
                          AND rca.client_id = a.client_id';
          RAISE INFO 'v_artist = %', v_artist;
          FOR v_obj_rec IN EXECUTE v_artist LOOP
             UPDATE client_cue.rc_artist SET original_artist_id = v_obj_rec.original_artist_id, matched = true WHERE row_id = v_obj_rec.row_id;
          END LOOP;
       END IF;
       -- ARTIST SYSTEMATIC MATCH AGAINST HISTORY MERGE; set systematic_main_row_id to history main if matched by name
       v_cmd := 'WITH hs_merge AS ( SELECT s.file_id,
                                           s.artist_id,
                                           s.similar_artist_id as merged_artist_id,
                                           coalesce(a.artist_name,a2.artist_name) as merged_artist_name,
                                           s.group_id,
                                           s.bin,
                                           s.merge,
                                           s.main
                                      FROM client_cue.rc_artist_similarity_history s
                                      LEFT JOIN client_cue.rc_artist_history a ON a.file_id = s.file_id AND a.row_id = s.similar_artist_id AND s.bin is false
                                      LEFT JOIN client_cue.artist_'||v_file_row.client_id||' a2 ON a2.row_id = s.similar_artist_id AND s.bin is true
                                     WHERE s.group_id is not null
                                       AND s.merge is true
                                     ORDER BY s.group_id
                                   ),
                       hs_main AS  (SELECT s.file_id,
                                           s.artist_id,
                                           s.similar_artist_id as main_artist_id,
                                           s.group_id,
                                           s.bin,
                                           s.main
                                      FROM client_cue.rc_artist_similarity_history s
                                      LEFT JOIN client_cue.rc_artist_history a ON a.file_id = s.file_id AND a.row_id = s.similar_artist_id AND s.bin is false
                                      LEFT JOIN client_cue.artist_'||v_file_row.client_id||' a2 ON a2.row_id = s.similar_artist_id AND s.bin is true
                                     WHERE s.group_id is not null
                                       AND s.main is true
                                     ORDER BY s.group_id
                                   ),
                       hs_match AS (SELECT a.row_id, a.artist_name, hs_main.main_artist_id
                                       FROM hs_merge hm
                                       JOIN client_cue.rc_artist a ON a.file_id = '||arg_file_id||' AND a.artist_name = hm.merged_artist_name
                                       JOIN hs_main ON hs_main.group_id = hm.group_id
                                   )
            UPDATE client_cue.rc_artist a
               SET invalid_dt = now(),
                   invalid_by = '||coalesce(v_create_by,0)||',
                   systematic_main_row_id = m.main_artist_id
              FROM hs_match m
             WHERE a.file_id = '||arg_file_id||'
               AND a.row_id = m.row_id';
       RAISE INFO 'Artist Systematic match against history merge v_cmd = %', v_cmd;
       EXECUTE v_cmd;

   -- Catch Artist Similarities from file and bin
    v_cmd := 'INSERT INTO CLIENT_CUE.RC_ARTIST_SIMILARITY
              WITH Sim AS (SELECT max(a.file_id) file_id,
                                  max(a.row_id) row_id,
                                  a2.row_id similar_artist_id,
                                  false as bin
                             FROM client_cue.rc_artist a
                             JOIN client_cue.rc_artist a2 ON a.file_id = a2.file_id
                                         AND a.artist_name != a2.artist_name
                                         AND (similarity(a.artist_name,a2.artist_name) > 0.94 or (a.artist_name ilike concat(''%'',a2.artist_name,''%'') and abs(char_length(a.artist_name)-char_length(a2.artist_name)) < 4))
                            WHERE a.file_id = '||arg_file_id||'
                              AND a.systematic_main_row_id IS NULL
                              AND a2.systematic_main_row_id IS NULL
                            GROUP BY 3
                           UNION
                           SELECT max(a.file_id) file_id,
                                  max(a.row_id) row_id,
                                  a2.row_id similar_artist_id,
                                  true as bin
                             FROM client_cue.rc_artist a
                             JOIN client_cue.artist_'||v_file_row.client_id||' a2 ON (a.artist_name = a2.artist_name)
                                                                 OR (similarity(a.artist_name,a2.artist_name) > 0.94 or (a.artist_name ilike concat(''%'',a2.artist_name,''%'') and abs(char_length(a.artist_name)-char_length(a2.artist_name)) < 4))
                            WHERE a.file_id = '||arg_file_id||'
                              AND a.systematic_main_row_id IS NULL
                            GROUP BY 3
                            ORDER BY 3
                           ),
                   Sim_enum AS (SELECT sim.*, row_number() over( Order by row_id) rn FROM sim),
                   Ord AS (SELECT file_id, unnest(string_to_array(row_id||'',''||similar_artist_id, '','')) pair_id, bin, rn
                             FROM sim_enum
                            ORDER BY rn, pair_id
                           ),
                   Pairs AS (select file_id, array_agg(pair_id Order by pair_id desc) pair_id, bin, rn from ord group by file_id, bin, rn)
              SELECT file_id,
                     pair_id[1]::bigint artist_id, 
                     pair_id[2]::bigint similar_artist_id,
                     bin,
                     false as merge,
                     false as main
                FROM pairs
               GROUP BY file_id, pair_id[1], pair_id[2], bin';
    RAISE INFO 'Catching Artist Similarities v_cmd = %', v_cmd;
    EXECUTE v_cmd;
    -- switches messed up bin ids (bin id should be similar_artist_id)
    v_cmd := 'WITH mess AS (SELECT s.* FROM client_cue.rc_artist_similarity s join client_cue.artist_'||v_file_row.client_id||' a on a.row_id = s.artist_id where s.file_id = '||arg_file_id||' AND s.bin is true)
              UPDATE client_cue.rc_artist_similarity cs
                 SET artist_id = m.similar_artist_id,
                     similar_artist_id = m.artist_id
                FROM mess m
               WHERE cs.artist_id = m.artist_id
                 AND cs.similar_artist_id = m.similar_artist_id
                 AND cs.bin is true';
    EXECUTE v_cmd;
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    RAISE INFO 'Switched Artist ids = %', cRows; 

    -- CATCHING BAD DATA; Check for incorrect Composer Name
    v_cmd := 'SELECT btrim(array_to_string(array_agg(coalesce(trim(upper(coalesce(trim(f.last_name),'''')||'', ''||coalesce(trim(f.first_name),'''')||'' ''||coalesce(trim(f.middle_name),''''))),'''')),chr(13))) composer_name
                FROM client_cue.rc_file_data f
                LEFT JOIN client_cue.composer_role cr ON upper(regexp_replace(cr.role_name,''\W+'',chr(32),''g'')) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                LEFT JOIN client_cue.publisher_role pr ON upper(regexp_replace(pr.role_name,''\W+'',chr(32),''g'')) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
               WHERE f.file_id = '||arg_file_id||'
                 AND pr.row_id IS NULL
                 AND coalesce(trim(f.artist),'''') = ''''
                 AND (btrim(regexp_replace(coalesce(f.last_name,'''')||'', ''||coalesce(f.first_name,'''')||'' ''||coalesce(f.middle_name,''''),''\s{2,}'',chr(32),''g'')) = '''' and cr.row_id is not null)';
    RAISE INFO 'v_cmd = %', v_cmd;
    EXECUTE v_cmd INTO v_return_value;
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    IF cRows > 0 AND coalesce(v_return_value,'') != '' THEN
       RAISE INFO 'Bad data found.';
       EXECUTE 'INSERT INTO tbl_errors(message)
                SELECT chr(13)||''Incorrect Composer name: '||regexp_replace(coalesce(v_return_value,''::text),E'''',chr(32))||'''';
    END IF;
   
    -- CATCHING BAD DATA; Check for incorrect Company/Composer Role
    v_cmd := 'SELECT btrim(array_to_string(array_agg(upper(btrim(regexp_replace(f.role,''\W+'',chr(32),''g'')))||''   ''||
                                                     upper(regexp_replace(coalesce(trim(f.company_name),''''),''\W+'',chr(32),''g''))),chr(13)))
                FROM client_cue.rc_file_data f
                LEFT JOIN client_cue.publisher_role pr ON upper(regexp_replace(pr.role_name,''\W+'',chr(32),''g'')) = upper(btrim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                LEFT JOIN client_cue.composer_role cr ON upper(regexp_replace(cr.role_name,''\W+'',chr(32),''g'')) = upper(btrim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
               WHERE f.file_id = '||arg_file_id||'
                 AND pr.row_id is null
                 AND cr.row_id is null';
    RAISE INFO 'v_cmd = %', v_cmd;
    EXECUTE v_cmd INTO v_return_value;
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    IF cRows > 0 AND coalesce(v_return_value,'') != '' THEN
       RAISE INFO 'Bad data found.';
       EXECUTE 'INSERT INTO tbl_errors(message)
                SELECT chr(13)||''Incorrect Company/Composer role: '||regexp_replace(coalesce(v_return_value,''::text),E'''',chr(32))||'''';
    END IF;
   
    -- COMPOSER
    v_composer := 'WITH comp AS(SELECT '||arg_file_id||' as file_id,
                                        '||v_file_row.client_id||' as client_id,
                                        upper(btrim(last_name)) as last_name,
                                        upper(btrim(first_name)) as first_name,
                                        upper(btrim(middle_name)) as middle_name,
                                        upper(btrim(regexp_replace(coalesce(last_name,'''')||'',''||chr(32)||coalesce(first_name,'''')||chr(32)||coalesce(middle_name,''''),''\s{2,}'',chr(32),''g''))) as composer_name,
                                        upper(trim(role)) as role,
                                        CASE WHEN UPPER(trim(affiliation)) = ''UNKNOWN'' THEN ''UNK''
                                             WHEN UPPER(trim(affiliation)) = ''NO SOCIETY'' THEN ''NS''
                                             WHEN UPPER(trim(affiliation)) = ''PUBLIC DOMAIN'' THEN ''PD''
                                        ELSE UPPER(nullif(trim(affiliation),'''')) 
                                        END as affiliation,
                                        ssn::integer as ssn_code,
                                        coalesce(interested_party_id, rc_interested_party_id) as interested_party_id
                                   FROM client_cue.rc_file_data 
                                   JOIN client_cue.composer_role cr ON upper(cr.role_name) = upper(trim(regexp_replace(role,''\W+'',chr(32),''g'')))
                                  WHERE file_id = '||arg_file_id||'
                                  GROUP BY 1,2,3,4,5,6,7,8,9,10
                                  ORDER BY 6
                                 )
                   INSERT INTO CLIENT_CUE.RC_COMPOSER
                   SELECT nextval(''client_cue.seq_composer__row_id''::regclass) as row_id,
                          c.file_id,
                          c.client_id,
                          c.last_name,
                          c.first_name,
                          c.middle_name,
                          c.composer_name,
                          null::varchar as similar_composer_name, -- deprecated
                          c.affiliation,
                          s.society_id,
                          null::bigint cae_code,
                          c.ssn_code,
                          null::varchar as notes,
                          null::varchar as address,
                          null::varchar as zip_code,
                          null::integer as country,
                          null::varchar as phone,
                          null::varchar as email,
                          null::varchar as city,
                          null::varchar as state,
                          false::boolean as matched,
                          null::bigint as original_composer_id,
                          CASE WHEN s.society_id IS NULL THEN c.affiliation ELSE null::varchar END as src_society,
                          CASE WHEN '||quote_literal(v_file_format)||' ~* ''V2'' THEN null::bigint /*btrim(c.interested_party_id)::bigint*/ ELSE nextval(''client_cue.seq_party__interested_party_id''::regclass) END as interested_party_id -- cannot rely on
                     FROM comp c
                     LEFT JOIN songdex.societies s on upper(s.acronym) = c.affiliation AND s.society_flg is true
                    GROUP BY 2,3,4,5,6,7,9,10,12,23
                    ORDER BY 7';
       RAISE INFO 'v_composer = %', v_composer;
       EXECUTE v_composer;
       GET DIAGNOSTICS cRows = ROW_COUNT;
       -- Check for existent
       IF cRows > 0 THEN   
          RAISE INFO 'Checking for existing composer ...';
          v_composer := 'SELECT rcc.row_id, c.row_id as original_composer_id
                           FROM client_cue.rc_composer rcc
                           JOIN client_cue.composer c ON coalesce(rcc.last_name,'''') = coalesce(c.last_name,'''')
                                                     AND coalesce(rcc.first_name,'''') = coalesce(c.first_name,'''')
                                                     AND coalesce(rcc.middle_name,'''') = coalesce(c.middle_name,'''')
                                                     AND coalesce(rcc.society_id,0) = coalesce(c.society_id,0)
                           WHERE rcc.file_id = '||arg_file_id||'
                             AND rcc.client_id = c.client_id';
          RAISE INFO 'v_composer = %', v_composer;
          FOR v_obj_rec IN EXECUTE v_composer LOOP
             UPDATE client_cue.rc_composer SET original_composer_id = v_obj_rec.original_composer_id, matched = true WHERE row_id = v_obj_rec.row_id;
          END LOOP;
       END IF;
       -- Update file rc_interested_party_id for easy later join only when FORMAT is V1
       IF v_file_format ~* 'V1' THEN
          RAISE INFO 'Updates file composer rc_interested_party_id for easy later join ...';
          v_cmd := 'UPDATE client_cue.rc_file_data f
                       SET rc_interested_party_id = rcc.interested_party_id::varchar
                      FROM client_cue.rc_composer rcc
                     WHERE f.file_id = '||arg_file_id||'
                       AND rcc.composer_name = upper(btrim(regexp_replace(coalesce(f.last_name,'''')||'',''||chr(32)||coalesce(f.first_name,'''')||chr(32)||coalesce(f.middle_name,''''),''\s{2,}'',chr(32),''g'')))
                       AND rcc.affiliation = upper(coalesce(trim(f.affiliation),''''))';
          RAISE INFO 'v_cmd = %', v_cmd;
          EXECUTE v_cmd;
       END IF;      


       -- COMPOSER SYSTEMATIC MATCH AGAINST HISTORY MERGE; set systematic_main_row_id to history main if matched by name
       v_cmd := 'WITH hs_merge AS ( SELECT s.file_id,
                                           s.composer_id,
                                           s.similar_composer_id as merged_composer_id,
                                           coalesce(c.composer_name,c2.composer_name) as merged_composer_name,
                                           coalesce(c.society_id,c2.society_id) society_id,
                                           s.group_id,
                                           s.bin,
                                           s.merge,
                                           s.main
                                      FROM client_cue.rc_composer_similarity_history s
                                      LEFT JOIN client_cue.rc_composer_history c ON c.file_id = s.file_id and c.row_id = s.similar_composer_id and s.bin is false
                                      LEFT JOIN client_cue.composer_'||v_file_row.client_id||' c2 ON c2.row_id = s.similar_composer_id and s.bin is true
                                     WHERE s.group_id is not null
                                       AND s.merge is true
                                     ORDER BY s.group_id
                                   ),
                       hs_main AS  (SELECT s.file_id,
                                           s.composer_id,
                                           s.similar_composer_id as main_composer_id,
                                           s.group_id,
                                           s.bin,
                                           s.main
                                      FROM client_cue.rc_composer_similarity_history s
                                      LEFT JOIN client_cue.rc_composer_history c ON c.file_id = s.file_id and c.row_id = s.similar_composer_id and s.bin is false
                                      LEFT JOIN client_cue.composer_'||v_file_row.client_id||' c2 ON c2.row_id = s.similar_composer_id and s.bin is true
                                     WHERE s.group_id is not null
                                       AND s.main is true
                                     ORDER BY s.group_id
                                   ),
                       hs_match AS (SELECT c.row_id, c.composer_name, hs_main.main_composer_id
                                       FROM hs_merge hm
                                       JOIN client_cue.rc_composer c ON c.file_id = '||arg_file_id||' AND c.composer_name = hm.merged_composer_name AND coalesce(c.society_id,0) = coalesce(hm.society_id,0)
                                       JOIN hs_main ON hs_main.group_id = hm.group_id
                                   )
            UPDATE client_cue.rc_composer c
               SET invalid_dt = now(),
                   invalid_by = '||coalesce(v_create_by,0)||',
                   systematic_main_row_id = m.main_composer_id
              FROM hs_match m
             WHERE c.file_id = '||arg_file_id||'
               AND c.row_id = m.row_id';
       RAISE INFO 'Composer Systematic match against history merge v_cmd = %', v_cmd;
       EXECUTE v_cmd;


   -- Catch Composer Similarities from file and bin
    v_cmd := 'INSERT INTO CLIENT_CUE.RC_COMPOSER_SIMILARITY
              WITH Sim AS (SELECT max(c.file_id) file_id,
                                  max(c.row_id) row_id,
                                  c2.row_id similar_composer_id,
                                  false as bin
                             FROM client_cue.rc_composer c
                             JOIN client_cue.rc_composer c2 ON c.file_id = c2.file_id
                                         AND c.composer_name != c2.composer_name
                                         AND (similarity(c.composer_name,c2.composer_name) > 0.94 or (c.composer_name ilike concat(''%'',c2.composer_name,''%'') and abs(char_length(c.composer_name)-char_length(c2.composer_name)) < 4))
                            WHERE c.file_id = '||arg_file_id||'
                              AND c.systematic_main_row_id IS NULL
                              AND c2.systematic_main_row_id IS NULL
                            GROUP BY 3
                           UNION
                           SELECT max(c.file_id) file_id,
                                  max(c.row_id) row_id,
                                  c2.row_id similar_composer_id,
                                  true as bin
                             FROM client_cue.rc_composer c
                             JOIN client_cue.composer_'||v_file_row.client_id||' c2 ON (c.composer_name = c2.composer_name)
                                                            OR (similarity(c.composer_name,c2.composer_name) > 0.94 or (c.composer_name ilike concat(''%'',c2.composer_name,''%'') and abs(char_length(c.composer_name)-char_length(c2.composer_name)) < 4))
                            WHERE c.file_id = '||arg_file_id||'
                              AND c.systematic_main_row_id IS NULL
                            GROUP BY 3
                            ORDER BY 3
                          ),
                   Sim_enum AS (SELECT sim.*, row_number() over( Order by row_id) rn FROM sim ),
                   Ord AS (SELECT file_id, unnest(string_to_array(row_id||'',''||similar_composer_id, '','')) pair_id, bin, rn
                             FROM sim_enum 
                            ORDER BY rn, pair_id
                           ),
                   Pairs AS (select file_id, array_agg(pair_id Order by pair_id desc) pair_id, bin, rn from ord group by file_id, bin, rn)
              SELECT file_id,
                     pair_id[1]::bigint composer_id,
                     pair_id[2]::bigint similar_composer_id,
                     bin,
                     false as merge,
                     false as main
                FROM pairs
               GROUP BY file_id, pair_id[1], pair_id[2], bin';
    RAISE INFO 'Catching Composer Similarities v_cmd = %', v_cmd;
    EXECUTE v_cmd;
    -- switches messed up bin ids (bin id should be similar_composer_id)
    v_cmd := 'WITH mess AS (SELECT s.* FROM client_cue.rc_composer_similarity s join client_cue.composer_'||v_file_row.client_id||' c on c.row_id = s.composer_id where s.file_id = '||arg_file_id||' AND s.bin is true)
              UPDATE client_cue.rc_composer_similarity cs
                 SET composer_id = m.similar_composer_id,
                     similar_composer_id = m.composer_id
                FROM mess m
               WHERE cs.composer_id = m.composer_id
                 AND cs.similar_composer_id = m.similar_composer_id
                 AND cs.bin is true';
    EXECUTE v_cmd;
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    RAISE INFO 'Switched Composer ids = %', cRows;
    
    -- CATCHING BAD DATA; Check for correct Publisher Role/Name
    v_cmd := 'SELECT array_to_string(array_agg(coalesce(trim(f.company_name),'''')),chr(13)) publisher_name
                FROM client_cue.rc_file_data f
                LEFT JOIN client_cue.composer_role cr ON upper(regexp_replace(cr.role_name,''\W+'',chr(32),''g'')) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                LEFT JOIN client_cue.publisher_role pr ON upper(regexp_replace(pr.role_name,''\W+'',chr(32),''g'')) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
               WHERE f.file_id = '||arg_file_id||'
                 AND cr.row_id IS NULL
                 AND coalesce(trim(f.artist),'''') = ''''
                 AND (
                      (btrim(regexp_replace(f.company_name,''\s{2,}'',chr(32),''g'')) = '''' and pr.row_id is not null)
                      OR
                      (btrim(regexp_replace(f.company_name,''\s{2,}'',chr(32),''g'')) != '''' and pr.row_id is null)
                     )
             ';
    EXECUTE v_cmd INTO v_return_value;
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    IF cRows > 0 AND coalesce(v_return_value,'') != '' THEN
       RAISE INFO 'Bad data found.';
       EXECUTE 'INSERT INTO tbl_errors(message)
                SELECT chr(13)||''Incorrect publisher role/name: '||regexp_replace(coalesce(v_return_value,''::text),E'''',chr(32))||'''';
    END IF;

    -- PUBLISHER
    v_publisher := 'WITH pub AS(SELECT '||arg_file_id||' as file_id,
                                       '||v_file_row.client_id||' as client_id,
                                       upper(btrim(regexp_replace(company_name,''\s{2,}'',chr(32),''g''))) publisher_name,
                                       upper(trim(role)) as role,
                                       CASE WHEN UPPER(trim(affiliation)) = ''UNKNOWN'' THEN ''UNK''
                                            WHEN UPPER(trim(affiliation)) = ''NO SOCIETY'' THEN ''NS''
                                            WHEN UPPER(trim(affiliation)) = ''PUBLIC DOMAIN'' THEN ''PD''
                                       ELSE UPPER(nullif(trim(affiliation),'''')) 
                                       END as affiliation,
                                       coalesce(interested_party_id, rc_interested_party_id) as interested_party_id
                                  FROM client_cue.rc_file_data 
                                  JOIN client_cue.publisher_role pr ON upper(pr.role_name) = upper(trim(regexp_replace(role,''\W+'',chr(32),''g'')))
                                 WHERE file_id = '||arg_file_id||'
                                   AND coalesce(trim(company_name),'''') != ''''
                                 GROUP BY 1,2,3,4,5,6
                                 ORDER BY 3
                                )
                    INSERT INTO CLIENT_CUE.RC_PUBLISHER
                    SELECT nextval(''client_cue.seq_publisher__row_id''::regclass) as row_id,
                           p.file_id,
                           p.client_id,
                           p.publisher_name,
                           null::varchar as similar_publisher_name, -- deprecated
                           p.affiliation,
                           s.society_id,
                           null::bigint as cae_code,
                           null::varchar as address,
                           null::varchar as zip_code,
                           null::integer as country,
                           null::varchar as phone,
                           null::varchar as email,
                           null::varchar as notes,
                           false::boolean as matched,
                           null::bigint as original_publisher_id,
                           CASE WHEN s.society_id IS NULL THEN p.affiliation ELSE null::varchar END as src_society,
                           null::varchar as city,
                           null::varchar as state,
                           CASE WHEN '||quote_literal(v_file_format)||' ~* ''V2'' THEN null::bigint /*p.interested_party_id::bigint*/ ELSE nextval(''client_cue.seq_party__interested_party_id''::regclass) END as interested_party_id -- cannot rely on
                      FROM pub p
                      LEFT JOIN songdex.societies s on upper(s.acronym) = p.affiliation AND s.society_flg is true
                     GROUP BY 2,3,4,6,7,8,17
                     ORDER BY 4';
       RAISE INFO 'v_publisher = %', v_publisher;
       EXECUTE v_publisher;
       GET DIAGNOSTICS cRows = ROW_COUNT;
       -- Check for existent
       IF cRows > 0 THEN   
          RAISE INFO 'Checking for existing publisher ...';
          v_publisher := 'SELECT rcp.row_id, p.row_id as original_publisher_id
                            FROM client_cue.rc_publisher rcp
                            JOIN client_cue.publisher p ON upper(rcp.publisher_name) = upper(p.publisher_name)
                                                       AND coalesce(rcp.society_id,0) = coalesce(p.society_id,0)
                           WHERE rcp.file_id = '||arg_file_id||'
                             AND rcp.client_id = p.client_id';
          RAISE INFO 'v_publisher = %', v_publisher;
          FOR v_obj_rec IN EXECUTE v_publisher LOOP
             UPDATE client_cue.rc_publisher SET original_publisher_id = v_obj_rec.original_publisher_id, matched = true WHERE row_id = v_obj_rec.row_id;
          END LOOP;
       END IF;
       -- Update file rc_interested_party_id for easy later join only when FORMAT is V1
       IF v_file_format ~* 'V1' THEN
          RAISE INFO 'Updates file publisher rc_interested_party_id for easy later join ...';
          v_cmd := 'UPDATE client_cue.rc_file_data f
                       SET rc_interested_party_id = rcp.interested_party_id::varchar
                      FROM client_cue.rc_publisher rcp
                     WHERE f.file_id = '||arg_file_id||'
                       AND rcp.publisher_name = upper(btrim(regexp_replace(f.company_name,''\s{2,}'',chr(32),''g'')))
                       AND rcp.affiliation = CASE WHEN UPPER(trim(f.affiliation)) = ''UNKNOWN'' THEN ''UNK''
                                                  WHEN UPPER(trim(f.affiliation)) = ''NO SOCIETY'' THEN ''NS''
                                                  WHEN UPPER(trim(f.affiliation)) = ''PUBLIC DOMAIN'' THEN ''PD''
                                             ELSE UPPER(nullif(trim(f.affiliation),'''')) 
                                             END';
          RAISE INFO 'v_cmd = %', v_cmd;
          EXECUTE v_cmd;
       END IF;
       -- PUBLISHER SYSTEMATIC MATCH AGAINST HISTORY MERGE; set systematic_main_row_id to history main if matched by name
       v_cmd := 'WITH hs_merge AS ( SELECT s.file_id,
                                           s.publisher_id,
                                           s.similar_publisher_id as merged_publisher_id,
                                           coalesce(p.publisher_name,p2.publisher_name) as merged_publisher_name,
                                           coalesce(p.society_id,p2.society_id) society_id,
                                           s.group_id,
                                           s.bin,
                                           s.merge,
                                           s.main
                                      FROM client_cue.rc_publisher_similarity_history s
                                      LEFT JOIN client_cue.rc_publisher_history p ON p.file_id = s.file_id and p.row_id = s.similar_publisher_id and s.bin is false
                                      LEFT JOIN client_cue.publisher_'||v_file_row.client_id||' p2 ON p2.row_id = s.similar_publisher_id and s.bin is true
                                     WHERE s.group_id is not null
                                       AND s.merge is true
                                     ORDER BY s.group_id
                                   ),
                      hs_main AS  ( SELECT s.file_id,
                                           s.publisher_id,
                                           s.similar_publisher_id as main_publisher_id,
                                           s.group_id,
                                           s.bin,
                                           s.main
                                      FROM client_cue.rc_publisher_similarity_history s
                                      LEFT JOIN client_cue.rc_publisher_history p ON p.file_id = s.file_id and p.row_id = s.similar_publisher_id and s.bin is false
                                      LEFT JOIN client_cue.publisher_'||v_file_row.client_id||' p2 ON p2.row_id = s.similar_publisher_id and s.bin is true
                                     WHERE s.group_id is not null
                                       AND s.main is true
                                     ORDER BY s.group_id
                                   ),
                      hs_match AS ( SELECT p.row_id, p.publisher_name, hs_main.main_publisher_id
                                      FROM hs_merge hm
                                      JOIN client_cue.rc_publisher p ON p.file_id = '||arg_file_id||' AND p.publisher_name = hm.merged_publisher_name AND coalesce(p.society_id,0) = coalesce(hm.society_id,0)
                                      JOIN hs_main ON hs_main.group_id = hm.group_id
                                   )
            UPDATE client_cue.rc_publisher p
               SET invalid_dt = now(),
                   invalid_by = '||coalesce(v_create_by,0)||',
                   systematic_main_row_id = m.main_publisher_id
              FROM hs_match m
             WHERE p.file_id = '||arg_file_id||'
               AND p.row_id = m.row_id';
       RAISE INFO 'Publisher Systematic match against history merge v_cmd = %', v_cmd;
       EXECUTE v_cmd;
       

    -- Catch Publisher Similarities from file and bin (excludes systematic merge)
    v_cmd := 'INSERT INTO CLIENT_CUE.RC_PUBLISHER_SIMILARITY
              WITH Sim AS (
                              SELECT max(p.file_id) file_id,
                                     max(p.row_id) row_id,
                                     p2.row_id similar_publisher_id,
                                     false as bin
                                FROM client_cue.rc_publisher p
                                JOIN client_cue.rc_publisher p2 ON p.file_id = p2.file_id
                                            AND p.publisher_name != p2.publisher_name
                                            AND (similarity(p.publisher_name,p2.publisher_name) > 0.94 or (p.publisher_name ilike concat(''%'',p2.publisher_name,''%'') and abs(char_length(p.publisher_name)-char_length(p2.publisher_name)) < 4))
                               WHERE p.file_id = '||arg_file_id||'
                                 AND p.systematic_main_row_id IS NULL
                                 AND p2.systematic_main_row_id IS NULL
                               GROUP BY 3
                              UNION
                              SELECT max(p.file_id) file_id,
                                     max(p.row_id) row_id,
                                     p2.row_id similar_publisher_id,
                                     true as bin
                                FROM client_cue.rc_publisher p
                                JOIN client_cue.publisher_'||v_file_row.client_id||' p2 ON (p.publisher_name = p2.publisher_name)
                                                                     OR (similarity(p.publisher_name,p2.publisher_name) > 0.94 or (p.publisher_name ilike concat(''%'',p2.publisher_name,''%'') and abs(char_length(p.publisher_name)-char_length(p2.publisher_name)) < 4))
                               WHERE p.file_id = '||arg_file_id||' 
                                 AND p.systematic_main_row_id IS NULL 
                               GROUP BY 3
                               ORDER BY 3
                           ),
                   Sim_enum AS (SELECT sim.*, row_number() over( Order by row_id) rn FROM sim),
                   Ord AS (SELECT file_id, unnest(string_to_array(row_id||'',''||similar_publisher_id, '','')) pair_id, bin, rn
                             FROM sim_enum
                            ORDER BY rn, pair_id
                           ),
                   Pairs AS (select file_id, array_agg(pair_id Order by pair_id desc) pair_id, bin, rn from ord group by file_id, bin, rn )
              SELECT file_id,
                     pair_id[1]::bigint publisher_id,
                     pair_id[2]::bigint similar_publisher_id,
                     bin,
                     false as merge,
                     false as main
                FROM pairs 
               GROUP BY file_id, pair_id[1], pair_id[2], bin, main';
    RAISE INFO 'Catching Publisher Similarities v_cmd = %', v_cmd;
    EXECUTE v_cmd;
    -- switches messed up bin ids (bin id should be similar_publisher_id)
    v_cmd := 'WITH mess AS (SELECT s.* FROM client_cue.rc_publisher_similarity s join client_cue.publisher_'||v_file_row.client_id||' p on p.row_id = s.publisher_id where s.file_id = '||arg_file_id||' AND s.bin is true)
              UPDATE client_cue.rc_publisher_similarity ps
                 SET publisher_id = m.similar_publisher_id,
                     similar_publisher_id = m.publisher_id
                FROM mess m
               WHERE ps.publisher_id = m.publisher_id
                 AND ps.similar_publisher_id = m.similar_publisher_id
                 AND ps.bin is true';
    EXECUTE v_cmd;
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    RAISE INFO 'Switched Publisher ids = %', cRows;
   
    -- CUESONG
    v_cuesong  := 'WITH cuesong AS (SELECT 
                                           nextval(''client_cue.seq_cuesong__row_id''::regclass) as row_id,
                                           '||arg_file_id||' as file_id,
                                           '||v_file_row.client_id||' as client_id,
                                           CASE WHEN '||quote_literal(v_file_format)||' ~* ''V2'' THEN nullif(btrim(f.title_id),'''')::bigint ELSE nextval(''client_cue.seq_rc_cuesong__title_id''::regclass) END as title_id,
                                           upper(trim(f.title)) as title,
                                           f.iswc as iswc_code,
                                           f.cd_number as cd_no,
                                           f.cut_number as cut_no,
                                           f.library,
                                           null::varchar as notes,
                                           array_to_string(array_agg( distinct trim(f.artist)),'','') as artists,
                                           array_to_string(array_agg( distinct nullif(upper(btrim(regexp_replace(coalesce(first_name,'''')||chr(32)||coalesce(middle_name,'''')||chr(32)||coalesce(last_name,''''),''\s{2,}'',chr(32),''g''))),'''')),'','') as composers,
                                           array_to_string(array_agg( distinct nullif(upper(btrim(regexp_replace(f.company_name,''\s{2,}'',chr(32),''g''))),'''')),'','') as publishers,
                                           null::bigint as songdex_song_id,
                                           false::boolean as matched,
                                           null::bigint as original_cuesong_id
                                      FROM client_cue.rc_file_data f
                                     WHERE f.file_id = '||arg_file_id||'
                                       AND coalesce(trim(f.title),'''') != ''''
                                     GROUP BY
                                           nullif(btrim(f.title_id),''''),
                                           f.title,
                                           f.iswc,
                                           f.cd_number,
                                           f.cut_number,
                                           f.library
                                   )
                   INSERT INTO CLIENT_CUE.RC_CUESONG
                   SELECT cs.row_id,
                          cs.file_id,
                          cs.client_id,
                          cs.title_id,
                          cs.title,
                          cs.iswc_code,
                          cs.cd_no,
                          cs.cut_no,
                          cs.library,
                          cs.notes,
                          array_to_json(string_to_array(cs.artists,'',''))::jsonb artists,
                          array_to_json(string_to_array(cs.composers,'',''))::jsonb composers,
                          array_to_json(string_to_array(cs.publishers,'',''))::jsonb publishers,
                          cs.songdex_song_id,
                          cs.matched,
                          cs.original_cuesong_id
                     FROM cuesong cs
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16';
       RAISE INFO 'v_cuesong = %', v_cuesong;
       EXECUTE v_cuesong;
       -- Check for a match in the Bin
       RAISE INFO 'Checking for existing cuesong ...';
       v_cuesong := 'WITH rc AS (
                                 SELECT DISTINCT
                                        s.row_id,
                                        s.title,
                                        array_to_json(array_agg(distinct a1.artist_name))::jsonb artists,
                                        c1.society_id copm_society_id,
                                        array_to_json(array_agg(distinct c1.composer_name))::jsonb composers,
                                        p1.society_id pub_society_id,
                                        array_to_json(array_agg(distinct p1.publisher_name))::jsonb publishers
                                   FROM client_cue.rc_cuesong s
                                   LEFT JOIN client_cue.rc_cuesong_artist a ON a.cuesong_id = s.row_id
                                   LEFT JOIN client_cue.rc_artist a1 ON coalesce(a1.systematic_main_row_id,a1.row_id) = a.artist_id
                                   LEFT JOIN client_cue.rc_cuesong_composer c ON c.file_id = s.file_id AND c.cuesong_id = s.row_id
                                   LEFT JOIN client_cue.rc_composer c1 ON coalesce(c1.systematic_main_row_id,c1.row_id) = c.composer_id
                                   LEFT JOIN client_cue.rc_cuesong_publisher p ON p.file_id = s.file_id AND p.cuesong_id = s.row_id
                                   LEFT JOIN client_cue.rc_publisher p1 ON coalesce(p1.systematic_main_row_id,p1.row_id) = p.publisher_id
                                  WHERE s.file_id = '||arg_file_id||'
                                  GROUP BY 1,2,4,6
                               ),
                          bin AS (
                                 SELECT  
                                        s.row_id original_cuesong_id,
                                        s.title,
                                        array_to_json(array_agg(distinct a1.artist_name))::jsonb artists,
                                        c1.society_id copm_society_id,
                                        array_to_json(array_agg(distinct c1.composer_name))::jsonb composers,
                                        p1.society_id pub_society_id,
                                        array_to_json(array_agg(distinct p1.publisher_name))::jsonb publishers
                                   FROM client_cue.cuesong s
                                   LEFT JOIN client_cue.cuesong_artist a ON a.client_id = s.client_id AND a.cuesong_id = s.row_id
                                   LEFT JOIN client_cue.artist a1 ON a1.client_id = s.client_id AND a1.row_id = a.artist_id 
                                   LEFT JOIN client_cue.cuesong_composer c ON c.client_id = s.client_id AND c.cuesong_id = s.row_id
                                   LEFT JOIN client_cue.composer c1 ON c1.client_id = s.client_id AND c1.row_id = c.composer_id
                                   LEFT JOIN client_cue.cuesong_publisher p ON p.client_id = s.client_id AND p.cuesong_id = s.row_id
                                   LEFT JOIN client_cue.publisher p1 ON p1.client_id = s.client_id AND p1.row_id = p.publisher_id
                                  WHERE s.client_id = '||v_file_row.client_id||'
                                  GROUP BY 1,2,4,6
                                )
                     SELECT DISTINCT rc.row_id, min(bin.original_cuesong_id) original_cuesong_id
                       FROM rc JOIN bin ON bin.title = rc.title
                                       AND coalesce(rc.artists,''0''::jsonb) = coalesce(bin.artists,''0''::jsonb)
                                       AND coalesce(rc.composers,''0''::jsonb) = coalesce(bin.composers,''0''::jsonb)
                                       AND coalesce(rc.copm_society_id,0) = coalesce(bin.copm_society_id,0)
                                       AND coalesce(rc.publishers,''0''::jsonb) = coalesce(bin.publishers,''0''::jsonb)
                                       AND coalesce(rc.pub_society_id,0) = coalesce(bin.pub_society_id,0)
                      GROUP BY 1';
       RAISE INFO 'v_cuesong = %', v_cuesong;
       FOR v_obj_rec IN EXECUTE v_cuesong LOOP
          UPDATE client_cue.rc_cuesong SET original_cuesong_id = v_obj_rec.original_cuesong_id, matched = true WHERE row_id = v_obj_rec.row_id;
       END LOOP;
       -- Update file rc_title_id for easy later join only when FORMAT is V1
       IF v_file_format ~* 'V1' THEN
          RAISE INFO 'Updates file rc_title_id for easy later join ...';
          v_cmd := 'UPDATE client_cue.rc_file_data f
                       SET rc_title_id = rcc.title_id::varchar
                      FROM client_cue.rc_cuesong rcc
                     WHERE f.file_id = '||arg_file_id||'
                       AND rcc.file_id = f.file_id
                       AND rcc.title = upper(trim(f.title))
                       AND coalesce(rcc.iswc_code,'''') = coalesce(f.iswc,'''')
                       AND coalesce(rcc.cd_no,'''') = coalesce(f.cd_number,'''')
                       AND coalesce(rcc.cut_no,'''') = coalesce(f.cut_number,'''')
                       AND coalesce(rcc.library,'''') = coalesce(f.library,'''')';
          RAISE INFO 'v_cmd = %', v_cmd;
          EXECUTE v_cmd;
       END IF;      
    
    -- CUESONG_ARTIST
    RAISE INFO 'Processing cuesong_artist ...';
    v_cuesong_artist  := 'WITH cue AS (SELECT f.file_id,
                                              coalesce(f.title_id, f.rc_title_id) as title_id,
                                              upper(regexp_replace(trim(f.artist),''\s{2,}'',chr(32),''g'')) as artist, 
                                              CASE WHEN upper(coalesce(f.sync_license_indicator,''No'')) = ''NO'' THEN null::varchar ELSE f.sync_licensor END sync_licensor, 
                                              CASE WHEN upper(coalesce(f.master_use_license_indicator,''No'')) = ''NO'' THEN null::varchar ELSE f.master_use_licensor END master_licensor
                                         FROM client_cue.rc_file_data f 
                                        WHERE f.file_id = '||arg_file_id||'
                                          AND upper(trim(f.artist)) != ''''
                                        GROUP BY 1,2,3,4,5
                                      )
                          INSERT INTO CLIENT_CUE.RC_CUESONG_ARTIST
                          SELECT nextval(''client_cue.seq_cuesong_composer__row_id''::regclass) row_id,
                                 '||arg_file_id||' as file_id,
                                 '||v_file_row.client_id||' as client_id,
                                 s.row_id as cuesong_id,
                                 CASE WHEN a.systematic_main_row_id IS NOT NULL THEN a.systematic_main_row_id ELSE a.row_id END as artist_id,
                                 cue.sync_licensor,
                                 cue.master_licensor,
                                 false::boolean as matched,
                                 null::bigint as original_cuesong_artist_id
                            FROM client_cue.rc_cuesong s
                            JOIN cue ON cue.file_id = s.file_id AND cue.title_id::bigint = s.title_id
                            JOIN client_cue.rc_artist a ON a.file_id = cue.file_id AND a.artist_name = cue.artist';
    RAISE INFO 'v_cuesong_artist = %', v_cuesong_artist;
    EXECUTE v_cuesong_artist;
    RAISE INFO 'Checking for existing cuesong_artist ...';
    v_cuesong_artist := 'SELECT rcca.row_id, ca.row_id as original_cuesong_artist_id
                           FROM client_cue.rc_cuesong_artist rcca
                           JOIN client_cue.rc_artist ra ON ra.row_id = rcca.artist_id AND ra.file_id = rcca.file_id
                           JOIN client_cue.artist a ON a.artist_name = ra.artist_name AND a.client_id = ra.client_id
                           JOIN client_cue.cuesong_artist_'||v_file_row.client_id||' ca ON ca.artist_id = a.row_id AND ca.client_id = a.client_id
                     WHERE rcca.file_id = '||arg_file_id;
    RAISE INFO 'v_cuesong_artist = %', v_cuesong_artist;
    FOR v_obj_rec IN EXECUTE v_cuesong_artist LOOP
       UPDATE client_cue.rc_cuesong_artist SET original_cuesong_artist_id = v_obj_rec.original_cuesong_artist_id, matched = true WHERE row_id = v_obj_rec.row_id;
    END LOOP;

    -- CUESONG_COMPOSER
    RAISE INFO 'Processing cuesong_composer ...';
    v_cuesong_composer := 'WITH cue AS (SELECT 
                                               coalesce(f.title_id, f.rc_title_id) as title_id,
                                               f.file_id,
                                               upper(btrim(regexp_replace(coalesce(last_name,'''')||'',''||chr(32)||coalesce(first_name,'''')||chr(32)||coalesce(middle_name,''''),''\s{2,}'',chr(32),''g''))) as composer_name, 
                                               upper(rol.role_name) as role_name,
                                               rol.row_id as role_id,
                                               CASE WHEN UPPER(trim(f.affiliation)) = ''UNKNOWN'' THEN ''UNK''
                                                    WHEN UPPER(trim(f.affiliation)) = ''NO SOCIETY'' THEN ''NS''
                                                    WHEN UPPER(trim(f.affiliation)) = ''PUBLIC DOMAIN'' THEN ''PD''
                                                ELSE UPPER(nullif(trim(f.affiliation),'''')) 
                                                END as affiliation,
                                               nullif(trim(replace(f.share_percent,''%'','''')),''0.00'') as share_pct,
                                               f.ssn::int
                                          FROM client_cue.rc_file_data f
                                          JOIN client_cue.composer_role rol ON upper(trim(regexp_replace(rol.role_name,''\W+'',chr(32),''g''))) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                                         WHERE f.file_id = '||arg_file_id||'
                                         GROUP BY 1,2,3,4,5,6,7,8
                                       )
                           INSERT INTO CLIENT_CUE.RC_CUESONG_COMPOSER
                           SELECT nextval(''client_cue.seq_cuesong_composer__row_id''::regclass) row_id,
                                  '||arg_file_id||' as file_id,
                                  '||v_file_row.client_id||' as client_id,
                                  s.row_id as cuesong_id,
                                  CASE WHEN c.systematic_main_row_id IS NOT NULL THEN c.systematic_main_row_id ELSE c.row_id END as composer_id,
                                  cue.role_id,
                                  sum(cue.share_pct::numeric)
                             FROM client_cue.rc_cuesong s 
                             JOIN cue ON cue.file_id = s.file_id AND cue.title_id::bigint = s.title_id
                             JOIN client_cue.rc_composer c ON c.file_id = cue.file_id
                                                          AND c.composer_name = cue.composer_name
                                                          AND c.affiliation = cue.affiliation
                            GROUP BY 4,5,6';
    RAISE INFO 'v_cuesong_composer = %', v_cuesong_composer;
    EXECUTE v_cuesong_composer;         
    RAISE INFO 'Checking for existing cuesong_composer ...';
    v_cuesong_composer := 'SELECT rcc.row_id, cc.row_id as original_cuesong_composer_id
                           FROM client_cue.rc_cuesong_composer rcc
                           JOIN client_cue.rc_composer rc ON rc.row_id = rcc.composer_id AND rc.file_id = rcc.file_id
                           JOIN client_cue.composer_'||v_file_row.client_id||' c ON c.composer_name = rc.composer_name
                           JOIN client_cue.cuesong_composer_'||v_file_row.client_id||' cc ON cc.composer_id = c.row_id
                     WHERE rcc.file_id = '||arg_file_id||'
                       AND rcc.role_id = cc.role_id';
    RAISE INFO 'v_cuesong_composer = %', v_cuesong_composer;
    FOR v_obj_rec IN EXECUTE v_cuesong_composer LOOP
       UPDATE client_cue.rc_cuesong_composer SET original_cuesong_composer_id = v_obj_rec.original_cuesong_composer_id, matched = true WHERE row_id = v_obj_rec.row_id;
    END LOOP;
   
    -- CATCHING BAD DATA; Check for incorrect composer name/share per song
    RAISE INFO 'CATCHING BAD DATA; Check for incorrect composer name/share per song.';
    v_cmd := 'WITH sim AS (
                           SELECT max(c.file_id) file_id,
                                  max(c.row_id) row_id,
                                  max(c.composer_name) composer_name,
                                  c2.row_id similar_composer_id,
                                  c2.composer_name similar_composer_name,
                                  false as bin
                             FROM client_cue.rc_composer c
                             JOIN client_cue.rc_composer c2 ON c.file_id = c2.file_id
                                                            AND c.composer_name != c2.composer_name
                                                            AND (similarity(c.composer_name,c2.composer_name) > 0.94 or (c.composer_name ilike concat(''%'',c2.composer_name,''%'') and abs(char_length(c.composer_name)-char_length(c2.composer_name)) < 4))
                            WHERE c.file_id = '||arg_file_id||'
                              AND c.systematic_main_row_id IS NULL
                              AND c2.systematic_main_row_id IS NULL
                            GROUP BY 4,5
                          ),
                   sim_song_enum AS (SELECT cc.cuesong_id,
                                            s.title, 
                                            cc.share_pct, 
                                            sim.*,
                                            row_number() over(partition by cc.cuesong_id, cc.share_pct) rn
                                       FROM sim
                                       JOIN client_cue.rc_cuesong_composer cc ON cc.file_id = sim.file_id AND cc.composer_id = sim.similar_composer_id
                                       JOIN client_cue.rc_cuesong s ON s.file_id = cc.file_id AND s.row_id = cc.cuesong_id
                                     ),
                   song_pct AS (SELECT cc.cuesong_id,
                                       s.title, 
                                       sum(cc.share_pct) share_pct
                                  FROM sim 
                                  JOIN client_cue.rc_cuesong_composer cc ON cc.file_id = sim.file_id AND cc.composer_id = sim.similar_composer_id
                                  JOIN client_cue.rc_cuesong s ON s.file_id = cc.file_id AND s.row_id = cc.cuesong_id
                                 GROUP BY 1,2
                                HAVING sum(cc.share_pct) > 100.00
                               )
              SELECT array_to_string(array_agg(''TITLE: ''||s.title||'' | COMPOSER: ''||s.similar_composer_name),chr(13))
                FROM sim_song_enum s join song_pct pct on pct.cuesong_id = s.cuesong_id
               WHERE s.rn > 1 
                 AND pct.share_pct > 100.00';
    RAISE INFO 'v_cmd = %', v_cmd;
    EXECUTE v_cmd INTO v_return_value;
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    RAISE INFO 'cRows = %',cRows;
    IF cRows > 0 AND coalesce(v_return_value,'') != '' THEN
       RAISE INFO 'Bad data found.';
       EXECUTE 'INSERT INTO tbl_errors(message)
                SELECT ''Incorrect composer name/share per song: '||chr(13)||regexp_replace(coalesce(v_return_value,''::text),E'''',chr(32))||'''';
    END IF;

    -- CUESONG PUBLISHER
    RAISE INFO 'Processing cuesong_publisher ...';
    v_cuesong_publisher := 'WITH cue AS (WITH counts AS (
                                                         SELECT 
                                                                f.file_id,
                                                                coalesce(f.title_id, f.rc_title_id)::bigint as title_id,
                                                                f.title,
                                                                -- upper(btrim(regexp_replace(f.role,''(-)+|(_)+'',chr(32)))),
                                                                count(distinct coalesce(trim(f.company_name),'''')),
                                                                count(distinct case when rol.row_id = 100 then upper(btrim(regexp_replace(coalesce(f.company_name,''''),''\s{2,}'',chr(32),''g''))) else null end) pub_count,
                                                                count(distinct case when rol.row_id = 110 then upper(btrim(regexp_replace(coalesce(f.company_name,''''),''\s{2,}'',chr(32),''g''))) else null end) admin_count
                                                           FROM client_cue.rc_file_data f
                                                           JOIN client_cue.publisher_role rol ON upper(trim(regexp_replace(rol.role_name,''\W+'',chr(32),''g''))) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                                                          WHERE f.file_id = '||arg_file_id||'
                                                            AND coalesce(trim(f.company_name),'''') != ''''
                                                            AND rol.row_id != 120
                                                          GROUP BY 1,2,3
                                                          ORDER BY 3
                                                        )
                                         SELECT c.*
                                           FROM counts c
                                          WHERE (pub_count > 0 AND admin_count < 2 )
                                          ORDER BY 3
                                          ),
                             cue_pub AS (SELECT
                                                f.file_id,
                                                coalesce(f.title_id, f.rc_title_id) as title_id,
                                                CASE WHEN p.systematic_main_row_id IS NOT NULL THEN p.systematic_main_row_id ELSE p.row_id END as publisher_id,
                                                upper(regexp_replace(coalesce(trim(f.company_name),''''),''\s{2,}'',chr(32),''g'')) publisher_name,
                                                upper(coalesce(trim(f.affiliation),'''')) as affiliation,
                                                nullif(trim(replace(f.share_percent,''%'','''')),''0.00'') as share_pct,
                                                rol.row_id as role_id
                                           FROM client_cue.rc_file_data f
                                           JOIN client_cue.publisher_role rol ON upper(trim(regexp_replace(rol.role_name,''\W+'',chr(32),''g''))) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                                           JOIN client_cue.rc_publisher p ON p.file_id = f.file_id
                                                                         AND p.publisher_name = upper(regexp_replace(coalesce(trim(f.company_name),''''),''\s{2,}'',chr(32),''g''))
                                                                         AND p.affiliation = CASE WHEN UPPER(trim(f.affiliation)) = ''UNKNOWN'' THEN ''UNK''
                                                                                                  WHEN UPPER(trim(f.affiliation)) = ''NO SOCIETY'' THEN ''NS''
                                                                                                  WHEN UPPER(trim(f.affiliation)) = ''PUBLIC DOMAIN'' THEN ''PD''
                                                                                             ELSE UPPER(nullif(trim(f.affiliation),'''')) 
                                                                                             END
                                          WHERE f.file_id = '||arg_file_id||'
                                            AND coalesce(trim(f.company_name),'''') != ''''
                                            AND rol.row_id = 100
                                          GROUP BY 1,2,3,4,5,6,7
                                          ORDER BY 4
                                          ),
                             cue_adm AS (SELECT 
                                                f.file_id,
                                                coalesce(f.title_id, f.rc_title_id) as title_id,
                                                upper(regexp_replace(coalesce(trim(f.company_name),''''),''\s{2,}'',chr(32),''g'')) as admin_name,
                                                CASE WHEN p.systematic_main_row_id IS NOT NULL THEN p.systematic_main_row_id ELSE p.row_id END as admin_id,
                                                upper(coalesce(trim(f.affiliation),'''')) as affiliation,
                                                nullif(trim(replace(f.share_percent,''%'','''')),''0.00'') as share_pct,
                                                rol.row_id as role_id
                                           FROM client_cue.rc_file_data f
                                           JOIN client_cue.publisher_role rol ON upper(trim(regexp_replace(rol.role_name,''\W+'',chr(32),''g''))) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g''))) AND rol.row_id = 110
                                           LEFT JOIN client_cue.rc_publisher p ON p.file_id = f.file_id
                                                                         AND p.publisher_name = upper(regexp_replace(coalesce(trim(f.company_name),''''),''\s{2,}'',chr(32),''g''))
                                                                         AND p.affiliation = CASE WHEN UPPER(trim(f.affiliation)) = ''UNKNOWN'' THEN ''UNK''
                                                                                                  WHEN UPPER(trim(f.affiliation)) = ''NO SOCIETY'' THEN ''NS''
                                                                                                  WHEN UPPER(trim(f.affiliation)) = ''PUBLIC DOMAIN'' THEN ''PD''
                                                                                             ELSE UPPER(nullif(trim(f.affiliation),'''')) 
                                                                                             END
                                          WHERE f.file_id = '||arg_file_id||'
                                            AND coalesce(trim(f.company_name),'''') != ''''
                                          GROUP BY 1,2,3,4,5,6,7
                                          ORDER BY 3,4,5
                                          )
                            INSERT INTO CLIENT_CUE.RC_CUESONG_PUBLISHER
                            SELECT nextval(''client_cue.seq_cuesong_publisher__row_id''::regclass) row_id,
                                   s.file_id,
                                   s.client_id,
                                   s.row_id as cuesong_id,
                                  /*deprecated CASE WHEN pub.publisher_id is null AND adm.admin_id is not null THEN adm.admin_id ELSE pub.publisher_id END as publisher_id,*/
                                   pub.publisher_id,
                                  /*deprecated CASE WHEN pub.publisher_id is not null AND adm.admin_id is not null AND c.admin_count = 1 THEN adm.admin_id ELSE null::bigint END as admin_id,*/
                                   CASE WHEN adm.admin_id is not null THEN adm.admin_id ELSE null::bigint END as admin_id,
                                   sum(coalesce(pub.share_pct::numeric,adm.share_pct::numeric)) as share_pct,
                                   pub.role_id
                              FROM client_cue.rc_cuesong s 
                              JOIN cue c ON c.file_id = s.file_id AND c.title_id::bigint = s.title_id
                              LEFT JOIN cue_pub pub ON pub.file_id = c.file_id AND pub.title_id::bigint = c.title_id::bigint /*AND pub.role_id = CASE WHEN c.admin_count > 1 THEN pub.role_id ELSE 100 END*/
                              LEFT JOIN cue_adm adm ON adm.file_id = c.file_id AND adm.title_id::bigint = c.title_id::bigint
                             WHERE coalesce(pub.publisher_id,adm.admin_id) is not null
                             GROUP BY 2,3,4,5,6,8
                             ORDER BY 4';
    RAISE INFO 'v_cuesong_publisher = %', v_cuesong_publisher;
    EXECUTE v_cuesong_publisher;         
    RAISE INFO 'Checking for existing cuesong_publisher ...';
    v_cuesong_publisher := 'SELECT rcp.row_id, cp.row_id as original_cuesong_publisher_id
                              FROM client_cue.rc_cuesong_publisher rcp
                              JOIN client_cue.rc_publisher rp ON rp.row_id = COALESCE(rcp.publisher_id,rcp.admin_id) AND rp.file_id = rcp.file_id
                              JOIN client_cue.publisher_'||v_file_row.client_id||' p ON p.publisher_name = rp.publisher_name
                              JOIN client_cue.cuesong_publisher_'||v_file_row.client_id||' cp ON cp.publisher_id = p.row_id
                        WHERE rcp.file_id = '||arg_file_id;
    RAISE INFO 'v_cuesong_publisher = %', v_cuesong_publisher;
    FOR v_obj_rec IN EXECUTE v_cuesong_publisher LOOP
       UPDATE client_cue.rc_cuesong_publisher SET original_cuesong_publisher_id = v_obj_rec.original_cuesong_publisher_id, matched = true WHERE row_id = v_obj_rec.row_id;
    END LOOP;

    /* CUESONG SUB PUBLISHER
     Consists of: (pub = 1 and sub-pub > 0) per cuesong
    */
    RAISE INFO 'Processing cuesong_sub_publisher ...';
    v_cuesong_sub_pub   := 'WITH cue AS (WITH counts AS (
                                                            SELECT
                                                                   f.file_id,
                                                                   coalesce(f.title_id, f.rc_title_id)::bigint as title_id,
                                                                   f.title,
                                                                   -- upper(btrim(regexp_replace(f.role,''(-)+|(_)+'',chr(32)))),
                                                                   count(distinct coalesce(trim(f.company_name),'''')),
                                                                   count(distinct case when rol.row_id = 100 then upper(btrim(regexp_replace(coalesce(f.company_name,''''),''\s{2,}'',chr(32),''g''))) else null end) pub_count,
                                                                   count(distinct case when rol.row_id = 110 then upper(btrim(regexp_replace(coalesce(f.company_name,''''),''\s{2,}'',chr(32),''g''))) else null end) admin_count,
                                                                   count(distinct case when rol.row_id = 120 then upper(btrim(regexp_replace(coalesce(f.company_name,''''),''\s{2,}'',chr(32),''g''))) else null end) subpub_count
                                                              FROM client_cue.rc_file_data f
                                                              JOIN client_cue.publisher_role rol ON upper(trim(regexp_replace(rol.role_name,''\W+'',chr(32),''g''))) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                                                             WHERE f.file_id = '||arg_file_id||'
                                                               AND coalesce(trim(f.company_name),'''') != ''''
                                                             GROUP BY 1,2,3
                                                             ORDER BY 3
                                                             )
                                            SELECT c.*
                                              FROM counts c
                                             WHERE (pub_count = 1 AND subpub_count > 0)
                                             ORDER BY 3
                                          ),
                                 cue_subpub AS (SELECT
                                                        f.file_id,
                                                        cs.client_id,
                                                        coalesce(f.title_id, f.rc_title_id)::bigint as title_id,
                                                        cp.row_id as cuesong_pub_id,
                                                        CASE WHEN p.systematic_main_row_id IS NOT NULL THEN p.systematic_main_row_id ELSE p.row_id END as publisher_id,
                                                        upper(regexp_replace(coalesce(trim(f.company_name),''''),''\s{2,}'',chr(32),''g'')) publisher_name,
                                                        upper(coalesce(trim(f.affiliation),'''')) as affiliation,
                                                        nullif(trim(replace(f.share_percent,''%'','''')),''0.00'') as share_pct,
                                                        rol.row_id as role_id
                                                   FROM client_cue.rc_file_data f
                                                   JOIN client_cue.publisher_role rol ON upper(trim(regexp_replace(rol.role_name,''\W+'',chr(32),''g''))) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                                                   JOIN client_cue.rc_cuesong cs ON cs.file_id = f.file_id AND cs.title_id = coalesce(f.title_id, f.rc_title_id)::bigint
                                                   JOIN client_cue.rc_cuesong_publisher cp ON cp.cuesong_id = cs.row_id AND cp.role_id = 100
                                                   JOIN client_cue.rc_publisher p ON p.file_id = f.file_id
                                                                                 AND p.publisher_name = upper(regexp_replace(coalesce(trim(f.company_name),''''),''\s{2,}'',chr(32),''g''))
                                                                                 AND p.affiliation = CASE WHEN UPPER(trim(f.affiliation)) = ''UNKNOWN'' THEN ''UNK''
                                                                                                          WHEN UPPER(trim(f.affiliation)) = ''NO SOCIETY'' THEN ''NS''
                                                                                                          WHEN UPPER(trim(f.affiliation)) = ''PUBLIC DOMAIN'' THEN ''PD''
                                                                                                     ELSE UPPER(nullif(trim(f.affiliation),'''')) 
                                                                                                     END
                                                  WHERE f.file_id = '||arg_file_id||'
                                                    AND coalesce(trim(f.company_name),'''') != ''''
                                                    AND rol.row_id = 120
                                                  GROUP BY 1,2,3,4,5,6,7,8,9
                                                  ORDER BY 4
                                              )
                            INSERT INTO CLIENT_CUE.RC_CUESONG_SUB_PUBLISHER
                            SELECT nextval(''client_cue.seq_cuesong_sub_publisher__row_id''::regclass) row_id,
                                   c.file_id,
                                   sp.client_id,
                                   sp.cuesong_pub_id,
                                   sp.publisher_id,
                                   sum(sp.share_pct::numeric) as share_pct,
                                   null::int,
                                   null::int,
                                   null::boolean,
                                   null::bigint
                              FROM cue c
                              JOIN cue_subpub sp ON sp.file_id = c.file_id AND sp.title_id = c.title_id
                             GROUP BY 2,3,4,5
                             ORDER BY 4';
    RAISE INFO 'v_cuesong_sub_pub = %', v_cuesong_sub_pub;
    EXECUTE v_cuesong_sub_pub;         
    RAISE INFO 'Checking for existing cuesong_sub_publisher ...';
    v_cuesong_publisher := 'SELECT rcp.row_id, cp.row_id as original_cuesong_sub_publisher_id
                              FROM client_cue.rc_cuesong_sub_publisher rcp
                              JOIN client_cue.rc_publisher rp ON rp.row_id = rcp.publisher_id AND rp.file_id = rcp.file_id
                              JOIN client_cue.publisher_'||v_file_row.client_id||' p ON p.publisher_name = rp.publisher_name
                              JOIN client_cue.cuesong_sub_publisher_'||v_file_row.client_id||' cp ON cp.publisher_id = p.row_id
                        WHERE rcp.file_id = '||arg_file_id;
    RAISE INFO 'v_cuesong_publisher = %', v_cuesong_publisher;
    FOR v_obj_rec IN EXECUTE v_cuesong_publisher LOOP
       UPDATE client_cue.rc_cuesong_sub_publisher SET original_cuesong_sub_publisher_id = v_obj_rec.original_cuesong_sub_publisher_id, matched = true WHERE row_id = v_obj_rec.row_id;
    END LOOP;

    /* CUESONG ORPHANT PUBLISHER DEPENDENT
       Consists of non-straight links of Adm and Sub-pub
       Exmaple: 1. Pub = 0 and Adm => 1
                2. Pub > 1 and Adm > 1
                3. Pub > 1 and Sub-pub > 0
    */
    RAISE INFO 'Processing cuesong_orphant_publisher_dependent ...';
    v_cmd          := 'WITH cue AS (WITH counts AS (
                                                    SELECT
                                                           f.file_id,
                                                           coalesce(f.title_id, f.rc_title_id)::bigint as title_id,
                                                           f.title,
                                                           -- upper(btrim(regexp_replace(f.role,''(-)+|(_)+'',chr(32)))),
                                                           count(distinct coalesce(trim(f.company_name),'''')),
                                                           count(distinct case when rol.row_id = 100 then upper(btrim(regexp_replace(coalesce(f.company_name,''''),''\s{2,}'',chr(32),''g''))) else null end) pub_count,
                                                           count(distinct case when rol.row_id = 110 then upper(btrim(regexp_replace(coalesce(f.company_name,''''),''\s{2,}'',chr(32),''g''))) else null end) admin_count,
                                                           count(distinct case when rol.row_id = 120 then upper(btrim(regexp_replace(coalesce(f.company_name,''''),''\s{2,}'',chr(32),''g''))) else null end) subpub_count
                                                      FROM client_cue.rc_file_data f
                                                      JOIN client_cue.publisher_role rol ON upper(trim(regexp_replace(rol.role_name,''\W+'',chr(32),''g''))) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                                                     WHERE f.file_id = '||arg_file_id||'
                                                       AND coalesce(trim(f.company_name),'''') != ''''
                                                     GROUP BY 1,2,3
                                                     ORDER BY 3
                                                   )
                                    SELECT c.*
                                      FROM counts c
                                     WHERE (pub_count = 0 AND (admin_count > 0 OR subpub_count > 0))
                                        OR (pub_count > 1 AND (admin_count > 1 OR subpub_count > 0))
                                   ),
                            cue_pub AS (SELECT
                                               f.file_id,
                                               p.client_id,
                                               cs.row_id cuesong_id,
                                               coalesce(f.title_id, f.rc_title_id)::bigint as title_id,
                                               CASE WHEN p.systematic_main_row_id IS NOT NULL THEN p.systematic_main_row_id ELSE p.row_id END as publisher_id,
                                               upper(regexp_replace(coalesce(trim(f.company_name),''''),''\s{2,}'',chr(32),''g'')) publisher_name,
                                               upper(coalesce(trim(f.affiliation),'''')) as affiliation,
                                               nullif(trim(replace(f.share_percent,''%'','''')),''0.00'') as share_pct,
                                               rol.row_id as role_id
                                          FROM client_cue.rc_file_data f
                                          JOIN client_cue.publisher_role rol ON upper(trim(regexp_replace(rol.role_name,''\W+'',chr(32),''g''))) = upper(trim(regexp_replace(f.role,''\W+'',chr(32),''g'')))
                                          JOIN client_cue.rc_cuesong cs ON cs.file_id = f.file_id AND cs.title_id = coalesce(f.title_id, f.rc_title_id)::bigint
                                          JOIN client_cue.rc_publisher p ON p.file_id = f.file_id
                                                                        AND p.publisher_name = upper(regexp_replace(coalesce(trim(f.company_name),''''),''\s{2,}'',chr(32),''g''))
                                                                        AND p.affiliation = CASE WHEN UPPER(trim(f.affiliation)) = ''UNKNOWN'' THEN ''UNK''
                                                                                                 WHEN UPPER(trim(f.affiliation)) = ''NO SOCIETY'' THEN ''NS''
                                                                                                 WHEN UPPER(trim(f.affiliation)) = ''PUBLIC DOMAIN'' THEN ''PD''
                                                                                            ELSE UPPER(nullif(trim(f.affiliation),''''))
                                                                                            END
                                         WHERE f.file_id = '||arg_file_id||'
                                           AND coalesce(trim(f.company_name),'''') != ''''
                                           AND rol.row_id in (110, 120)
                                         GROUP BY 1,2,3,4,5,6,7,8,9
                                         ORDER BY 4
                                       )
                            INSERT INTO CLIENT_CUE.RC_CUESONG_ORPHAN_PUBLISHER_DEPENDENT
                            SELECT nextval(''client_cue.cuesong_orphan_publisher_dependent_row_id''::regclass) row_id,
                                   c.file_id,
                                   cp.client_id,
                                   cp.cuesong_id,
                                   cp.publisher_id,
                                   cp.role_id,
                                   sum(cp.share_pct::numeric) as share_pct,
                                   null::int,
                                   null::int,
                                   null::boolean,
                                   null::bigint
                              FROM cue c
                              JOIN cue_pub cp ON cp.file_id = c.file_id AND cp.title_id = c.title_id
                             GROUP BY 2,3,4,5,6
                             ORDER BY 6';
    RAISE INFO 'v_cuesong_orphant_publisher = %', v_cmd;
    EXECUTE v_cmd;

    -- CATCHING BAD DATA; Check for incorrect publisher name/share per song
    RAISE INFO 'CATCHING BAD DATA; Check for incorrect publisher name/share per song.';
    v_cmd := 'WITH sim AS (
                           SELECT max(p.file_id) file_id,
                                  max(p.row_id) row_id,
                                  max(p.publisher_name) publisher_name,
                                  p2.row_id similar_publisher_id,
                                  p2.publisher_name similar_publisher_name,
                                  false as bin
                             FROM client_cue.rc_publisher p
                             JOIN client_cue.rc_publisher p2 ON p.file_id = p2.file_id
                                                            AND p.publisher_name != p2.publisher_name
                                                            AND (similarity(p.publisher_name,p2.publisher_name) > 0.94 or (p.publisher_name ilike concat(''%'',p2.publisher_name,''%'') and abs(char_length(p.publisher_name)-char_length(p2.publisher_name)) < 4))
                            WHERE p.file_id = '||arg_file_id||'
                              AND p.systematic_main_row_id IS NULL
                              AND p2.systematic_main_row_id IS NULL
                            GROUP BY 4,5
                          ),
                   sim_song_enum AS (SELECT cp.cuesong_id,
                                            s.title, 
                                            cp.share_pct, 
                                            sim.*,
                                            row_number() over(partition by cp.cuesong_id, cp.share_pct) rn
                                       FROM sim
                                       JOIN client_cue.rc_cuesong_publisher cp ON cp.file_id = sim.file_id AND cp.publisher_id = sim.similar_publisher_id
                                       JOIN client_cue.rc_cuesong s ON s.file_id = cp.file_id AND s.row_id = cp.cuesong_id
                                     ),
                   song_pct AS (SELECT cp.cuesong_id,
                                       s.title, 
                                       sum(cp.share_pct) share_pct
                                  FROM sim 
                                  JOIN client_cue.rc_cuesong_publisher cp ON cp.file_id = sim.file_id AND cp.publisher_id = sim.similar_publisher_id
                                  JOIN client_cue.rc_cuesong s ON s.file_id = cp.file_id AND s.row_id = cp.cuesong_id
                                 GROUP BY 1,2
                                HAVING sum(cp.share_pct) > 100.00
                     )
              SELECT array_to_string(array_agg(''TITLE: ''||s.title||'' | publisher: ''||s.similar_publisher_name),chr(13))
                FROM sim_song_enum s join song_pct pct on pct.cuesong_id = s.cuesong_id
               WHERE s.rn > 1 
                 AND pct.share_pct > 100.00';
    RAISE INFO 'v_cmd = %', v_cmd;
    EXECUTE v_cmd INTO v_return_value;
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    IF cRows > 0 AND coalesce(v_return_value,'') != '' THEN
       RAISE INFO 'Bad data found.';
       EXECUTE 'INSERT INTO tbl_errors(message)
                SELECT ''Incorrect publisher name/share per song: '||chr(13)||regexp_replace(coalesce(v_return_value,''::text),E'''',chr(32))||'''';

    END IF;
    
    -- CUESHEET
    v_cuesheet := 'WITH ep_group AS (SELECT coalesce(e.episode_id, p.production_id) ep_id,
                                            nextval(''client_cue.seq_cuesheet__group_id''::regclass) as group_id
                                       FROM client_cue.rc_program p
                                       LEFT JOIN client_cue.rc_episode e ON e.file_id = p.file_id AND e.program_id = p.row_id
                                      WHERE p.file_id = '||arg_file_id||'
                                      GROUP BY 1
                                     ) 
                   INSERT INTO CLIENT_CUE.RC_CUESHEET
                   SELECT nextval(''client_cue.seq_cuesheet__row_id''::regclass) as row_id,
                          g.group_id,
                          '||arg_file_id||' as file_id,
                          '||v_file_row.client_id||' as client_id,
                          p.row_id as program_id,
                          e.row_id as episode_id,
                          s.row_id as cuesong_id,
                          f.sequence_number::int as sequence_id,
                          f.cue_number as cue_no,
                          COALESCE(f.cue_duration_minute::int,0) as duration_mm,
                          COALESCE(f.cue_duration_second::int,0) as duration_ss,
                          trim(f.cue_usage),
                          trim(f.cue_sub_usage),
                          trim(f.cue_theme),
                          case when upper(coalesce(f.sync_license_indicator,''No'')) = ''NO'' then false else true end as sync_license_indicator,
                          nullif(trim(f.cue_comments),'''') as notes
                     FROM client_cue.rc_file_data f
                     JOIN ep_group g ON g.ep_id = coalesce(f.program_id::bigint, f.rc_program_id::bigint, f.production_id::bigint, f.rc_production_id::bigint)
                     JOIN client_cue.rc_program p ON p.file_id = f.file_id AND p.production_id = coalesce(f.production_id::bigint, f.rc_production_id::bigint, f.program_id::bigint, f.rc_program_id::bigint)
                     LEFT JOIN client_cue.rc_episode e ON e.file_id = f.file_id AND e.episode_id = coalesce(f.program_id::bigint, f.rc_program_id::bigint)
                     JOIN client_cue.rc_cuesong s ON s.file_id = f.file_id AND s.title_id = coalesce(f.title_id::bigint, f.rc_title_id::bigint)
                    WHERE f.file_id = '||arg_file_id||'
                    GROUP BY 
                          g.group_id,
                          p.row_id,
                          e.row_id,
                          s.row_id,
                          p.production_id,
                          e.episode_id,
                          f.sequence_number::int,
                          f.cue_number,
                          f.cue_duration_minute::int,
                          f.cue_duration_second::int,
                          f.cue_usage,
                          f.cue_sub_usage,
                          f.cue_theme,
                          case when upper(coalesce(f.sync_license_indicator,''No'')) = ''NO'' then false else true end,
                          f.cue_comments
                    ORDER BY
                          g.group_id,
                          f.sequence_number::int';
       RAISE INFO 'v_cuesheet = %', v_cuesheet;
       RAISE INFO 'Collecting cuesheet ids for cuesheet creation.';
       EXECUTE v_cuesheet;
       GET DIAGNOSTICS cRows = ROW_COUNT;
       RAISE INFO 'Cuesheet collected rows = [%]', cRows;
       RAISE INFO 'Checking for existing rc_cuesheet ...';
       v_cuesheet := 'SELECT rccs.row_id, cs.row_id as original_cuesheet_id
                        FROM client_cue.rc_cuesheet rccs
                        JOIN client_cue.rc_program rcp ON rcp.row_id = rccs.program_id AND rcp.file_id = rccs.file_id
                        JOIN client_cue.rc_episode rce ON rce.row_id = rccs.episode_id AND rce.file_id = rccs.file_id
                        JOIN client_cue.rc_cuesong rcs ON rcs.row_id = rccs.cuesong_id AND rcs.file_id = rccs.file_id
                        JOIN client_cue.cuesheet_'||v_file_row.client_id||' cs ON cs.client_id = rccs.client_id AND cs.duration_mm = rccs.duration_mm AND cs.duration_ss = rccs.duration_ss
                        JOIN client_cue.program_'||v_file_row.client_id||' p ON p.row_id = cs.program_id AND p.title = rcp.title
                        JOIN client_cue.episode_'||v_file_row.client_id||' e ON e.row_id = cs.episode_id AND e.title = rce.title
                        JOIN client_cue.cuesong_'||v_file_row.client_id||' s ON s.row_id = cs.cuesong_id AND s.title = rcs.title
                       WHERE rccs.file_id = '||arg_file_id;
      RAISE INFO 'v_cuesheet = %', v_cuesheet;
      FOR v_obj_rec IN EXECUTE v_cuesheet LOOP
         UPDATE client_cue.rc_cuesheet SET original_cuesheet_id = v_obj_rec.original_cuesheet_id, matched = true WHERE row_id = v_obj_rec.row_id;
      END LOOP;

    -- CUESHEET_USAGE
    v_cuesheet_usage    := 'INSERT INTO CLIENT_CUE.RC_CUESHEET_USAGE
                            SELECT nextval(''client_cue.seq_cuesheet_usage__row_id''::regclass) row_id,
                                   '||arg_file_id||' as file_id,
                                   '||v_file_row.client_id||' as client_id,
                                   rcc.row_id as cueuse_id, 
                                   cut.row_id as usage_id
                              FROM client_cue.rc_cuesheet rcc
                              JOIN client_cue.cuesheet_usage_type cut ON cut.usage_name ~* trim(rcc.cue_usage)
                              LEFT JOIN client_cue.rc_cuesheet_usage cu ON cu.cueuse_id = rcc.row_id AND cu.usage_id = cut.row_id
                             WHERE rcc.file_id = '||arg_file_id||'
                               AND coalesce(trim(rcc.cue_usage),'''') != ''''
                               AND cu.row_id is null';
    RAISE INFO 'v_cuesheet_usage = %', v_cuesheet_usage;
    EXECUTE v_cuesheet_usage;
    RAISE INFO 'Checking for existing rc_cuesheet_usage ...';
    v_cuesheet_usage := 'SELECT rcc.row_id, cu.row_id as original_cuesheet_usage_id
                           FROM client_cue.rc_cuesheet rcc
                           JOIN client_cue.rc_cuesheet_usage rccu ON rccu.cueuse_id = rcc.row_id AND rccu.file_id = rcc.file_id
                           JOIN client_cue.cuesheet_usage_type cut ON cut.usage_name = trim(rcc.cue_usage)
                           JOIN client_cue.cuesheet_usage_'||v_file_row.client_id||' cu ON cu.cueuse_id = rcc.original_cuesheet_id AND cu.usage_id = cut.row_id
                          WHERE rcc.file_id = '||arg_file_id;
    RAISE INFO 'v_cuesheet_usage = %', v_cuesheet_usage;
    FOR v_obj_rec IN EXECUTE v_cuesheet_usage LOOP
       UPDATE client_cue.rc_cuesheet_usage SET original_cuesheet_usage_id = v_obj_rec.original_cuesheet_usage_id, matched = true WHERE row_id = v_obj_rec.row_id;
    END LOOP;

    -- CUESHEET_THEME
    v_cuesheet_theme    := 'INSERT INTO CLIENT_CUE.RC_CUESHEET_THEME
                            SELECT nextval(''client_cue.seq_cuesheet_theme__row_id''::regclass) row_id,
                                   '||arg_file_id||' as file_id,
                                   '||v_file_row.client_id||' as client_id,
                                   rcc.row_id as cueuse_id, 
                                   ctt.row_id as theme_id
                              FROM client_cue.rc_cuesheet rcc
                              JOIN client_cue.cuesheet_theme_type ctt ON ctt.theme_name ~* trim(rcc.cue_theme)
                              LEFT JOIN client_cue.rc_cuesheet_theme ct ON ct.cueuse_id = rcc.row_id AND ct.theme_id = ctt.row_id
                             WHERE rcc.file_id = '||arg_file_id||'
                               AND coalesce(trim(rcc.cue_theme),'''') != ''''
                               AND ct.row_id is null';
    RAISE INFO 'v_cuesheet_theme = %', v_cuesheet_theme;
    EXECUTE v_cuesheet_theme;
    RAISE INFO 'Checking for existing rc_cuesheet_theme ...';
    v_cuesheet_theme := 'SELECT rcc.row_id, cu.row_id as original_cuesheet_theme_id
                           FROM client_cue.rc_cuesheet rcc
                           JOIN client_cue.rc_cuesheet_theme rcct ON rcct.cueuse_id = rcc.row_id AND rcct.file_id = rcc.file_id
                           JOIN client_cue.cuesheet_theme_type ctt ON ctt.theme_name = trim(rcc.cue_theme)
                           JOIN client_cue.cuesheet_theme_'||v_file_row.client_id||' cu ON cu.cueuse_id = rcc.original_cuesheet_id AND cu.theme_id = ctt.row_id
                          WHERE rcc.file_id = '||arg_file_id;
    RAISE INFO 'v_cuesheet_theme = %', v_cuesheet_theme;
    FOR v_obj_rec IN EXECUTE v_cuesheet_theme LOOP
       UPDATE client_cue.rc_cuesheet_usage SET original_cuesheet_theme_id = v_obj_rec.original_cuesheet_theme_id, matched = true WHERE row_id = v_obj_rec.row_id;
    END LOOP;

    -- CATCHING BAD DATA; Check for duplicates
    RAISE INFO 'Checking for duplicates ...';
    v_cmd := 'WITH sort_list AS (SELECT 
                                         cue_sheet_type,
                                         production_id,
                                         series_name,
                                         program_id,
                                         program_name,
                                         program_version,
                                         program_duration_minute,
                                         program_duration_second,
                                         production_number,
                                         episode_number,
                                         epn,
                                         season,
                                         part_number,
                                         network_station,
                                         animated_indicator,
                                         prepared_by,
                                         music_content_indicator,
                                         program_comments,
                                         program_instructions,
                                         sequence_number,
                                         title_id,
                                         title,
                                         cue_number,
                                         cue_duration_minute,
                                         cue_duration_second,
                                         cue_usage,
                                         cue_sub_usage,
                                         cue_theme,
                                         artist,
                                         sync_license_indicator,
                                         sync_licensor,
                                         master_use_license_indicator,
                                         master_use_licensor,
                                         interested_party_id,
                                         role,
                                         first_name,
                                         middle_name,
                                         last_name,
                                         company_name,
                                         affiliation,
                                         share_percent,
                                         ssn,
                                         ip_comments,
                                         row_number() over(PARTITION BY 
                                                           cue_sheet_type,
                                                           production_id,
                                                           series_name,
                                                           program_id,
                                                           program_name,
                                                           program_version,
                                                           program_duration_minute,
                                                           program_duration_second,
                                                           production_number,
                                                           episode_number,
                                                           epn,
                                                           season,
                                                           part_number,
                                                           network_station,
                                                           animated_indicator,
                                                           prepared_by,
                                                           music_content_indicator,
                                                           program_comments,
                                                           program_instructions,
                                                           sequence_number,
                                                           title_id,
                                                           title,
                                                           cue_number,
                                                           cue_duration_minute,
                                                           cue_duration_second,
                                                           cue_usage,
                                                           cue_sub_usage,
                                                           cue_theme,
                                                           artist,
                                                           sync_license_indicator,
                                                           sync_licensor,
                                                           master_use_license_indicator,
                                                           master_use_licensor,
                                                           interested_party_id,
                                                           role,
                                                           first_name,
                                                           middle_name,
                                                           last_name,
                                                           company_name,
                                                           affiliation,
                                                           share_percent,
                                                           ssn,
                                                           ip_comments
                                                           ORDER BY 
                                                                 sequence_number, 
                                                                 title,
                                                                 artist,
                                                                 role,
                                                                 first_name,
                                                                 middle_name,
                                                                 last_name,
                                                                 company_name,
                                                                 affiliation) rn
                                  FROM client_cue.rc_file_data
                                 WHERE file_id = '||arg_file_id||'
                                )
              SELECT * FROM sort_list WHERE rn > 1';
    RAISE INFO 'v_cmd = %', v_cmd;
    EXECUTE v_cmd;
    GET CURRENT DIAGNOSTICS cRows = ROW_COUNT;
    IF cRows > 0 THEN
       RAISE INFO 'Bad data found.';
       EXECUTE 'INSERT INTO tbl_errors(message)
                SELECT ''Duplicate rows (count:'||cRows||').''';
    END IF;

    -- Sending out the bad data output and completing the process according to the user choice
    RAISE INFO 'Finalizing ...';
    IF EXISTS(select 1 from tbl_errors) THEN
       RAISE INFO 'There is bad data.';
       EXECUTE 'select array_to_string(array_agg(message),chr(13)) from tbl_errors' INTO v_return_value;
       IF arg_proceed THEN
          RAISE INFO 'Proceed with bad data.';
          EXECUTE 'UPDATE client_cue.rc_file SET status_id = '||v_ready||', note = null, processed_dt = now(), processed_by = '||coalesce(v_create_by,0)||' WHERE file_id = '||arg_file_id;
          RETURN 'Complete'::text;
       ELSE
          RETURN coalesce(v_return_value,'');
       END IF;
    ELSE       
       RAISE INFO 'No bad data.';
       v_cmd := 'UPDATE client_cue.rc_file SET status_id = '||v_ready||', note = null, processed_dt = now(), processed_by = '||coalesce(v_create_by,0)||' WHERE file_id = '||arg_file_id;
       -- RAISE INFO 'v_cmd = %', v_cmd;
       EXECUTE v_cmd;
       RETURN 'Complete'::text;
    END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;