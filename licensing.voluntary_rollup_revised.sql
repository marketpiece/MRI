-- Function: licensing.voluntary_rollup_revised(BIGINT, CHARACTER VARYING, BIGINT, CHARACTER VARYING, BOOLEAN)
 
-- DROP FUNCTION licensing.voluntary_rollup_revised(BIGINT, CHARACTER VARYING, BIGINT, CHARACTER VARYING, BOOLEAN);
 
CREATE OR REPLACE FUNCTION licensing.voluntary_rollup_revised(
   arg_client_id            BIGINT,
   arg_royalty_statement_id CHARACTER VARYING,
   arg_row_id               BIGINT,
   arg_check_number         CHARACTER VARYING,
   arg_override_constraint  BOOLEAN DEFAULT FALSE)
  RETURNS character varying AS
$BODY$
/* $DESCRIPTION = Unified revision of volontary rollup
*/
DECLARE
   cmd                         TEXT;
   v_schema                    TEXT;
   v_file                      TEXT;
   v_report_table_name         TEXT;
   v_table_name                TEXT;
   v_client_name               TEXT;
   v_account_id                VARCHAR;
   v_format_type               INTEGER;
   v_negative_allowed          BOOLEAN;
   v_publisher                 TEXT;
   v_payee_id                  INTEGER;
   v_admin_id                  INTEGER;
   v_co_id                     INTEGER;
   v_check_number              CHARACTER VARYING;
   v_period_quarter            TEXT;
   v_quarter                   TEXT;
   v_year                      TEXT;
   v_start_date                TEXT;
   v_end_date                  TEXT;
   v_check_amount              NUMERIC;
   v_filepath                  TEXT;
   v_user                      INTEGER;
   v_file_delimiter            VARCHAR;
   v_header                    VARCHAR;
   v_file_extension            VARCHAR;
   v_e_msg                     VARCHAR DEFAULT NULL;
   v_check_number_flg          BOOLEAN DEFAULT FALSE;
   v_royalty_statement_id      VARCHAR;
   v_remitted_statement_id     BIGINT;
   v_cmd                       TEXT;
 
   v_stmt_count                INTEGER;
   v_payee_count               INTEGER;
   v_format_type_count         INTEGER;
   v_is_processed              BOOLEAN DEFAULT FALSE;
   v_sub_acc_count             INTEGER;
   v_rd_property               VARCHAR;
   v_rad_property              VARCHAR;
   v_report_columns            VARCHAR;
   v_copy_columns              VARCHAR;
   v_rep_groupby_cols          VARCHAR;
   v_rep_groupby_src           VARCHAR;
   v_rep_notgroupby_cols       VARCHAR;
   v_rep_notgroupby_src        VARCHAR;
   v_rec                       RECORD; 
 BEGIN
 
   IF arg_client_id IS NULL THEN
      RAISE EXCEPTION 'Client_id cannot be null. Please set the client_id.';
   END IF;
 
   -- Determining the schema
   cmd := 'SELECT content_schema, company_name FROM licensing.license_companies WHERE company_id = '||arg_client_id;
   EXECUTE cmd INTO v_schema, v_client_name;
   
   IF v_schema IS NULL OR v_client_name IS NULL THEN
      RAISE EXCEPTION 'The selected client_id does not have schema/client_name; schema = %, client_name = %', v_schema, v_client_name;
   END IF;
   
   IF arg_royalty_statement_id IS NULL AND arg_check_number IS NULL THEN
      RAISE EXCEPTION 'Please set the statement_id or the check_number.';
   END IF;
 
   /* Checks:
         1. statement count
         2. payee count
         3. statement and check_number association
         4. whether the rollup is already processed
      Fetches:
         1. check_number, statement_id, account_id(s), admin_id, period_quarter, check_amount
         2. Format type, which represents the Publisher
   */
   cmd := 'WITH main AS ( SELECT COUNT(DISTINCT st.royalty_statement_id) AS stmt_count,
                                 COUNT(DISTINCT st.payee_id) AS payee_count,
                                 MAX(st.check_number) AS check_number,
                                 MAX(st.royalty_statement_id) AS royalty_statement_id,
                                 MAX(st.payee_id) AS payee_id,
                                 MAX(st.admin_id) AS admin_id, 
                                 MAX(st.co_id) AS co_id, 
                                 MAX(st.period_quarter) AS period_quarter,
                                 MAX(st.check_amount) AS check_amount,
                                 MAX(st.remitted_statement_id) AS remitted_statement_id,
                                 COUNT(DISTINCT af.format_type) AS format_type_count,
                                 MAX(COALESCE(af.format_type,1)) AS format_type,
                                 ARRAY_AGG(DISTINCT st.account_id) AS acc_ids
                            FROM '||v_schema||'.royalty_statement st
                            LEFT JOIN licensing.rollup_account_format af ON af.account_id = st.account_id
                           WHERE true
                            '||CASE WHEN arg_check_number IS NOT NULL THEN ' AND check_number = '||quote_literal(arg_check_number) ELSE '' END||'
                            '||CASE WHEN arg_royalty_statement_id IS NOT NULL THEN ' AND royalty_statement_id = '||arg_royalty_statement_id ELSE '' END||'
                          ),
                  chk1 AS (
                            SELECT COUNT(*)::int AS rollup_count
                              FROM licensing.rollup_create_log
                             WHERE (file_path IS NOT NULL AND error IS NULL)
                               AND NOT COALESCE(invalid, false)
                              '||CASE WHEN arg_check_number IS NOT NULL THEN ' AND check_number = '||quote_literal(arg_check_number) ELSE '' END||'
                              '||CASE WHEN arg_royalty_statement_id IS NOT NULL THEN ' AND statement_id = '||arg_royalty_statement_id ELSE '' END||'
                              '||CASE WHEN arg_row_id IS NOT NULL THEN ' AND rollup_id = '||arg_row_id ELSE '' END||'
                           ),
                  chk2 AS (SELECT COUNT(*) sub_acc_count
                             FROM irs.super_payee_group irs, main m
                            WHERE irs.sub_account_id IN (SELECT * FROM UNNEST(m.acc_ids))
                           ),
                  chk3 AS (SELECT COUNT(*) AS check_number_count
                             FROM '||v_schema||'.royalty_statement st
                             JOIN main m ON m.check_number = st.check_number
                          ),
                  chk4 AS (SELECT ARRAY_AGG(DISTINCT st.account_id) AS account_ids
                             FROM '||v_schema||'.royalty_statement st
                             LEFT JOIN main m ON m.check_number = st.check_number
                          )
           SELECT m.stmt_count, m.payee_count, m.check_number, m.royalty_statement_id, m.payee_id, m.admin_id, m.co_id, m.period_quarter, m.check_amount, m.remitted_statement_id, m.format_type_count, m.format_type, ARRAY_TO_STRING(CASE WHEN c3.check_number_count > 1 THEN c4.account_ids ELSE m.acc_ids END,'',''),
                  CASE WHEN c1.rollup_count > 0 THEN TRUE ELSE FALSE END,
                  c2.sub_acc_count, 
                  CASE WHEN c3.check_number_count > 1 THEN TRUE ELSE FALSE END AS check_number_flg
             FROM main m
             NATURAL JOIN chk1 c1
             NATURAL JOIN chk2 c2
             NATURAL JOIN chk3 c3
             NATURAL JOIN chk4 c4';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_stmt_count, v_payee_count,  v_check_number, v_royalty_statement_id, v_payee_id, v_admin_id, v_co_id, v_period_quarter, v_check_amount, v_remitted_statement_id, v_format_type_count, v_format_type, v_account_id,
                    v_is_processed,
                    v_sub_acc_count,
                    v_check_number_flg;
   
   v_e_msg := CASE WHEN arg_royalty_statement_id IS NOT NULL AND arg_check_number IS NOT NULL AND (v_stmt_count = 0) THEN
                        FORMAT('The selected statement_id [%s] is not associated with the selected check_number [%s]. Please set the one you are not sure with to null!', arg_royalty_statement_id, arg_check_number) 
                   WHEN (arg_royalty_statement_id IS NOT NULL AND arg_check_number IS NULL) AND (v_stmt_count = 0) THEN
                        FORMAT('There is no statement with the selected statement_id [%s]', arg_royalty_statement_id)
                   WHEN (arg_royalty_statement_id IS NULL AND arg_check_number IS NOT NULL) AND (v_stmt_count = 0) THEN
                        FORMAT('There is no check with the selected check_number [%s]', arg_check_number)
                   WHEN (arg_check_number IS NOT NULL) AND (v_payee_count > 1) THEN
                        FORMAT('There are %s payee_ids with the selected check_number [%s]', v_payee_count, arg_check_number)
                   WHEN (arg_check_number IS NOT NULL) AND (v_format_type_count > 1) THEN
                        FORMAT('There are more than one format types for check_number [%s]. There should be only one.', arg_check_number)
              ELSE NULL
              END;
   
   IF v_e_msg IS NOT NULL THEN
      RAISE EXCEPTION '%', v_e_msg;
   END IF;
   
   IF v_is_processed THEN 
      RETURN FORMAT('This %s is already processed!', CASE WHEN arg_royalty_statement_id IS NOT NULL AND arg_check_number IS NOT NULL THEN 
                                                               FORMAT('statement_id [%s] and check_number [%s]', arg_royalty_statement_id, arg_check_number)
                                                          WHEN arg_royalty_statement_id IS NOT NULL AND arg_check_number IS NULL THEN
                                                               FORMAT('statement_id [%s]', arg_royalty_statement_id)
                                                          WHEN arg_royalty_statement_id IS NULL AND arg_check_number IS NOT NULL THEN
                                                               FORMAT('check_number [%s]', arg_check_number)
                                                     END);
   END IF;
   
   -- Multiple statements
   IF (v_sub_acc_count = 1) AND (v_period_quarter >= '2014Q4') AND NOT arg_override_constraint THEN
      RETURN 'Payee ID is excluded from being run in this function. If you would like to run it anyway,' ||CHR(10)||
             'please call function with arg_override_constraint set to TRUE.';
   END IF;

   -- Retrive all rollup attributes 
   cmd := 'WITH p1 AS (SELECT format_desc, negative_allowed, 
                              CASE WHEN header_allowed THEN ''HEADER''::VARCHAR ELSE '''' END AS header_name, file_delimiter, file_extension
                         FROM licensing.rollup_format
                        WHERE format_type = '||v_format_type||'
                      ),
                p2 AS (SELECT ARRAY_TO_STRING(ARRAY_AGG(source_name ORDER BY column_order),'','') AS source_name,
                              ARRAY_TO_STRING(ARRAY_AGG(source_name2 ORDER BY column_order),'','') AS source_name2
                         FROM licensing.rollup_report_properties
                        WHERE client_id = '||arg_client_id||'
                      ),
                p3 AS (SELECT ARRAY_TO_STRING(ARRAY_AGG(column_name||'' ''||data_type ORDER BY column_order),'','') AS column_name,
                              ARRAY_TO_STRING(ARRAY_REMOVE(ARRAY_AGG(column_header ORDER BY column_order), NULL),'','') AS copy_columns,
                              ARRAY_TO_STRING(ARRAY_REMOVE(ARRAY_AGG(CASE WHEN provided AND group_by THEN column_name ELSE NULL END ORDER BY column_order), NULL),'','') AS col_provided_groupby,
                              ARRAY_TO_STRING(ARRAY_REMOVE(ARRAY_AGG(CASE WHEN provided AND group_by THEN source_name ELSE NULL END ORDER BY column_order), NULL),'','') AS sour_provided_groupby,
                              ARRAY_TO_STRING(ARRAY_REMOVE(ARRAY_AGG(CASE WHEN provided AND NOT group_by THEN column_name ELSE NULL END ORDER BY column_order), NULL),'','') AS col_provided_not_groupby,
                              ARRAY_TO_STRING(ARRAY_REMOVE(ARRAY_AGG(CASE WHEN provided AND NOT group_by THEN REPLACE(source_name,source_name,''SUM(''||source_name||'')'') ELSE NULL END ORDER BY column_order), NULL),'','') AS sour_provided_not_groupby
                         FROM licensing.rollup_properties
                        WHERE rollup_type = ''V''
                          AND format_type = '||v_format_type||'
                       )
           SELECT * 
             FROM p1 
             NATURAL JOIN p2
             NATURAL JOIN p3';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_publisher, v_negative_allowed, v_header, v_file_delimiter, v_file_extension,
                    v_rd_property, v_rad_property, 
                    v_report_columns, v_copy_columns, v_rep_groupby_cols, v_rep_groupby_src, v_rep_notgroupby_cols, v_rep_notgroupby_src;

   
   v_table_name := 'stmnt_'||CASE WHEN (v_stmt_count = 1) AND COALESCE(arg_check_number, v_check_number) IS NOT NULL THEN
                                  v_payee_id||'_'||REGEXP_REPLACE(COALESCE(arg_check_number, v_check_number),'-','_','g')||'_'||v_period_quarter
                                  WHEN (v_stmt_count = 1) AND COALESCE(arg_check_number, v_check_number) IS NULL THEN
                                  v_payee_id||'_Recouped_'||v_period_quarter||'_'||arg_royalty_statement_id
                             ELSE v_admin_id||'_'||REGEXP_REPLACE(COALESCE(arg_check_number, v_check_number),'-','_','g')||'_'||v_period_quarter
                             END;
   v_report_table_name := 'v_report_'||CASE WHEN (v_stmt_count = 1) AND COALESCE(arg_check_number, v_check_number) IS NOT NULL THEN
                                            v_payee_id||'_'||REGEXP_REPLACE(COALESCE(arg_check_number, v_check_number),'-','_','g')
                                            WHEN (v_stmt_count = 1) AND COALESCE(arg_check_number, v_check_number) IS NULL THEN
                                            'recouped_'||v_period_quarter||'_'||arg_royalty_statement_id
                                       ELSE v_admin_id||'_'||REGEXP_REPLACE(COALESCE(arg_check_number, v_check_number),'-','_','g')
                                       END;                                       
   RAISE INFO 'v_table_name = %, v_report_table_name = %', v_table_name, v_report_table_name;   

   IF EXISTS (SELECT 1 
                FROM information_schema.tables
               WHERE table_schema = v_schema
                 AND table_name = LOWER(v_table_name)) THEN
      RAISE INFO 'Table %.% already exists...',v_schema , v_table_name;
      cmd := 'TRUNCATE '||v_schema||'.'||v_table_name;
      EXECUTE cmd;
   ELSE  
      cmd := 'CREATE TABLE '||v_schema||'.'||v_table_name||' ('||v_report_columns||')';
      RAISE INFO 'cmd = %', cmd;
      EXECUTE cmd;
   END IF;

   /* General Report Table */
   cmd := 'CREATE TEMP TABLE '||v_report_table_name||' (
              payee_id              BIGINT,
              service_name          CHARACTER VARYING,
              check_number          CHARACTER VARYING,
              isrc                  CHARACTER VARYING,
              member_reference      INTEGER,
              territory             CHARACTER VARYING,
              client_name           CHARACTER VARYING,
              upc_ean               CHARACTER VARYING,
              pip_code              CHARACTER VARYING,
              sales_period_start    DATE,
              sales_period_end      DATE,
              offering_id           INTEGER,
              amount                NUMERIC,
              recoup_amount         NUMERIC,
              royalty_statement_id  BIGINT,
              source_song_id        INTEGER,
              song_title            CHARACTER VARYING,
              composers             CHARACTER VARYING,
              publisher_share       NUMERIC(5,4),
              play_count            BIGINT,
              weighted_play_count   NUMERIC,
              publisher_name        CHARACTER VARYING,
              per_play_rate         NUMERIC,
              period_month          DATE,
              product_title         CHARACTER VARYING,
              artist                CHARACTER VARYING,
              publisher_song_code   CHARACTER VARYING, 
              sales_type            CHARACTER VARYING,
              client_catalog_id     CHARACTER VARYING,
              duration              CHARACTER VARYING,
              tier_name             CHARACTER VARYING
              '||CASE WHEN v_format_type = 3 THEN ', payee_name           CHARACTER VARYING' ELSE '' END||'
            ) ON COMMIT DROP';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;
         
   -- Creating report_adjustment_detail table
   cmd := 'CREATE TEMP TABLE report_adj_detail (
              report_summary_id   BIGINT,
              license_row_id      BIGINT,
              source_song_id      BIGINT,
              source_song_title   CHARACTER VARYING,
              composers character VARYING,
              publisher_share     NUMERIC(5,4),
              play_count          INTEGER,
              weighted_count      NUMERIC,
              publisher_name      CHARACTER VARYING,
              effective_rate      NUMERIC,
              period_month        DATE,
              track_album         CHARACTER VARYING,
              track_artist        CHARACTER VARYING,
              play_type_id        INTEGER,
              sub_tier_id         INTEGER,
              new_royalties       NUMERIC(12,5)
            ) ON COMMIT DROP';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;
   
   -- IF v_sub_acc_count = 0 OR (v_period_quarter  < '2014Q4') OR arg_override_constraint THEN
   cmd := 'WITH reps AS (SELECT report_summary_id
                           FROM '||v_schema||'.royalty_statement 
                         '||CASE WHEN NOT v_check_number_flg AND arg_royalty_statement_id IS NOT NULL THEN 
                                 'WHERE remitted_statement_id = '||COALESCE(v_remitted_statement_id,0)
                                 WHEN v_check_number_flg THEN                                  
                                 'WHERE check_number = '||QUOTE_LITERAL(COALESCE(arg_check_number,v_check_number)) 
                             ELSE 'WHERE royalty_statement_id = '||v_royalty_statement_id
                             END||'
                          GROUP BY report_summary_id
                        ),
                repa AS (
                         SELECT (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).report_summary_id,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).license_row_id,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).source_song_id,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).source_song_title,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).composer_composite,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).new_publisher_share,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).play_count,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).new_weighted_count,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).publisher_name,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).new_effective_rate_amount,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).period_yr,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).period_qtr,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).track_album,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).track_artist,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).play_type_id,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).new_effective_rate_type_id,
                                (licensing.report_adjustment_detail('||arg_client_id||', r.report_summary_id)).new_royalty_amount
                           FROM reps r
                          )
           INSERT INTO report_adj_detail
           SELECT report_summary_id, license_row_id, source_song_id, source_song_title, composer_composite, new_publisher_share, play_count,
                  new_weighted_count, publisher_name, new_effective_rate_amount, 
                 (period_yr::varchar||CASE WHEN length((period_qtr*3-2)::varchar) = 1 THEN ''0''||(period_qtr*3-2)::varchar ELSE (period_qtr*3-2)::varchar END||''01'')::date AS period_month,
                  track_album, track_artist, play_type_id, 
                  CASE WHEN (play_type_id IN (10,20) AND '||arg_client_id||' = 5818802) THEN new_effective_rate_type_id::int4 ELSE NULL END AS tier_id,
                  new_royalty_amount
             FROM repa';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;
   
   v_client_name := LOWER(REPLACE(v_client_name, '.', ''));
   v_filepath := '/mnt/itops-prod/workproduct/clients/'||v_client_name||'/accounting/rollup/'||v_period_quarter;
   v_file := REGEXP_REPLACE(v_table_name, 'stmnt_', '')||v_file_extension;
   v_table_name := LOWER(v_table_name);
   RAISE INFO 'v_table_name = %', v_table_name;
       
   cmd := 'SELECT royalty_statement_id, payee_id, period_quarter
             FROM '||v_schema||'.royalty_statement 
             '||CASE WHEN NOT v_check_number_flg AND arg_royalty_statement_id IS NOT NULL THEN
                     'WHERE remitted_statement_id = '||COALESCE(v_remitted_statement_id,0)
                     WHEN v_check_number_flg THEN
                     'WHERE check_number = '||QUOTE_LITERAL(COALESCE(arg_check_number,v_check_number))
                ELSE 'WHERE royalty_statement_id = '||v_royalty_statement_id
                END;
   RAISE INFO 'cmd = %', cmd;

   FOR v_rec IN EXECUTE cmd LOOP
      WITH chk AS (SELECT COUNT(1) AS chk_count
                     FROM information_schema.columns
                    WHERE table_schema = v_schema
                      AND table_name = 'report_detail_'||LOWER(v_period_quarter)
                      AND column_name = 'tier_id')
      SELECT 
             'WITH service_table AS (SELECT * FROM licensing.service_name WHERE client_id = '||arg_client_id||')
              INSERT INTO '||v_report_table_name||'
              SELECT '||v_rec.payee_id||' AS Payee, service_table.service_name, COALESCE(rs.check_number, ''RECOUPED''), lp.client_data_8,
                     '||v_account_id||' AS account_id,  ''USA'', '||QUOTE_LITERAL(v_client_name)||', lp.client_data_6, NULL,
                     (SUBSTRING(rs.period_quarter, ''\d{4}'')||CASE SUBSTRING(rs.period_quarter FROM 6 FOR 1)::INT4
                                                                   WHEN 1 THEN ''-01-01''
                                                                   WHEN 2 THEN ''-04-01''
                                                                   WHEN 3 THEN ''-07-01''
                                                                   WHEN 4 THEN ''-10-01''
                                                              ELSE NULL 
                                                              END)::DATE AS start_date,
                     (SUBSTRING(rs.period_quarter, ''\d{4}'')||CASE SUBSTRING(rs.period_quarter FROM 6 FOR 1)::INT4
                                                                   WHEN 1 THEN ''-03-31''
                                                                   WHEN 2 THEN ''-03-30''
                                                                   WHEN 3 THEN ''-09-30''
                                                                   WHEN 4 THEN ''-12-31''
                                                              ELSE NULL 
                                                              END)::DATE AS end_date,
                     service_table.noi_offering_id, 
                     CASE WHEN '||v_check_number_flg||' IS TRUE THEN rd.royalties_remit ELSE rd.royalties END,
                     COALESCE(rd.royalties_recoup, 0), 
                     rs.royalty_statement_id, 
                     '||v_rd_property
                      ||CASE WHEN v_format_type = 3 THEN ', c.company_name' ELSE '' END||'
                FROM '||v_schema||'.royalty_statement rs
                LEFT JOIN '||v_schema||'.royalty_statement rs2 ON rs2.remitted_statement_id = rs.royalty_statement_id
                JOIN '||v_schema||'.report_detail_'||v_rec.period_quarter||' rd ON rd.report_summary_id = COALESCE(rs2.report_summary_id,rs.report_summary_id)
                JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = rd.license_row_id
                '||CASE WHEN v_format_type = 3 THEN 'JOIN songdex.company c ON c.company_id = rs.payee_id' ELSE '' END||'
                LEFT JOIN service_table on COALESCE(service_table.play_type_id, 0) = COALESCE(rd.play_type_id, 0)
                '||CASE WHEN chk_count = 1 THEN 'LEFT JOIN licensing.rate_tier rt ON rt.tier_id = rd.tier_id AND rt.client_id = '||arg_client_id ELSE '' END||'
               WHERE rs.royalty_statement_id = '||v_rec.royalty_statement_id||'
               
              UNION ALL /* Adjastment detail */
              
              SELECT '||v_rec.payee_id||' AS Payee, service_table.service_name, COALESCE(rs.check_number, ''RECOUPED''), lp.client_data_8,
                     '||v_account_id||' AS account_id,  ''USA'', '||QUOTE_LITERAL(v_client_name)||', lp.client_data_6, NULL,
                     (SUBSTRING(rs.period_quarter, ''\d{4}'')||CASE SUBSTRING(rs.period_quarter FROM 6 FOR 1)::INT4 
                                                                   WHEN 1 THEN ''-01-01''
                                                                   WHEN 2 THEN ''-04-01''
                                                                   WHEN 3 THEN ''-07-01''
                                                                   WHEN 4 THEN ''-10-01''
                                                              ELSE NULL 
                                                              END)::DATE AS start_date,
                     (SUBSTRING(rs.period_quarter, ''\d{4}'')||CASE SUBSTRING(rs.period_quarter FROM 6 FOR 1)::INT4
                                                                   WHEN 1 THEN ''-03-31''
                                                                   WHEN 2 THEN ''-03-30''
                                                                   WHEN 3 THEN ''-09-30''
                                                                   WHEN 4 THEN ''-12-31''
                                                              ELSE NULL 
                                                              END)::DATE AS end_date,
                     service_table.noi_offering_id, rad.new_royalties, 0, rs.royalty_statement_id,
                     '||v_rad_property
                      ||CASE WHEN v_format_type = 3 THEN ', c.company_name' ELSE '' END||'
                FROM '||v_schema||'.royalty_statement rs 
                JOIN report_adj_detail rad ON rad.report_summary_id = rs.report_summary_id
                JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = rad.license_row_id
                '||CASE WHEN v_format_type = 3 THEN 'JOIN songdex.company c ON c.company_id = rs.payee_id' ELSE '' END||'
                LEFT JOIN service_table on COALESCE(service_table.play_type_id, 0) = COALESCE(rad.play_type_id, 0)
               WHERE rs.royalty_statement_id = '||v_rec.royalty_statement_id||'
     
              '||CASE WHEN v_negative_allowed THEN 'UNION ALL /* Negative allowed */
              
              SELECT '||v_rec.payee_id||' AS Payee, service_table.service_name, COALESCE(rs.check_number, ''RECOUPED''), lp.client_data_8,
                     '||v_account_id||' AS account_id,  ''USA'', '||QUOTE_LITERAL(v_client_name)||', lp.client_data_6, NULL,
                     (SUBSTRING(rs.period_quarter, ''\d{4}'')||CASE SUBSTRING(rs.period_quarter FROM 6 FOR 1)::INT4 
                                                                   WHEN 1 THEN ''-01-01''
                                                                   WHEN 2 THEN ''-04-01''
                                                                   WHEN 3 THEN ''-07-01''
                                                                   WHEN 4 THEN ''-10-01''
                                                              ELSE NULL 
                                                              END)::DATE AS start_date,
                     (SUBSTRING(rs.period_quarter, ''\d{4}'')||CASE SUBSTRING(rs.period_quarter FROM 6 FOR 1)::INT4
                                                                   WHEN 1 THEN ''-03-31''
                                                                   WHEN 2 THEN ''-03-30''
                                                                   WHEN 3 THEN ''-09-30''
                                                                   WHEN 4 THEN ''-12-31''
                                                              ELSE NULL 
                                                              END)::DATE AS end_date,
                     service_table.noi_offering_id,
                     CASE WHEN '||v_check_number_flg||' IS TRUE THEN rd.royalties_remit*(-1) ELSE rd.royalties*(-1) END,
                     COALESCE(rd.royalties_recoup*(-1), 0), 
                     rs.royalty_statement_id,
                     '||v_rd_property
                      ||CASE WHEN v_format_type = 3 THEN ', c.company_name' ELSE '' END||'
                FROM '||v_schema||'.royalty_statement rs
                LEFT JOIN '||v_schema||'.royalty_statement rs2 ON rs2.remitted_statement_id = rs.royalty_statement_id
                JOIN '||v_schema||'.report_detail_'||v_rec.period_quarter||' rd ON rd.report_summary_id = COALESCE(rs2.report_summary_id,rs.report_summary_id)
                JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = rd.license_row_id
                '||CASE WHEN v_format_type = 3 THEN 'JOIN songdex.company c ON c.company_id = rs.payee_id' ELSE '' END||'
                LEFT JOIN service_table on COALESCE(service_table.play_type_id, 0) = COALESCE(rd.play_type_id, 0)
                '||CASE WHEN chk_count = 1 THEN 'LEFT JOIN licensing.rate_tier rt ON rt.tier_id = rd.tier_id AND rt.client_id = '||arg_client_id ELSE '' END||'
               WHERE rs.royalty_statement_id = '||v_rec.royalty_statement_id||'
                 AND rs.total_accountable = rs.total_recoupment'
               ELSE ''
               END
        FROM chk
        INTO cmd;
      RAISE INFO 'cmd = %', cmd;
      EXECUTE cmd;
   END LOOP;
        
   -- Updating the general report table with pip_code values
   cmd := 'UPDATE '||v_report_table_name||' a
              SET pip_code = foo.pip_code1
             FROM (SELECT distinct a.pip_code, a.composers, a.title, b.pip_code as pip_code1, b.source_song_id
                     FROM umpg.pip_code_catalog a
                    RIGHT JOIN umpg.source_song_to_pip_pivot b on a.pip_code = b.pip_code
                     LEFT JOIN umpg.pip_code c on c.pip_code = b.pip_code
                    WHERE active) foo
           WHERE a.source_song_id = foo.source_song_id';
   RAISE INFO '%', cmd;
   EXECUTE cmd;
   
   -- Updating publisher_song_code
   cmd := 'UPDATE '||v_report_table_name||'
              SET publisher_song_code = licensing.retrieve_publisher_song_code(source_song_id, payee_id)
            WHERE publisher_song_code IS NULL';
   RAISE INFO '%', cmd;
   EXECUTE cmd;
   
   -- Feeding output report
   cmd :='INSERT INTO '||v_schema||'.'||v_table_name||'('||v_rep_groupby_cols||','||v_rep_notgroupby_cols||')
          SELECT '||v_rep_groupby_src||','||v_rep_notgroupby_src||'
            FROM '||v_report_table_name||'
           GROUP BY '||v_rep_groupby_src||'';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;

   -- Update/Insert into rollup_create_log
   cmd := 'WITH stmts AS (SELECT DISTINCT royalty_statement_id, account_id, admin_id, co_id, check_number, period_quarter, check_amount, client_id,
                                 period_remittance, period_accountable, period_recoupment, payee_id, remitted_statement_id,
                                 CASE WHEN '||v_check_number_flg||' IS TRUE THEN period_remittance ELSE period_accountable - period_recoupment END AS statement_amount
                            FROM '||v_schema||'.royalty_statement
                          '||CASE WHEN NOT v_check_number_flg AND arg_royalty_statement_id IS NOT NULL THEN
                                  'WHERE remitted_statement_id = '||COALESCE(v_remitted_statement_id,0)
                                  WHEN v_check_number_flg THEN
                                  'WHERE check_number = '||QUOTE_LITERAL(COALESCE(arg_check_number,v_check_number))
                             ELSE 'WHERE royalty_statement_id = '||v_royalty_statement_id
                             END||'
                         ),
                rollupamount AS (SELECT royalty_statement_id,
                                        ROUND(CASE WHEN '||v_check_number_flg||' IS TRUE THEN SUM(amount) ELSE SUM(amount - recoup_amount) END, 5) AS rollup_amount
                                   FROM '||v_report_table_name||'
                                  GROUP BY royalty_statement_id
                                ),
                report_attr AS (SELECT s.royalty_statement_id, s.account_id, s.admin_id, s.co_id, s.check_number, s.period_quarter, s.check_amount, s.payee_id, '||v_format_type||' AS format_type,
                                       CASE WHEN r.rollup_amount NOT BETWEEN ((s.statement_amount) - 0.05) AND ((s.statement_amount) + 0.05) THEN
                                            NOW()||'', rollup amount not equal to statement amount: statement_id: ''||s.royalty_statement_id||'': rollup = ''||r.rollup_amount||'', statement = ''||s.statement_amount
                                       ELSE NULL
                                       END AS err_msg,
                                       '||QUOTE_LITERAL(v_filepath||'/'||v_file)||' AS file_path,
                                       CASE WHEN log.statement_id IS NOT NULL THEN TRUE ELSE FALSE END AS rollup_exists
                                 FROM stmts s 
                                 LEFT JOIN rollupamount r ON r.royalty_statement_id = s.royalty_statement_id
                                 LEFT JOIN licensing.rollup_create_log log ON log.statement_id = r.royalty_statement_id
                                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
                              ),
                update_rollups AS (UPDATE licensing.rollup_create_log l
                                      SET admin_id = r.admin_id,
                                          payee_id = r.payee_id,
                                          co_id = r.co_id,
                                          file_path = CASE WHEN r.err_msg IS NOT NULL THEN NULL ELSE r.file_path END,
                                          error = r.err_msg,
                                          invalid = ''f'',
                                          check_amount = r.check_amount
                                     FROM report_attr r
                                    WHERE l.statement_id = r.royalty_statement_id
                                      AND r.rollup_exists
                                      AND l.client_id = '||arg_client_id||'
                                      AND COALESCE(l.invalid, FALSE) = FALSE
                                  )
           INSERT INTO licensing.rollup_create_log(statement_id, account_id, admin_id, payee_id, co_id, period_quarter, format_type, error, file_path,
                                                   create_dt, invalid, check_number, check_amount, client_id, create_by)
           SELECT royalty_statement_id, account_id, admin_id, payee_id, co_id, period_quarter, format_type, err_msg, CASE WHEN err_msg IS NOT NULL THEN NULL ELSE file_path END,
                   NOW()::TIMESTAMP, ''f'', check_number, check_amount, '||arg_client_id||', retrieve_user_id()
             FROM report_attr
            WHERE NOT rollup_exists';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;

   -- Output
   cmd := 'COPY ( SELECT '||v_copy_columns||'
           FROM '||v_schema||'.'||v_table_name||' )
             TO '''||v_filepath||'/'||v_file||E''' 
           WITH CSV '||v_header||' DELIMITER AS '''||v_file_delimiter||E'''';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;

   RETURN v_filepath||'/'||v_file;
   
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100; 