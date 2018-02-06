-- Function: licensing.voluntary_rollup(bigint, character varying, bigint, character varying, boolean)

-- DROP FUNCTION licensing.voluntary_rollup(bigint, character varying, bigint, character varying, boolean);

/* Redundant legacy code built by my colleague, which late I inheritted and revised
   Off me such redundancy
*/
CREATE OR REPLACE FUNCTION licensing.voluntary_rollup(
    bigint,
    character varying,
    bigint,
    character varying,
    boolean DEFAULT false)
  RETURNS character varying AS
$BODY$
-- $DESCRIPTION =  
-- $1 = arg_client_id
-- $2 = arg_royalty_statement_id
-- $3 = arg_row_id
-- $4 = arg_check_number
-- $5 = arg_override_constraint

DECLARE

arg_client_id               ALIAS for $1;
arg_royalty_statement_id    ALIAS for $2;
arg_row_id                  ALIAS for $3;
arg_check_number            ALIAS for $4;
arg_override_constraint     ALIAS for $5;

cmd                         text;
v_schema                    text;
v_file                      text;
v_report_table_name         text;
v_table_name                text;
v_client_name               text;
v_column_count              integer;
--v_account_id                integer;
v_account_id                varchar;
v_format_type               integer;
v_negative_allowed          boolean;
v_publisher                 text;
v_payee_id                  integer;
v_admin_id                  integer;
v_co_id                     integer;
v_check_number              character varying;
v_period_quarter            text;
v_quarter                   text;
v_year                      text;
v_start_date                text;
v_end_date                  text;
v_check_amount              numeric;
ctr                         integer;
v_str                       text;
v_str1                      text;
v_src                       text;
v_src2                      text;
v_col                       text;
v_col2                      text;
v_code                      text;
v_rec                       record;
v_rec1                      record;
v_rec2                      record;
v_rec3                      record;
v_rec4                      record;
v_recS                      record;
v_rec6                      record;
v_rec7                      record;
v_rec8                      record;
v_filepath                  text;
v_user                      integer;
v_header_allowed            boolean;
v_file_delimiter            varchar;
v_header                    varchar;
v_file_extension            varchar;
v_count                     integer;
v_remitted_stmnt            bigint;
v_field                     varchar;
v_payee_name                varchar;
v_comp_name                 varchar;
v_join                      varchar;
v_join_tbl                  text;
v_error_flg                 boolean default false;
v_rollup_amount             numeric;
v_e_msg                     varchar default '';
v_payee_ids                 varchar;
v_check_number_flg          boolean default false;
v_royalty_statement_id      varchar;
v_statement_amount          numeric;
v_cmd                       text;
v_col3                      text;
c_str                       text;
v_rep_sum_count             integer;
v_rep_adj_count             integer;
c_cmd                       text;
v_report_summary_id         bigint;

BEGIN

IF arg_client_id IS NULL THEN
   RAISE EXCEPTION 'Client_id cannot be null. Please set the client_id.';
END IF;

-- Determining the schema
cmd := 'SELECT content_schema, company_name
        FROM licensing.license_companies
        WHERE company_id = '||arg_client_id||'';
EXECUTE cmd INTO v_schema, v_client_name;

IF v_schema IS NULL OR v_client_name IS NULL THEN
   RAISE EXCEPTION 'The selected client_id does not have schema/client_name; schema = %, client_name = %', v_schema, v_client_name;
END IF;

IF arg_royalty_statement_id IS NULL AND arg_check_number IS NULL THEN
   RAISE EXCEPTION 'Please set the statement_id or the check_number.';

ELSIF arg_royalty_statement_id IS NOT NULL AND arg_check_number IS NOT NULL THEN
   -- Checking the conformancy of arg_royalty_statement_id and arg_check_number
   cmd := 'SELECT COALESCE(COUNT(*), 0)
           FROM '||v_schema||'.royalty_statement
           WHERE royalty_statement_id = '||arg_royalty_statement_id||'
           AND check_number = '||quote_literal(arg_check_number);
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_count;

   IF v_count = 0 THEN
      RAISE EXCEPTION 'The selected statement_id: % is not associated with the selected check_number: %. Please set the one you are not sure with to null!', arg_royalty_statement_id, arg_check_number;
   END IF;
   
   cmd := 'SELECT COALESCE(COUNT(*), 0)
           FROM '||v_schema||'.royalty_statement
           WHERE check_number = '||quote_literal(arg_check_number);
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_count;

   IF v_count > 1 THEN
      v_check_number_flg := true;
   END IF;
   
   cmd := 'SELECT COUNT(*)
           FROM licensing.rollup_create_log
           WHERE statement_id = '||arg_royalty_statement_id||'
           AND check_number = '||quote_literal(arg_check_number)||'
           AND (file_path IS NOT NULL AND error IS NULL)
           AND COALESCE(invalid, false) = FALSE'; 
   IF arg_row_id IS NOT NULL THEN
      cmd := cmd || ' AND rollup_id = '||arg_row_id;
   END IF; 
                   
ELSIF arg_royalty_statement_id IS NOT NULL THEN
   cmd := 'SELECT COALESCE(COUNT(*), 0)
           FROM '||v_schema||'.royalty_statement
           WHERE royalty_statement_id = '||arg_royalty_statement_id;
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_count;

   IF v_count = 0 THEN
      RAISE EXCEPTION 'There is no statement with the selected statement_id: %', arg_royalty_statement_id;
   END IF;

   cmd := 'SELECT check_number
           FROM '||v_schema||'.royalty_statement
           WHERE royalty_statement_id = '||arg_royalty_statement_id;
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_check_number;

   IF v_check_number IS NOT NULL THEN
   cmd := 'SELECT COALESCE(COUNT(*), 0)
           FROM '||v_schema||'.royalty_statement
           WHERE check_number = '||quote_literal(v_check_number);
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_count;
   IF v_count > 1 THEN
      v_check_number_flg := true;
   END IF;
   END IF;
   
   cmd := 'SELECT COUNT(*)
           FROM licensing.rollup_create_log
           WHERE statement_id = '||arg_royalty_statement_id||'
           AND (file_path IS NOT NULL AND error IS NULL)
           AND COALESCE(invalid, false) = FALSE';   
   IF arg_row_id IS NOT NULL THEN
      cmd := cmd || ' AND rollup_id = '||arg_row_id;
   END IF; 
                 
ELSIF arg_check_number IS NOT NULL THEN
   cmd := 'SELECT COALESCE(COUNT(*), 0)
           FROM '||v_schema||'.royalty_statement
           WHERE check_number = '||quote_literal(arg_check_number);
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_count;

   IF v_count = 0 THEN
      RAISE EXCEPTION 'There is no check with the selected check_number: %', arg_check_number;
   ELSEIF v_count = 1 THEN
      cmd := 'SELECT royalty_statement_id
              FROM '||v_schema||'.royalty_statement
              WHERE check_number = '||quote_literal(arg_check_number);
      RAISE INFO 'cmd = %', cmd;
      EXECUTE cmd INTO v_royalty_statement_id;
   ELSIF v_count > 1 THEN
      v_check_number_flg := true;
   END IF;
   
   cmd := 'SELECT COUNT(*)
           FROM licensing.rollup_create_log
           WHERE check_number = '||quote_literal(arg_check_number)||'
           AND (file_path IS NOT NULL AND error IS NULL)
           AND COALESCE(invalid, false) = FALSE';
   IF arg_row_id IS NOT NULL THEN
      cmd := cmd || ' AND rollup_id = '||arg_row_id;
   END IF;

END IF; 
   
RAISE INFO 'cmd = %', cmd;
EXECUTE cmd INTO v_column_count;
RAISE INFO 'v_column_count = %', v_column_count;

IF v_column_count > 0 THEN
   RETURN 'This statement_id:%/check_number:% is already processed!', arg_royalty_statement_id, arg_check_number;
ELSE
v_user := retrieve_user_id();
  
--IF arg_royalty_statement_id IS NOT NULL AND NOT v_check_number_flg THEN
IF NOT v_check_number_flg THEN
   -- Checking for exceptions
   cmd := 'SELECT COUNT(DISTINCT payee_id)
           FROM '||v_schema||'.royalty_statement
           WHERE royalty_statement_id = '||COALESCE(arg_royalty_statement_id, v_royalty_statement_id);
   EXECUTE cmd INTO v_count;

   IF v_count > 1 THEN
      RAISE EXCEPTION 'There are % payee_ids for statement: %. There should be only one.', v_count, arg_royalty_statement_id;
   END IF; 

   cmd := 'SELECT COUNT(DISTINCT check_number)
           FROM '||v_schema||'.royalty_statement
           WHERE royalty_statement_id = '||COALESCE(arg_royalty_statement_id, v_royalty_statement_id);
   EXECUTE cmd INTO v_count;

   IF v_count > 1 THEN
      RAISE EXCEPTION 'There are % check_numbers for statement: %. There should be only one.', v_count, arg_royalty_statement_id;
   END IF;

   -- Determining table name components
   cmd := 'SELECT payee_id, admin_id, co_id, check_number, period_quarter, check_amount
           FROM '||v_schema||'.royalty_statement
           WHERE royalty_statement_id = '||COALESCE(arg_royalty_statement_id, v_royalty_statement_id);
   EXECUTE cmd INTO v_payee_id, v_admin_id, v_co_id, v_check_number, v_period_quarter, v_check_amount;

   -- Determining account_id to get the publisher
   cmd:= 'SELECT account_id
          FROM '||v_schema||'.royalty_statement
          WHERE royalty_statement_id = '||COALESCE(arg_royalty_statement_id, v_royalty_statement_id)||'
          AND client_id = '||arg_client_id||'';
   EXECUTE cmd INTO v_account_id;
   IF v_account_id IS NULL THEN
      cmd := 'SELECT account_id
              FROM licensing.rollup_account_format
              Where admin_id = '||v_admin_id||'
              AND payee_id = '||v_payee_id||'
              AND co_id = '||v_co_id||'';
      EXECUTE cmd INTO v_account_id;
   END IF;
   IF v_account_id IS NULL THEN
      RAISE EXCEPTION 'The account_id is null for the selected statement [%]', COALESCE(arg_royalty_statement_id, v_royalty_statement_id);
   END IF;
   
   -- Determing publisher
   IF v_account_id IS NULL THEN
      v_format_type := 1;
   ELSE
      cmd := 'SELECT coalesce((SELECT format_type
              FROM licensing.rollup_account_format
              WHERE account_id = '||v_account_id||' 
              GROUP BY format_type), 1)';
   EXECUTE cmd INTO v_format_type;
   END IF;
--ELSIF arg_check_number IS NOT NULL THEN
ELSE --v_check_number_flg THEN   
   cmd := 'SELECT COUNT(DISTINCT account_id)
           FROM '||v_schema||'.royalty_statement
           WHERE check_number = '||quote_literal(COALESCE(arg_check_number, v_check_number));
   EXECUTE cmd INTO v_count;

   cmd := 'SELECT account_id
           FROM '||v_schema||'.royalty_statement
           WHERE check_number = '||quote_literal(COALESCE(arg_check_number, v_check_number))||'
           GROUP BY account_id';
           
   RAISE NOTICE 'Getting all account_ids associated with the check_number...';
   v_account_id := '';
   ctr := 1;
   FOR v_rec7 IN EXECUTE cmd
   LOOP    
       IF ctr = v_count THEN
          v_account_id := v_account_id||v_rec7.account_id;
       ELSE
          v_account_id := v_account_id||v_rec7.account_id||', ';
       END IF;
       ctr := ctr + 1;
   END LOOP;
           
   cmd := 'SELECT admin_id, co_id, period_quarter
           FROM '||v_schema||'.royalty_statement
           WHERE check_number = '||quote_literal(COALESCE(arg_check_number, v_check_number))||'
           GROUP BY admin_id, co_id, period_quarter';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_admin_id, v_co_id, v_period_quarter;
   RAISE INFO 'v_admin_id = %, v_co_id = %, v_period_quarter = %', v_admin_id, v_co_id, v_period_quarter;

   -- Determing publisher
   cmd := 'SELECT coalesce(COUNT(DISTINCT format_type), 0)
           FROM licensing.rollup_account_format f
           WHERE EXISTS (SELECT s.account_id 
                         FROM '||v_schema||'.royalty_statement s 
                         WHERE s.account_id = f.account_id 
                         AND check_number = '||quote_literal(COALESCE(arg_check_number, v_check_number))||' 
                         AND format_type IS NOT NULL)';
   RAISE INFO 'cmd = %', cmd; 
   EXECUTE cmd INTO v_count;

   IF v_count = 0 THEN
      v_format_type = 1;
   ELSIF v_count = 1 THEN
      cmd := 'SELECT format_type 
              FROM licensing.rollup_account_format f
              WHERE EXISTS (SELECT * FROM '||v_schema||'.royalty_statement s 
                            WHERE s.account_id = f.account_id 
                            AND check_number = '||quote_literal(COALESCE(arg_check_number, v_check_number))||' 
                            AND format_type IS NOT NULL) 
              GROUP BY format_type';
      RAISE INFO 'cmd = %', cmd; 
      EXECUTE cmd INTO v_format_type;
  ELSE
      RAISE EXCEPTION 'There are more than one format_type for check_number = %. There should be only one.', arg_check_number;
  END IF; 
END IF;      

RAISE INFO 'v_format_type = %', v_format_type;
-- Adding payee_name for UMPG
IF v_format_type = 3 THEN
   RAISE INFO 'format type is 3';
   v_field := ', '||'payee_name character varying';
   v_payee_name := ', '||'payee_name';
   v_comp_name := ', '||'company_name';
   v_join := 'JOIN songdex.company c ON c.company_id = st.payee_id';
ELSE
   v_field := '';
   v_payee_name := '';
   v_comp_name := '';
   v_join := '';
END IF;

v_join_tbl := 'report_detail_'||lower(v_period_quarter);
/*
IF NOT EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = v_schema
               AND table_name = v_join_tbl
               AND column_name = 'tier_id') THEN */
v_count := 0;
cmd := 'SELECT 1 FROM information_schema.columns
               WHERE table_schema = '''||v_schema||'''
               AND table_name = '''||v_join_tbl||'''
               AND column_name = ''tier_id''';
RAISE INFO 'cmd = %', cmd;
EXECUTE cmd INTO v_count;
RAISE INFO 'v_count = %', v_count;

IF v_count = 1 THEN              
      v_join_tbl := 'LEFT JOIN licensing.rate_tier rt ON rt.tier_id = rd.tier_id AND rt.client_id = '||arg_client_id||' ';
ELSE
      v_join_tbl := '';
END IF;

RAISE INFO 'v_field = %, v_payee_name = %, v_comp_name = %, v_join = %, v_join_tbl = %', v_field, v_payee_name, v_comp_name, v_join, v_join_tbl;
-------------------------------------------------------------------------------------------------------------
-- Don't create the report file if payee_id: 1138702,1202202,1231502,1232802,
--                                           2532702,200690
-- select sub_payee_id from irs.super_payee_group
-------------------------------------------------------------------------------------------------------------
cmd := 'SELECT 1 FROM irs.super_payee_group 
        WHERE sub_account_id IN ('||v_account_id||')';
RAISE INFO 'cmd = %', cmd;
EXECUTE cmd into v_count;

IF v_count != 1 OR (v_period_quarter  < '2014Q4') OR arg_override_constraint THEN

cmd := 'SELECT format_desc, negative_allowed, header_allowed, file_delimiter, file_extension
        FROM licensing.rollup_format
        WHERE format_type = '||v_format_type||'';
RAISE INFO 'cmd = %', cmd;
EXECUTE cmd INTO v_publisher, v_negative_allowed, v_header_allowed, v_file_delimiter, v_file_extension;

IF v_header_allowed THEN 
   v_header := 'HEADER';
ELSE
   v_header := '';
END IF;

-------------------------------------------------------------------------------------------------------------
IF NOT v_check_number_flg THEN
   IF coalesce(arg_check_number, v_check_number) IS NOT NULL THEN
      v_table_name := 'stmnt_'||v_payee_id||'_'||regexp_replace(coalesce(arg_check_number, v_check_number),'-','_','g')||'_'||v_period_quarter;
      v_report_table_name := 'v_report_'||v_payee_id||'_'||regexp_replace(coalesce(arg_check_number, v_check_number),'-','_','g');
   ELSE
      v_table_name := 'stmnt_'||v_payee_id||'_Recouped_'||v_period_quarter||'_'||arg_royalty_statement_id;
      v_report_table_name := 'v_report_recouped_'||v_period_quarter||'_'||arg_royalty_statement_id;
   END IF;
ELSE
   v_table_name := 'stmnt_'||v_admin_id||'_'||regexp_replace(coalesce(arg_check_number, v_check_number),'-','_','g')||'_'||v_period_quarter;
   v_report_table_name := 'v_report_'||v_admin_id||'_'||regexp_replace(coalesce(arg_check_number, v_check_number),'-','_','g');
END IF;

v_client_name := lower(replace(v_client_name, '.', ''));
v_filepath := '/mnt/itops-prod/workproduct/clients/'||v_client_name||'/accounting/rollup/'||v_period_quarter;
v_file := regexp_replace(v_table_name, 'stmnt_', '')||v_file_extension;
v_table_name := lower(v_table_name);
raise info 'v_table_name = %', v_table_name;
-------------------------------------------------------------------------------------------------------------

-- Creating a general report table
cmd := 'CREATE TEMP TABLE '||v_report_table_name||' (
           payee_id bigint,
           service_name character varying,
           check_number character varying,  
           isrc character varying,
           member_reference integer,
           territory character varying,
           client_name character varying,
           upc_ean character varying,
           pip_code character varying,
           sales_period_start date,
           sales_period_end date,
           offering_id integer,
           amount numeric,
           recoup_amount numeric,
           source_song_id integer,
           song_title character varying,
           composers character varying,
           publisher_share numeric(5,4),
           play_count bigint,
           weighted_play_count numeric,
           publisher_name character varying,
           per_play_rate numeric,
           period_month date,
           product_title character varying,
           artist character varying,
           publisher_song_code character varying, 
           sales_type character varying,
           client_catalog_id character varying,
           duration character varying,
           tier_name character varying
           '||v_field||'
           ) ON COMMIT DROP';
RAISE INFO 'cmd = %', cmd;
EXECUTE cmd;

-- Creating report_adjustment_detail table
cmd := 'CREATE TEMP TABLE report_adj_detail (
            report_summary_id bigint,
            license_row_id bigint,
            source_song_id bigint,
            source_song_title character varying,
            composers character varying,
            publisher_share numeric(5,4),
            play_count integer,
            weighted_count numeric,
            publisher_name character varying,
            effective_rate numeric,
            period_month date,
            track_album character varying,
            track_artist character varying,
            play_type_id integer,
            sub_tier_id integer,
            new_royalties numeric(12,5)
           ) ON COMMIT DROP';
RAISE INFO 'cmd = %', cmd;
EXECUTE cmd;
--||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cmd := 'SELECT COUNT(*)
        FROM licensing.rollup_report_properties
        WHERE client_id = '||arg_client_id||'';
RAISE INFO 'Getting column count for client %', v_client_name;
EXECUTE cmd INTO v_column_count;
RAISE INFO 'There are % columns required for %', v_column_count, v_client_name;

cmd := 'SELECT source_name, source_name2
        FROM licensing.rollup_report_properties
        WHERE client_id = '||arg_client_id||'
        ORDER BY column_order';
RAISE INFO 'Getting all columns required for client %', v_client_name;

v_str := '';
c_str := '';
ctr := 1;
FOR v_rec IN EXECUTE cmd
LOOP    
    IF ctr = v_column_count THEN
       v_str := v_str||v_rec.source_name;
       c_str := c_str||v_rec.source_name2;
    ELSE
       v_str := v_str||v_rec.source_name||', ';
       c_str := c_str||v_rec.source_name2||', ';
    END IF;
    ctr := ctr + 1;
END LOOP;

cmd := 'SELECT COUNT(*)
        FROM licensing.rollup_report_properties
        WHERE client_id = '||arg_client_id||'
          AND group_by IS TRUE';
RAISE INFO 'Getting column count for publisher %', v_publisher;
EXECUTE cmd INTO v_column_count;  

cmd := 'SELECT source_name
        FROM licensing.rollup_report_properties
        WHERE client_id = '||arg_client_id||'
          AND group_by IS TRUE
        ORDER BY column_order';
RAISE INFO 'Getting all columns that should be populated AND be present in GROUP BY statement%', cmd;

v_str1 := '';
ctr := 1;
FOR v_rec4 IN EXECUTE cmd
LOOP    
    IF ctr = v_column_count THEN
       v_str1 := v_str1||v_rec4.source_name;
    ELSE
       v_str1 := v_str1||v_rec4.source_name||', ';
    END IF;
    ctr := ctr + 1;
END LOOP;

-- Creating report table
cmd := 'SELECT COUNT(*)
        FROM licensing.rollup_properties
        WHERE format_type = '||v_format_type||'
          AND rollup_type = ''V''';
RAISE INFO 'Getting column count for publisher %', v_publisher;
EXECUTE cmd INTO v_column_count;
raise info 'There are % columns required for publisher %', v_column_count, v_publisher;

cmd := 'SELECT column_name, data_type, column_header
        FROM licensing.rollup_properties
        WHERE format_type = '||v_format_type||'
          AND rollup_type = ''V''
        ORDER BY column_order';
RAISE INFO 'Getting all columns required for client %', arg_client_id;

v_col := '';
v_col3 := '';
ctr := 1;

FOR v_rec1 IN EXECUTE cmd
LOOP    
    IF ctr = v_column_count THEN
       v_col := v_col||v_rec1.column_name||'  '||v_rec1.data_type;
       v_col3 := v_col3||v_rec1.column_header;
    ELSE
       v_col := v_col||v_rec1.column_name||'  '||v_rec1.data_type||', ';
       v_col3 := v_col3||v_rec1.column_header||', ';
    END IF;
    ctr := ctr + 1;
END LOOP;

RAISE INFO 'column_headers = %', v_col3;

-- Check if the voluntary_rollup_report table is existing, if not, create the table
IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = v_schema
                 AND table_name = v_table_name) THEN
   RAISE INFO 'Table %.% already exists...',v_schema , v_table_name;
   cmd := 'TRUNCATE '||v_schema||'.'||v_table_name||'';
   EXECUTE cmd;
ELSE  
-- create voluntary rollup report table
   cmd := 'CREATE TABLE '||v_schema||'.'||v_table_name||' ('||v_col||')';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;
END IF;

--Populating the voluntary_rollup_report table
cmd := 'SELECT COUNT(*)
        FROM licensing.rollup_properties
        WHERE format_type = '||v_format_type||'
          AND rollup_type = ''V''
          AND provided IS TRUE
          AND group_by IS TRUE';
RAISE INFO 'Getting column count for publisher %', v_publisher;
EXECUTE cmd INTO v_column_count;  

cmd := 'SELECT column_name, source_name
        FROM licensing.rollup_properties
        WHERE format_type = '||v_format_type||'
          AND rollup_type = ''V''
          AND provided IS TRUE
          AND group_by IS TRUE
        ORDER BY column_order';
RAISE INFO 'Getting all columns that should be populated AND be present in GROUP BY statement%', cmd;

v_col := '';
v_src := '';
ctr := 1;
FOR v_rec2 IN EXECUTE cmd
LOOP    
    IF ctr = v_column_count THEN
       v_col := v_col||v_rec2.column_name;
       v_src := v_src||v_rec2.source_name;
    ELSE
       v_col := v_col||v_rec2.column_name||', ';
       v_src := v_src||v_rec2.source_name||', ';
    END IF;
    ctr := ctr + 1;
END LOOP;

cmd := 'SELECT COUNT(*)
        FROM licensing.rollup_properties
        WHERE format_type = '||v_format_type||'
          AND rollup_type = ''V''
          AND provided IS TRUE
          AND group_by IS FALSE';
EXECUTE cmd INTO v_column_count;

cmd := 'select column_name, source_name
        FROM licensing.rollup_properties
        WHERE format_type = '||v_format_type||'
          AND rollup_type = ''V''
          AND provided IS TRUE
          AND group_by IS FALSE
        ORDER BY column_order';
raise info 'Getting all columns that should not be used in GROUP BY statement %', cmd;

v_col2 := '';
v_src2 := '';
FOR v_rec3 IN EXECUTE cmd
LOOP    
       v_col2 := v_col2||', '||v_rec3.column_name;
       v_src2 := v_src2||', '||'sum('||v_rec3.source_name||')';
END LOOP;

--||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

IF NOT v_check_number_flg AND arg_royalty_statement_id IS NOT NULL THEN
   cmd := 'SELECT remitted_statement_id
           FROM '||v_schema||'.royalty_statement
           WHERE royalty_statement_id = '||arg_royalty_statement_id||'
           AND client_id = '||arg_client_id||'';
   RAISE INFO '%', cmd;        
   EXECUTE cmd INTO v_remitted_stmnt;      
   RAISE INFO 'v_remitted_stmnt = %', v_remitted_stmnt;

   v_cmd := 'SELECT report_summary_id
             FROM '||v_schema||'.royalty_statement
             WHERE remitted_statement_id = '||COALESCE(v_remitted_stmnt,0)||'
             GROUP BY report_summary_id';

   cmd := 'SELECT DISTINCT royalty_statement_id, account_id, admin_id, co_id, check_number, period_quarter, check_amount, period_accountable, period_recoupment, payee_id
           FROM '||v_schema||'.royalty_statement
           WHERE remitted_statement_id = '||COALESCE(v_remitted_stmnt,0);
ELSE -- v_check_number_flg 
   cmd := 'SELECT DISTINCT royalty_statement_id, account_id, admin_id, co_id, check_number, period_quarter, check_amount, period_remittance, payee_id
           FROM '||v_schema||'.royalty_statement
           WHERE check_number = '||quote_literal(COALESCE(arg_check_number, v_check_number));

   v_cmd := 'SELECT report_summary_id
             FROM '||v_schema||'.royalty_statement
             WHERE check_number = '||quote_literal(COALESCE(arg_check_number, v_check_number))||'
             GROUP BY report_summary_id';
END IF;
RAISE INFO '%', cmd;

FOR v_rec8 IN EXECUTE v_cmd 
LOOP
    v_cmd := 'INSERT INTO report_adj_detail
              SELECT report_summary_id, license_row_id, source_song_id, source_song_title, composer_composite, new_publisher_share, play_count,
                     new_weighted_count, publisher_name, new_effective_rate_amount, 
                     (period_yr::varchar||CASE WHEN length((period_qtr*3-2)::varchar) = 1 THEN ''0''||(period_qtr*3-2)::varchar ELSE (period_qtr*3-2)::varchar END||''01'')::date AS period_month,
                     track_album, track_artist, play_type_id , CASE WHEN (play_type_id IN (10,20) AND '||arg_client_id||' = 5818802) THEN new_effective_rate_type_id ELSE NULL END, new_royalty_amount
              FROM licensing.report_adjustment_detail('||arg_client_id||', '||v_rec8.report_summary_id||')';
    RAISE INFO 'v_cmd = %', v_cmd;
    EXECUTE v_cmd;
END LOOP;

FOR v_rec6 IN EXECUTE cmd
LOOP  
-- Determining the start and end dates
v_year := substring(v_rec6.period_quarter, '[0-9]{4}');
v_quarter := substring(v_rec6.period_quarter from 6 for 1);
IF v_quarter = '1' THEN
   v_start_date := v_year||'-01-01';
   v_end_date := v_year||'-03-31';
ELSIF v_quarter = '2' THEN
   v_start_date := v_year||'-04-01';
   v_end_date := v_year||'-6-30';
ELSIF v_quarter = '3' THEN
   v_start_date := v_year||'-07-01';
   v_end_date := v_year||'-9-30';
ELSIF v_quarter = '4' THEN
   v_start_date := v_year||'-10-01';
   v_end_date := v_year||'-12-31';
ELSE
   RAISE EXCEPTION 'invalid quarter period';
END If;

c_cmd := 'SELECT report_summary_id
          FROM '||v_schema||'.royalty_statement
          WHERE royalty_statement_id = '||v_rec6.royalty_statement_id;
RAISE INFO '%', c_cmd;
EXECUTE c_cmd INTO v_report_summary_id;
   
c_cmd := 'SELECT count(*)
          FROM '||v_schema||'.report_detail_'||v_rec6.period_quarter||'
          WHERE report_summary_id = '||v_report_summary_id;
RAISE INFO '%', c_cmd;
EXECUTE c_cmd INTO v_rep_sum_count;
RAISE INFO 'v_rep_sum_count = %', v_rep_sum_count;

c_cmd := 'SELECT count(*)
          FROM report_adj_detail
          WHERE report_summary_id = '||v_report_summary_id; 
RAISE INFO '%', c_cmd;
EXECUTE c_cmd INTO v_rep_adj_count;
RAISE INFO 'v_rep_adj_count = %', v_rep_adj_count;
   
IF NOT v_check_number_flg AND arg_royalty_statement_id IS NOT NULL THEN
  IF v_rep_sum_count > 0 THEN
    IF v_rec6.royalty_statement_id = v_remitted_stmnt THEN
       cmd := 'INSERT INTO '||v_report_table_name||'
               WITH service_table AS (SELECT * FROM licensing.service_name WHERE client_id = '||arg_client_id||')
               SELECT '||v_rec6.payee_id||', service_table.service_name, coalesce(st.check_number, ''RECOUPED''), lp.client_data_8,  
                      '||v_account_id||',  ''USA'', '''||v_client_name||''',
                      lp.client_data_6, NULL, '''||v_start_date||''', '''||v_end_date||''',
                      service_table.noi_offering_id, rd.royalties, coalesce(rd.royalties_recoup, 0), '||v_str||v_comp_name||'
               FROM '||v_schema||'.royalty_statement st
               JOIN '||v_schema||'.royalty_statement st2 ON st.royalty_statement_id = st2.remitted_statement_id
               JOIN '||v_schema||'.report_detail_'||v_rec6.period_quarter||' rd ON st2.report_summary_id = rd.report_summary_id
               --LEFT JOIN report_adj_detail rad ON st2.report_summary_id = rad.report_summary_id
               JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = rd.license_row_id
               '||v_join||'
               LEFT JOIN service_table on coalesce(service_table.play_type_id, 0) = coalesce(rd.play_type_id, 0)
               '||v_join_tbl||'
               WHERE st.royalty_statement_id = '||v_rec6.royalty_statement_id||'';
    ELSE
       cmd := 'INSERT INTO '||v_report_table_name||'
               WITH service_table AS (SELECT * FROM licensing.service_name WHERE client_id = '||arg_client_id||')
               SELECT '||v_rec6.payee_id||', service_table.service_name, coalesce(st.check_number, ''RECOUPED''), lp.client_data_8,  
                      '||v_account_id||',  ''USA'', '''||v_client_name||''',
                      lp.client_data_6, NULL, '''||v_start_date||''', '''||v_end_date||''',
                      service_table.noi_offering_id, rd.royalties, coalesce(rd.royalties_recoup, 0), '||v_str||v_comp_name||'
               FROM '||v_schema||'.royalty_statement st
               JOIN '||v_schema||'.report_detail_'||v_rec6.period_quarter||' rd ON st.report_summary_id = rd.report_summary_id
               --LEFT JOIN report_adj_detail rad ON st.report_summary_id = rad.report_summary_id
               JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = rd.license_row_id
               '||v_join||'
               LEFT JOIN service_table on coalesce(service_table.play_type_id, 0) = coalesce(rd.play_type_id, 0)
               '||v_join_tbl||'
               WHERE st.royalty_statement_id = '||v_rec6.royalty_statement_id||'';
    END IF;
  END IF;
  RAISE INFO '%', cmd;
  EXECUTE cmd;

  IF v_rep_adj_count > 0 THEN
     cmd := 'INSERT INTO '||v_report_table_name||'
             WITH service_table AS (SELECT * FROM licensing.service_name WHERE client_id = '||arg_client_id||')
             SELECT '||v_rec6.payee_id||', service_table.service_name, coalesce(st.check_number, ''RECOUPED''), lp.client_data_8,  
                    '||v_account_id||',  ''USA'', '''||v_client_name||''',
                    lp.client_data_6, NULL, '''||v_start_date||''', '''||v_end_date||''',
                    service_table.noi_offering_id, rad.new_royalties, 0, '||c_str||v_comp_name||'
             FROM '||v_schema||'.royalty_statement st
             --JOIN '||v_schema||'.report_detail_'||v_rec6.period_quarter||' rd ON st.report_summary_id = rd.report_summary_id
             JOIN report_adj_detail rad ON st.report_summary_id = rad.report_summary_id
             JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = rad.license_row_id
             '||v_join||'
             LEFT JOIN service_table on coalesce(service_table.play_type_id, 0) = coalesce(rad.play_type_id, 0)
             --'||v_join_tbl||'
             WHERE st.royalty_statement_id = '||v_rec6.royalty_statement_id||'';
     RAISE INFO '%', cmd;
     EXECUTE cmd;
  END IF;
  
  v_statement_amount := v_rec6.period_accountable - v_rec6.period_recoupment;

  cmd := 'SELECT SUM(amount - recoup_amount) AS rollup_amount
          FROM '||v_report_table_name; 

ELSE --v_check_number_flg 
  IF v_rep_sum_count > 0 THEN
     cmd := 'INSERT INTO '||v_report_table_name||'
             WITH service_table AS (SELECT * FROM licensing.service_name WHERE client_id = '||arg_client_id||')
             SELECT '||v_rec6.payee_id||', service_table.service_name, coalesce(st.check_number, ''RECOUPED''), lp.client_data_8,  
                    '||v_rec6.account_id||',  ''USA'', '''||v_client_name||''',
                    lp.client_data_6, NULL, '''||v_start_date||''', '''||v_end_date||''',
                 service_table.noi_offering_id, rd.royalties_remit, coalesce(rd.royalties_recoup, 0), '||v_str||v_comp_name||'
             FROM '||v_schema||'.royalty_statement st 
             JOIN '||v_schema||'.report_detail_'||v_rec6.period_quarter||' rd ON st.report_summary_id = rd.report_summary_id
             --LEFT JOIN report_adj_detail rad ON st.report_summary_id = rad.report_summary_id
             JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = rd.license_row_id
             '||v_join||'
             LEFT JOIN service_table on coalesce(service_table.play_type_id, 0) = coalesce(rd.play_type_id, 0)
             '||v_join_tbl||'
             WHERE st.royalty_statement_id = '||v_rec6.royalty_statement_id||'';
     RAISE INFO '%', cmd;
     EXECUTE cmd;
  END IF;

  IF v_rep_adj_count > 0 THEN
     cmd := 'INSERT INTO '||v_report_table_name||'
             WITH service_table AS (SELECT * FROM licensing.service_name WHERE client_id = '||arg_client_id||')
             SELECT '||v_rec6.payee_id||', service_table.service_name, coalesce(st.check_number, ''RECOUPED''), lp.client_data_8,  
                    '||v_account_id||',  ''USA'', '''||v_client_name||''',
                    lp.client_data_6, NULL, '''||v_start_date||''', '''||v_end_date||''',
                    service_table.noi_offering_id, rad.new_royalties, 0, '||c_str||v_comp_name||'
             FROM '||v_schema||'.royalty_statement st
             --JOIN '||v_schema||'.report_detail_'||v_rec6.period_quarter||' rd ON st.report_summary_id = rd.report_summary_id
             JOIN report_adj_detail rad ON st.report_summary_id = rad.report_summary_id
             JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = rad.license_row_id
             '||v_join||'
             LEFT JOIN service_table on coalesce(service_table.play_type_id, 0) = coalesce(rad.play_type_id, 0)
             --'||v_join_tbl||'
             WHERE st.royalty_statement_id = '||v_rec6.royalty_statement_id||'';
     RAISE INFO '%', cmd;
     EXECUTE cmd;
  END IF;
  
   v_statement_amount := v_rec6.period_remittance;

   cmd := 'SELECT SUM(amount) AS rollup_amount
           FROM '||v_report_table_name; 

END IF;

RAISE INFO '%', cmd;
EXECUTE cmd INTO v_rollup_amount;
RAISE INFO 'rollup_amount = %', v_rollup_amount;
RAISE INFO 'statement_amount = %', v_statement_amount;

IF v_rollup_amount NOT BETWEEN ((v_statement_amount) - 0.05) AND ((v_statement_amount) + 0.05) THEN
   v_error_flg := true;
   v_e_msg := NOW() || ', rollup amount not equal to statement amount: statement_id: ' ||v_rec6.royalty_statement_id||': rollup = '||v_rollup_amount||', statement = '||v_statement_amount;
   RAISE INFO 'error_message = %', v_e_msg;
   RAISE NOTICE '%', v_e_msg;
ELSE
   v_error_flg := false;
   v_e_msg := '';
END IF;

-- Updating the general report table with pip_code values 
cmd := 'UPDATE '||v_report_table_name||' a
        SET pip_code = foo.pip_code1
  FROM
          (SELECT distinct a.pip_code, a.composers, a.title, b.pip_code as pip_code1, b.source_song_id
           FROM umpg.pip_code_catalog a
           RIGHT JOIN umpg.source_song_to_pip_pivot b on a.pip_code = b.pip_code
           LEFT JOIN umpg.pip_code c on c.pip_code = b.pip_code
           WHERE active ) foo
        WHERE a.source_song_id = foo.source_song_id';
RAISE INFO '%', cmd;
EXECUTE cmd;

-- Updating publisher_song_code
cmd := 'UPDATE '||v_report_table_name||'
        SET publisher_song_code = licensing.retrieve_publisher_song_code(source_song_id, payee_id)
        WHERE publisher_song_code IS NULL';
RAISE INFO '%', cmd;
EXECUTE cmd;

-- Inserting into output report 
cmd :='INSERT INTO '||v_schema||'.'||v_table_name||'('||v_col||v_col2||')
       SELECT '||v_src||v_src2||'
       FROM '||v_report_table_name||'
       GROUP BY '||v_src||'';
RAISE INFO 'cmd = %', cmd;
EXECUTE cmd;

v_comp_name := '';
-- Inserting negative values
IF v_negative_allowed IS TRUE THEN
   cmd := 'TRUNCATE '||v_report_table_name||'';
   EXECUTE cmd;
   IF NOT v_check_number_flg THEN
      IF v_rec6.royalty_statement_id = v_remitted_stmnt THEN
         cmd := 'INSERT INTO '||v_report_table_name||'
                 WITH service_table AS (SELECT * FROM licensing.service_name WHERE client_id = '||arg_client_id||')
		 SELECT '||v_rec6.payee_id||', service_table.service_name, coalesce(st.check_number, ''RECOUPED''), lp.client_data_8,
			 '||v_account_id||',  ''USA'', '''||v_client_name||''',
			 lp.client_data_6, NULL, '''||v_start_date||''', '''||v_end_date||''',
			 service_table.noi_offering_id, rd.royalties*(-1), rd.royalties_recoup*(-1), '||v_str||v_comp_name||'
		 FROM '||v_schema||'.royalty_statement st
		 JOIN '||v_schema||'.royalty_statement st2 ON st.royalty_statement_id = st2.remitted_statement_id
		 JOIN '||v_schema||'.report_detail_'||v_rec6.period_quarter||' rd ON st2.report_summary_id = rd.report_summary_id
		 --LEFT JOIN report_adj_detail rad ON st2.report_summary_id = rad.report_summary_id
		 JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = coalesce(rd.license_row_id, rad.license_row_id)
		 '||v_join||'
		 LEFT JOIN service_table on coalesce(service_table.play_type_id, 0) = coalesce(rd.play_type_id, 0)
		 '||v_join_tbl||'
		 WHERE st.royalty_statement_id = '||v_rec6.royalty_statement_id||'
		 AND st.total_accountable = st.total_recoupment';
      ELSE
          cmd := 'INSERT INTO '||v_report_table_name||'
		  WITH service_table AS (SELECT * FROM licensing.service_name WHERE client_id = '||arg_client_id||')
		  SELECT '||v_rec6.payee_id||', service_table.service_name, coalesce(st.check_number, ''RECOUPED''), lp.client_data_8,
			 '||v_account_id||',  ''USA'', '''||v_client_name||''',
			 lp.client_data_6, NULL, '''||v_start_date||''', '''||v_end_date||''',
			 service_table.noi_offering_id, rd.royalties*(-1), rd.royalties_recoup*(-1), '||v_str||v_comp_name||'
		  FROM '||v_schema||'.royalty_statement st
		  JOIN '||v_schema||'.report_detail_'||v_rec6.period_quarter||' rd ON st.report_summary_id = rd.report_summary_id
		  --LEFT JOIN report_adj_detail rad ON st.report_summary_id = rad.report_summary_id
		  JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = coalesce(rd.license_row_id, rad.license_row_id)
		  '||v_join||'
		  LEFT JOIN service_table on coalesce(service_table.play_type_id, 0) = coalesce(rd.play_type_id, 0)
		  '||v_join_tbl||'
		  WHERE st.royalty_statement_id = '||v_rec6.royalty_statement_id||'
		  AND st.total_accountable = st.total_recoupment';
      END IF;  
  ELSE --v_check_number_flg
    cmd := 'INSERT INTO '||v_report_table_name||'
            WITH service_table AS (SELECT * FROM licensing.service_name WHERE client_id = '||arg_client_id||')
            SELECT '||v_rec6.payee_id||', service_table.service_name, coalesce(st.check_number, ''RECOUPED''), lp.client_data_8,
                '||v_account_id||',  ''USA'', '''||v_client_name||''',
                 lp.client_data_6, NULL, '''||v_start_date||''', '''||v_end_date||''',
                 service_table.noi_offering_id, rd.royalties_remit*(-1), rd.royalties_recoup*(-1), '||v_str||v_comp_name||'
            FROM '||v_schema||'.royalty_statement st
            JOIN '||v_schema||'.report_detail_'||v_rec6.period_quarter||' rd ON st.report_summary_id = rd.report_summary_id
            --LEFT JOIN report_adj_detail rad ON st.report_summary_id = rad.report_summary_id
            JOIN licensing.license_part_'||arg_client_id||' lp ON lp.row_id = coalesce(rd.license_row_id, rad.license_row_id)
            '||v_join||'
            LEFT JOIN service_table on coalesce(service_table.play_type_id, 0) = coalesce(rd.play_type_id, 0)
            '||v_join_tbl||'
            WHERE st.royalty_statement_id = '||v_rec6.royalty_statement_id||'
            AND st.total_accountable = st.total_recoupment';
  END IF;      
  RAISE INFO '%', cmd;
  EXECUTE cmd;

-- Updating the general report table with pip_code values 
cmd := 'UPDATE '||v_report_table_name||' a
        SET pip_code = foo.pip_code1
  FROM
          (SELECT distinct a.pip_code, a.composers, a.title, b.pip_code as pip_code1, b.source_song_id
           FROM umpg.pip_code_catalog a
           RIGHT JOIN umpg.source_song_to_pip_pivot b on a.pip_code = b.pip_code
           LEFT JOIN umpg.pip_code c on c.pip_code = b.pip_code
           WHERE active ) foo
        WHERE a.source_song_id = foo.source_song_id';
RAISE INFO '%', cmd;
EXECUTE cmd;

-- Updating publisher_song_code
cmd := 'UPDATE '||v_report_table_name||'
        SET publisher_song_code = licensing.retrieve_publisher_song_code(source_song_id, payee_id)
        WHERE publisher_song_code IS NULL';
RAISE INFO '%', cmd;
EXECUTE cmd;

cmd :='INSERT INTO '||v_schema||'.'||v_table_name||'('||v_col||v_col2||')
       SELECT '||v_src||v_src2||'
       FROM '||v_report_table_name||'
       GROUP BY '||v_src||'';
RAISE INFO 'cmd = %', cmd;
EXECUTE cmd;
END IF;

cmd := 'TRUNCATE '||v_report_table_name||'';
EXECUTE cmd;

-------------------------------------------------------------------------------------------------------------

--Updating rollup log table
IF v_error_flg IS TRUE THEN
   cmd := 'SELECT COUNT(*)
           FROM licensing.rollup_create_log
           WHERE statement_id = '||v_rec6.royalty_statement_id||'
           AND client_id = '||arg_client_id||'
           AND file_path IS NULL 
           AND error IS NULL
           AND COALESCE(invalid, false) = false';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_column_count;
   RAISE INFO 'v_column_count = %', v_column_count;
   
   If v_column_count > 0 THEN
      cmd := 'UPDATE licensing.rollup_create_log
              SET admin_id = '||v_rec6.admin_id||',
                  payee_id = '||v_rec6.payee_id||',
                  co_id = '||v_rec6.co_id||',
                  error = '''||v_e_msg||''',
                  invalid = ''f'',
                  check_amount = '||v_rec6.check_amount||'
              WHERE statement_id = '||v_rec6.royalty_statement_id||'
              AND client_id = '||arg_client_id||'
              AND COALESCE(invalid, false) = false';
      RAISE INFO '%', cmd;
      EXECUTE cmd;
   ELSE 
      cmd := 'INSERT INTO licensing.rollup_create_log (statement_id, account_id, admin_id,
                 payee_id, co_id, period_quarter, format_type, error, create_dt, invalid,
                 check_number, check_amount, client_id, create_by)
              VALUES ('||v_rec6.royalty_statement_id||', '||v_rec6.account_id||', '||v_rec6.admin_id||',
                      '||v_rec6.payee_id||', '||v_rec6.co_id||', '''||v_rec6.period_quarter||''', '||v_format_type||',
                      '''||v_e_msg||''', NOW()::timestamp, ''f'', '''||v_rec6.check_number||''',
                      '||v_rec6.check_amount||', '||arg_client_id||', '||v_user||')';
      RAISE INFO '%', cmd;
      EXECUTE cmd;
    END IF;
ELSE
IF v_rec6.check_number IS NOT NULL THEN

   cmd := 'SELECT COUNT(*)
           FROM licensing.rollup_create_log
           WHERE statement_id = '||v_rec6.royalty_statement_id||'
           AND client_id = '||arg_client_id||'
           AND file_path IS NULL 
           AND error IS NULL
           AND COALESCE(invalid, false) = false';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_column_count;
   RAISE INFO 'v_column_count = %', v_column_count;
   
   If v_column_count > 0 THEN
      cmd := 'UPDATE licensing.rollup_create_log
              SET admin_id = '||v_rec6.admin_id||',
                  payee_id = '||v_rec6.payee_id||',
                  co_id = '||v_rec6.co_id||',
                  file_path = '''||v_filepath||'/'||v_file||E''',
                  invalid = ''f'',
                  check_amount = '||v_rec6.check_amount||'
              WHERE statement_id = '||v_rec6.royalty_statement_id||'
              AND client_id = '||arg_client_id||'
              AND COALESCE(invalid, false) = false';
      RAISE INFO '%', cmd;
      EXECUTE cmd;
   ELSE
      cmd := 'INSERT INTO licensing.rollup_create_log (statement_id, account_id, admin_id,
                 payee_id, co_id, period_quarter, format_type, file_path, create_dt, invalid,
                 check_number, check_amount, client_id, create_by)
              VALUES ('||v_rec6.royalty_statement_id||', '||v_rec6.account_id||', '||v_rec6.admin_id||',
                      '||v_rec6.payee_id||', '||v_rec6.co_id||', '''||v_rec6.period_quarter||''', '||v_format_type||',
                      '''||v_filepath||'/'||v_file||E''', NOW()::timestamp, ''f'', '''||v_rec6.check_number||''',
                      '||v_rec6.check_amount||', '||arg_client_id||', '||v_user||')';
      RAISE INFO '%', cmd;
      EXECUTE cmd;
    END IF;

ELSE

   cmd := 'SELECT COUNT(*)
           FROM licensing.rollup_create_log
           WHERE statement_id = '||v_rec6.royalty_statement_id||'
           AND client_id = '||arg_client_id||'
           AND file_path IS NULL 
           AND error IS NULL
           AND COALESCE(invalid, false) = false';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd INTO v_column_count;
   RAISE INFO 'v_column_count = %', v_column_count;
   
   If v_column_count > 0 THEN
      cmd := 'UPDATE licensing.rollup_create_log
              SET admin_id = '||v_rec6.admin_id||',
                  payee_id = '||v_rec6.payee_id||',
                  co_id = '||v_rec6.co_id||',
                  file_path = '''||v_filepath||'/'||v_file||E''',
                  invalid = ''f'',
                  check_amount = null
              WHERE statement_id = '||v_rec6.royalty_statement_id||'
              AND client_id = '||arg_client_id||'
              AND COALESCE(invalid, false) = false';
      RAISE INFO '%', cmd;
      EXECUTE cmd;
   ELSE
      cmd := 'INSERT INTO licensing.rollup_create_log (statement_id, account_id, admin_id,
                 payee_id, co_id, period_quarter, format_type, file_path, create_dt, invalid,
                 check_number, check_amount, client_id, create_by)
              VALUES ('||v_rec6.royalty_statement_id||', '||v_rec6.account_id||', '||v_rec6.admin_id||',
                      '||v_rec6.payee_id||', '||v_rec6.co_id||', '''||v_rec6.period_quarter||''', '||v_format_type||',
                      '''||v_filepath||'/'||v_file||E''', NOW()::timestamp, ''f'', null,
                      '||v_rec6.check_amount||', '||arg_client_id||', '||v_user||')';
      RAISE INFO '%', cmd;
      EXECUTE cmd;
    END IF;      
END IF;

END IF;

END LOOP;

cmd := 'COPY ( SELECT '||v_col3||'
               FROM '||v_schema||'.'||v_table_name||' )
        TO '''||v_filepath||'/'||v_file||E''' 
        WITH CSV '||v_header||' DELIMITER AS '''||v_file_delimiter||E'''';
RAISE INFO 'cmd = %', cmd;
EXECUTE cmd;

   RETURN v_filepath||'/'||v_file;

ELSE
   RETURN 'Payee ID is excluded from being run in this function. If you would like to run it anyway,' ||
          ' please call function with arg_override_constraint set to TRUE.';
END IF;

END IF;


   RETURN v_filepath||'/'||v_file;
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
