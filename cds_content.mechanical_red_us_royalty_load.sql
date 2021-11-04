 -- Function: cds_content.mechanical_red_us_royalty_load(integer)
 
 -- DROP FUNCTION cds_content.mechanical_red_us_royalty_load(integer);
 
 CREATE OR REPLACE FUNCTION cds_content.mechanical_red_us_royalty_load(arg_format_id integer)
   RETURNS character varying AS
 $BODY$ 
/* *********************************************************************
     $DESCRIPTION = NEW MECHANICAL RED US, NEW ROYALTY RATES/ALGORITHMS

     --=== DIGITAL AUDIO LONGPLAY/DIGITAL AUDIO TRACK ===--
     If CONFIGURATION = "DIGITAL AUDIO LONGPLAY or DIGITAL AUDIO TRACK" THEN assume test is TRUE 
       Match PARENT PROD NO, then ISRC found in parent product PARENT PROD NO in sales report is G010001806559H which matches the content of field DISTRIBUTOR NUMBER 
       in MARIE for product ID 145; ISRC CAC608710010 in product ID 145 matches track ID 2277 
       Get timing of matched track Timing of track ID 2277 is 01:45 
       Get mech US rate according to timing of track in IP MANAGEMENT > MUSIC > TRACKS > track 2277 > Summary field > Current Mech Royalty US > 0.09500 USD 
       *** IMPORTANT NOTE: the formula for the content of these fields is not vetted , rate should actually be 0.09100 USD; I'll continue example with correct rate 
       ROYALTY = case when track timing > 5min then incrmental_rate * timing else 0.09100 end * 10 (number of downloads; DOWNLOAD QT) 0.91 USD 
       accrue right-holders royalties according to US share split of embedded song applied to ROYALTY share of Rene Dupere is 50% of ROYALTY in the US (from territory WORLD), so: 0.455 USD      

	   SPECIAL NOTE FOR LONGPLAY:
	   Because of the ISRC issue with DIGITAL AUDIO LONGPLAY the isrc match is skipped, so that the LONGPLAY is matched only by PPN(product).
	   And download_qt is applied to each track of the matched product.
	   
     --=== MASTERTONE/RINGBACKTONE INSTRUCTION ===--
     If CONFIGURATION = "MASTERTONE" or "RINGBACK TONE" THEN
	   Match PARENT PROD NO, then ISRC found in parent product
     If (DOWNLOAD AM * RATE) < FLOOR THEN ROYALTY = FLOOR
		ELSE ROYALTY = (DOWNLOAD AM * RATE) * SHARE / 100
	accrue right-holders royalties according to US share split of embedded song applied to ROYALTY
																																										
    IMPORTANT NOTES: 
      You have to match the product first and then the ISRC to tracks strictly within this product; do not try to match tracks by ISRC at large
      FLOOR can be found in the new ringtone rate pane (in this file, RINGTONE RATE SCREEN tab) and would be 0,05$ CAD. 
      FLOOR is specified in canadian dollars but it has to be converted each quarter in the US dollars equivalent because DOWNLOAD AM is in provided in US dollars by Red
      RATE can also be found in the new ringtone rate pane and would be 5
																																										
   *********************************************************************
*/
   --$1 = File format_id 
  
 DECLARE
  arg_format_id      ALIAS for $1;
  
  v_period                varchar;
  v_q                     varchar;
  v_reporting_period      varchar;
  v_abs_path              varchar;    
  v_path                  varchar;  
  v_file_name             varchar;
  v_file_exp              varchar;
  v_target_table          varchar;
  v_royalty_table         varchar;
  v_schema                varchar;
  v_royalty               varchar;
                          
  cmd                     text;
                          
  v_return                varchar;
  v_count                 integer;
  v_newtable              boolean;
  v_newroyaltytable       boolean;
                          
  v_rates                 varchar[][];
  v_dim                   int;
  v_us_rate               varchar;
  v_incremental_rate      varchar;
  v_ringtone_rate         varchar;
  v_ringtone_floor_rate   varchar;
  v_er_cad_usd            varchar;  
  
  rec                     record; 
BEGIN   
  v_newtable := false;
  v_newroyaltytable := false;
  v_schema := 'cds_content';
  v_target_table  := v_schema||'.'||'mech_prod_sales_us_';
  v_royalty_table := v_schema||'.'||'mech_red_royalty_';
 
  --FORMAT VALIDATION
  IF coalesce(arg_format_id,0) = 0 THEN
    raise exception 'Invalid function argument ';
  END IF;
  
  --=SOURCE FILE PROCESSING=-
  cmd := 'SELECT file_dir FROM cds_content.mech_royalty_types WHERE format_id = '||arg_format_id ||' AND invalid_date IS NULL ';
  EXECUTE cmd into v_path;  
  raise info 'Format_id = %, Abstract file path = %', arg_format_id, v_path;
  
  IF v_path IS NULL THEN
    raise exception 'Invalid file name % ', v_path;
  END IF; 
  v_abs_path  := substring(v_path from E'[\\w\\-\\/]+(?=/)');
  v_file_exp  := 'error_ppns.csv';
  v_file_name := substring(v_path from E'[\\w_\\-\\.\\s]+$');
  v_period    := substring(v_file_name from E'[[:digit:]]{4}[Q,q][0-9]+'); 
  IF v_period IS NULL THEN 
    raise exception 'Can not create target table name. Missing period'; 
  END IF;
  
  --GETTING REPORTING PERIOD NEEDED FOR THE EXCHANGE RATE
  v_q := right(v_period,1)||substr(v_period,5,1)||substr(v_period,3,2); 
  v_reporting_period := coalesce(reporting_period_id,0) from cds_content.reporting_period where reporting_period_name = v_q;
  
  v_target_table  := lower(v_target_table||v_period);
  v_royalty_table := lower(v_royalty_table||v_period);
  
  --CHECK WHETHER HAS BEEN ALREADY PROCESSED
  IF EXISTS( SELECT * FROM information_schema.tables WHERE table_schema||'.'||table_name = v_royalty_table ) THEN 
    cmd := 'SELECT count (1) from '||v_royalty_table||' where format_id = '||arg_format_id;
    EXECUTE cmd into v_count;
    IF( v_count > 0 )THEN
      raise exception 'The mechanical royalty data for period % and format_id % already exists.',v_period, arg_format_id;
    END IF;
  ELSE
    v_newroyaltytable := true;
  END IF;
  
  --CREATING MECH_PROD_SALES_US TABLE AND INGESTING THE SALES FILE
  EXECUTE 'DROP TABLE IF EXISTS '||v_target_table;
  cmd := 'CREATE TABLE '||v_target_table||' ( 
                                              PROVIDER VARCHAR,
                                              LABEL VARCHAR,
                                              ARIST VARCHAR,
                                              PARENT_TITLE VARCHAR,
                                              CONFIGURATION VARCHAR,
                                              RACK_ARTIST VARCHAR,
                                              TRACK_TITLE VARCHAR,
                                              ISRC VARCHAR,
                                              PROD_NO VARCHAR,
                                              PARENT_PROD_NO VARCHAR,
                                              COUNTRY VARCHAR,
                                              SALES_PERIOD VARCHAR,
                                              TOTAL_AM VARCHAR,
                                              DOWNLOAD_QT VARCHAR,
                                              DOWNLOAD_AM VARCHAR,
                                              STREAM_PREMIUM_QT VARCHAR,
                                              STREAM_PREMIUM_AM VARCHAR,
                                              STREAM_AD_SUPPORTED_QT VARCHAR,
                                              STREAM_AD_SUPPORTED_AM VARCHAR,
                                              LOCKER_QT VARCHAR,
                                              LOCKER_AM VARCHAR,
                                              OTHER_QT VARCHAR,
                                              OTHER_AM VARCHAR
                                              /*ALBUMS VARCHAR,
                                              CUEVRES VARCHAR,
                                              MASTERTONE VARCHAR,
                                              RINGBACK_TONE VARCHAR*/
                                            )';
  raise info 'Creating table for mechanical roayalty load process, period %', v_period;
  EXECUTE cmd;
  v_newtable := true;
  
  --INGESTING THE SALES FILE   
  cmd := 'COPY '||v_target_table||' FROM '''||v_path||E''' WITH CSV DELIMITER ''\t'' HEADER QUOTE ''"'' ';
  raise info 'INGESTING SALES FILE %', v_file_name;
  EXECUTE cmd;
  IF( v_newtable ) THEN
   
    cmd := 'ALTER TABLE '||v_target_table||' ADD COLUMN FORMAT_ID INTEGER DEFAULT 0';
    -- raise info '%',cmd;
    EXECUTE cmd;
	
  END IF;
  cmd := 'UPDATE '||v_target_table ||' 
             SET format_id = ' ||arg_format_id||' WHERE format_id = 0 ';
  raise info 'UPDATE format_id.';
  -- raise info 'cmd = %', cmd;
  EXECUTE cmd;
   
  raise info 'Cleaning Up values(comma) from columns download_qt and download_am.';
  EXECUTE 'UPDATE '||v_target_table ||' SET download_qt = COALESCE(download_qt,''0''), download_am = replace(coalesce(download_am,''0''),'','','''')'; 
  
  --GROUPING THE DATA SALES BY ONLY NEEDED COLUMNS 
  cmd := 'CREATE TEMP TABLE new_prod_sales_us ON COMMIT DROP AS
          SELECT parent_prod_no, 
                 configuration,
                 case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then ''digital audio''
                      when lower(configuration) ~* ''mastertone|ringback tone'' then ''tone''
                 end acc_type,
                 isrc,
                 max(country) country, 
                 max(format_id) format_id,
                 sum(download_qt::numeric) download_qt, 
                 sum(download_am::numeric) download_am
           FROM '||v_target_table||'
          WHERE country = ''US''
            AND (download_qt::numeric > 0 or download_am::numeric > 0)
          GROUP BY parent_prod_no, configuration, acc_type, isrc
          ORDER BY parent_prod_no';
  EXECUTE cmd;

  /*
  --=DATA GEARS UP FOR THE CALCULATIONS (OLD MECH RED)=-
  raise info 'Old Red US accounting (controlled and full rate per product) - Drives data partitioned per RH Society Affiliation';	
  --GENERATES TABLE PROD_LIST_SOCIETY WHERE RHs ARE AFFILIATED TO SOCIETIES (SODRAC, PD)
  cmd := 'SELECT * FROM cds_content.mechanical_rate_prod_calc(null, true, true)';
   -- -=PRO RATA CONTROLLED=-
       -- GENERATES TABLE PROD_LIST WHERE RHs ARE WITH NO SOCIETY AFFILIATION
       -- RHs = EITHER AFFILIATED OR UNDER CONTRACT OR CIRQUE COMPANY
  cmd := 'SELECT * FROM cds_content.mechanical_rate_prod_calc(null, true, false)';
  */ 
  
  --CREATING TEMP TABLE, WHICH CAPTURES THE MOVES OUT OF THE BUSINESS MODEL
  cmd:= 'CREATE TEMP TABLE tbl_error (
         ErrCode varchar,
         ErrMessage varchar,
         PPN varchar) on commit drop';
  EXECUTE cmd;
  --CHECKS FOR MULTIPLE PPNs REFERENCING ONE PRODUCT
  raise info 'Checking for multiple PPNs referencing One product.';  
  cmd = 'INSERT INTO tbl_error 
         SELECT distinct ''01'', ''Multiple PPNs referencing One product.'', tbl.parent_prod_no
           FROM (
                 WITH dn AS (select parent_prod_no from new_prod_sales_us group by parent_prod_no)
                 SELECT count(distinct product_id), dn.parent_prod_no
                   FROM cds_content.product_distributor_numbers p
                   JOIN dn on dn.parent_prod_no = p.distributor_number and p.invalid_date is null
                  GROUP by dn.parent_prod_no
                 HAVING count(distinct product_id) > 1
                ) tbl';
  EXECUTE cmd;
  --CHECKS FOR UNMATCHED PPNs
  raise info 'Checking for unmatched PPNS.';  
  cmd := 'INSERT INTO tbl_error 
          SELECT distinct ''02'', ''Found unmatched PPNs.'', tbl.parent_prod_no
            FROM (            
                  WITH dnum AS (select pdn.product_id, array_agg(distinct pdn.distributor_number) as distributornumbers
                                  from cds_content.product_distributor_numbers pdn
                                  join cds_content.products p on p.product_id = pdn.product_id AND p.invalid_date is NULL
                                 where pdn.invalid_date is NULL
                                 group by pdn.product_id 
                               ),
                       red AS (select parent_prod_no from new_prod_sales_us group by parent_prod_no) 
                  SELECT distinct red.parent_prod_no
                    FROM red
                    LEFT JOIN dnum on red.parent_prod_no = ANY(dnum.distributornumbers)
                   WHERE (dnum.product_id is null or trim(red.parent_prod_no) = '''')
                 ) tbl';
  EXECUTE cmd;
  --RAISES ERROR MESSAGES
  FOR rec IN (select distinct errcode, errmessage from tbl_error) LOOP
    raise info 'Unallowed: %', rec.ErrMessage; 
  END LOOP;
  --EXPORTS THE LIST OF PPN ERRORS
  IF EXISTS(SELECT 1 FROM tbl_error) THEN 
    cmd := 'COPY (select distinct * from tbl_error order by errcode) TO '''||v_abs_path||'/'||v_file_exp||E''' WITH CSV DELIMITER '','' HEADER';
	EXECUTE cmd;
    raise info 'See the exported Error file here % ', v_abs_path||'/'||v_file_exp;
	raise exception 'Found PPN errors, see the above messages.';
  END IF;
 
  -- RETRIEVING THE MECHANICAL RATES
  raise info 'Retrieving the mechanical rates and exchange rate.';  
  cmd := 'WITH tbl as (select array_agg(fo.rates) as rate_arr, array_length(array_agg(fo.rates),1) as dim
                         from (
                               select rate||''^''||coalesce(incremental_rate,0.00) as rates
                                 from cds_content.mechanical_rates
                                where invalid_date IS NULL 
                                  and service_type_id in (1,3,4)
                                  and territory_id in (210,34)
                                order by service_type_id
                              ) fo 
                      ),
               curr as (
                       select currency_to, exchange_rate as er_cad_usd, reportingperiodid 
                         from cds_content.exchange_rate
                        where currency_from = 1
                          and currency_to = 4 
                          and reportingperiodid = '||v_reporting_period||'
                          and begin_date is null
                          and end_date is null
             )              
         SELECT array[string_to_array(split_part(array_to_string(rate_arr,'',''),'','',1),''^'')]||
                      string_to_array(split_part(array_to_string(rate_arr,'',''),'','',2),''^'')||
                      string_to_array(split_part(array_to_string(rate_arr,'',''),'','',3),''^''), 
                dim, curr.er_cad_usd
           FROM tbl, curr';
  EXECUTE cmd INTO v_rates, v_dim, v_er_cad_usd; 
  IF (coalesce(v_dim,0) < 3) or (coalesce(v_er_cad_usd::numeric,0) = 0) THEN 
    raise exception 'The mech rates and exchange rate need to be setup!';
  END IF;
  v_incremental_rate := v_rates[1][2];
  v_us_rate := 'case when cds_content.duration_in_sec(coalesce(t.duration,''0:0:0''))::numeric / 60 > 5.00 then '
                     ||v_incremental_rate||' * (CEIL(cds_content.duration_in_sec(t.duration)::numeric/60.00)) 
                else '||v_rates[1][1]||' end';
  v_ringtone_rate := v_rates[2][1]; -- ringtone rate
  v_ringtone_floor_rate := v_rates[3][1]; 
  raise info 'v_rates = %', v_rates;
  v_ringtone_floor_rate := (v_ringtone_floor_rate::numeric * v_er_cad_usd::numeric)::varchar;  -- conversion from cad to usd

  raise info 'v_incremental_rate = %, v_us_rate = %, v_ringtone_rate = %, v_ringtone_floor_rate = %,  v_dim = %, v_er_cad_usd = %', v_incremental_rate, v_us_rate, v_ringtone_rate, v_ringtone_floor_rate, v_dim, v_er_cad_usd;
  -- ACCOUNTING TYPES / ALGORITHMS
  v_royalty := 'case when lower(red.acc_type) ~* ''digital audio'' then
                          round(((red.download_qt::numeric * '||v_us_rate||') * ts.mech_perf_sync::numeric ) / 100, 5) 
                     when lower(red.acc_type) ~* ''tone'' then
                          case when (red.download_am::numeric * '||v_ringtone_rate||') < '||v_ringtone_floor_rate||' then 
                                     round(('||v_ringtone_floor_rate||' * ts.mech_perf_sync::numeric) / 100, 5)
                          else round(((red.download_am::numeric * '||v_ringtone_rate||') * ts.mech_perf_sync::numeric) / 100, 5)
                          end 
                 end';  
				 
  -- MECHANICAL ROYALTY RESULT STRUCTURE 
  cmd := CASE WHEN v_newroyaltytable THEN 'CREATE TABLE '||v_royalty_table||' AS ' ELSE 'INSERT INTO '||v_royalty_table||' ' END;
  cmd := cmd ||
         'WITH distributer_numbers AS (
                        select pdn.product_id, array_agg(distinct pdn.distributor_number) as distributornumbers
                          from cds_content.product_distributor_numbers pdn
                         group by pdn.product_id
                        ),
               society_mech_territories AS (
                         select rs.rightsholderid, rs.table_index, rt.rights_type_id::varchar, 
                                rt.rights_type, s.society_id, s.society_name, t.territory_id, t.territory_name 
                           from cds_content.rightsholders_societies rs 
                           join cds_content.societies s ON rs.societyid = s.society_id AND s.invalid_date IS NULL 
                           join cds_content.territories t ON rs.territoryid = t.territory_id AND t.invalid_date IS NULL 
                           join cds_content.societies_rightstypes srt ON s.society_id = srt.societyid AND srt.invalid_date IS NULL 
                           join cds_content.rights_type rt ON srt.rightstypeid = rt.rights_type_id AND rt.invalid_date IS NULL 
                          where rs.invalid_date IS NULL
                            and rt.rights_type_id = 3 
                            and coalesce(rs.rightstypeid,3) = 3 
                          order by rightsholderid
                        ),
               territory_shares AS (
                         with shr AS (select ss1.songid, ss1.contractid, ss1.rightsholderid, ss1.territoryid, ss1.mech_perf_sync::numeric, 
                                             row_number() OVER(PARTITION BY ss1.songid, ss1.rightsholderid 
                                                                   ORDER BY case when ss1.territoryid = 210 then 1 
                                                                                 when ss1.territoryid = -2 then 2 
                                                                                 when ss1.territoryid = -1 then 3 
                                                                            end ) as rownum
                                        from cds_content.songs_shares ss1 
                                       where ss1.invalid_date IS NULL
                                         and ss1.territoryid in (210, -2, -1)
                                         and ss1.mech_perf_sync::numeric > 0.00
                                      )  
                         select songid, contractid, rightsholderid, mech_perf_sync::numeric, rownum, territoryid
                           from shr 
                          where shr.rownum = 1
                          group by rownum, songid, contractid, rightsholderid, mech_perf_sync::numeric, territoryid
                          order by songid, rightsholderid, rownum
                        )
          Select 
                 p.product_id as merchandiseid, p.product_title, p.catalog_number, p.artist, p.label, p.cirkipedia_hyperlink,
                 mt.cdnumber, mt.playorder,
                 t.track_id, t.track_title, ts.songid, ts.rightsholderid, t.duration, t.isrc, t.ownership, t.note, max(soc.society_id) as society_id,
                 0 ca_controlled, 0 us_controlled, 0 full_rate_ca, 0 full_rate_us, 0 cirque_controlled_ca, 0 cirque_controlled_us,
                 case when coalesce(rh.company_name,'''') = '''' then rh.first_name||'' ''||rh.last_name else rh.company_name end as rightsholder_name,
                 ts.contractid, 
                 red.country as territory_sold,
                 ts.mech_perf_sync, 
                 red.download_qt as quantity, '
                 ||v_us_rate ||' as rate_used, '
                 ||v_royalty ||' as final_rate,
                 ''USD''::varchar as currency, 
                 red.format_id, 
                 case when COALESCE(soc.society_id,0) = 27 AND soc.territory_id in (-1,405) then ''Y''
                      when ts.rightsholderid in (502, 681, 506, 523, 503, 524, 505, 1480, 803, 1685, 1712, 1686, 1713) then ''Y''
                 else ''N'' end as society,
                 red.configuration, 
                 red.download_am, '
                 ||v_ringtone_rate ||' as ringtone_rate
            From new_prod_sales_us red
            join distributer_numbers dnum on red.parent_prod_no = ANY(dnum.distributornumbers) 
            join cds_content.products p on p.product_id = dnum.product_id AND p.invalid_date is null
            join cds_content.merchandise_tracks mt on mt.merchandiseid = p.product_id and mt.invalid_date is null
            join cds_content.track t on t.track_id = mt.trackid AND t.invalid_date is null AND t.isrc = CASE WHEN red.configuration ~* ''DIGITAL AUDIO LONGPLAY'' THEN t.isrc ELSE red.isrc END
            join cds_content.relationships rs on rs.trackid = t.track_id AND rs.invalid_date is null
            join territory_shares ts on ts.songid = rs.songid
            left join cds_content.rightsholders rh on rh.rightsholder_id = ts.rightsholderid and rh.invalid_date is null
            left join society_mech_territories soc on soc.rightsholderid = ts.rightsholderid
           Where upper(red.country) = ''US''
           Group by 
                    p.product_id, p.product_title, p.catalog_number, p.artist, p.label, p.cirkipedia_hyperlink,
                    mt.cdnumber, mt.playorder,
                    t.track_id, t.track_title, ts.songid, ts.rightsholderid, t.duration, t.isrc, t.ownership, t.note,
                    case when coalesce(rh.company_name,'''') = '''' then rh.first_name||'' ''||rh.last_name else rh.company_name end,
                    ts.contractid, 
                    red.country,
                    ts.mech_perf_sync, 
                    red.download_qt, 
                    red.acc_type,
                    red.format_id,
                    case when COALESCE(soc.society_id,0) = 27 AND soc.territory_id in (-1,405) then ''Y''
                         when ts.rightsholderid in (502, 681, 506, 523, 503, 524, 505, 1480, 803, 1685, 1712, 1686, 1713) then ''Y''
                    else ''N'' end,
                    red.configuration,
                    red.download_am
           Order by red.configuration, t.track_title';
 
   raise info 'CREATE/INGEST MECHANICAL ROYALTY TABLE FOR PERIOD % ',v_period;
   raise info 'cmd = %', cmd;
   EXECUTE cmd;

   RETURN v_royalty_table;
 
 END;
 $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION cds_content.mechanical_red_us_royalty_load(integer)
  OWNER TO ikamov;