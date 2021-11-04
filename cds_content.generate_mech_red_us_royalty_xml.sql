-- Function: cds_content.generate_mech_red_us_royalty_xml(character varying, boolean)

-- DROP FUNCTION cds_content.generate_mech_red_us_royalty_xml(character varying, boolean);

CREATE OR REPLACE FUNCTION cds_content.generate_mech_red_us_royalty_xml(arg_table_name character varying, arg_invoice_flag boolean)
  RETURNS character varying AS
$BODY$

--- $DESCRIPTION = Generate Mechanical RED xmls from table cds_content.mech_red_royalty_<qtr_year>
--- $1 = table_name (mechanical red royalty table needed for the xml production)
--- $2 = invoice flag (generating statement without  invoice number)

DECLARE

 cmd                     text;
 v_schema                varchar;
 v_table_name            varchar;
 v_period                varchar;
 v_file_path             varchar;
 v_file_path_in          varchar;
 v_file_name             varchar;
 v_temp_table            varchar;
 v_xml_table             varchar;
 v_reporting_period      integer;
 v_seq_name              varchar;

 v_statement_type        varchar[] := '{"SINGLE RH STATEMENTS", "SODRAC RH STATEMENTS"}';
 v_single_rh_table       varchar;
 v_single_rh_query       varchar; 
 v_sodrac_table          varchar;
 v_sodrac_query          varchar;
 v_invoice_query         varchar;
  
 rec                     record; 
BEGIN 

 v_schema := 'cds_content';
 v_table_name := v_schema||'.'||arg_table_name;
 v_period := substring(upper(arg_table_name), '[0-9]Q([0-9])')||'Q'||substring(upper(arg_table_name), '([0-9]{2})Q');
 v_file_path := directory_path||'STMT-CDS-Mechanical-Red-Statement/DATA_IN/' FROM cds_content.generated_file_directories WHERE file_type='xml';
 v_file_path_in := directory_path||'STMT-CDS-Mechanical-Red-AccountsPayable/DATA_IN/' FROM cds_content.generated_file_directories WHERE file_type='xml';
 v_temp_table := 'mech_royalty_xml_'||v_period;
 v_xml_table := v_schema||'.'||v_temp_table;
 v_reporting_period := reporting_period_id from cds_content.reporting_period where reporting_period_name = v_period;
 v_seq_name := v_schema||'.royalty_invoice_master_royalty_invoice_master_id_seq';

 RAISE INFO 'Processing Table % for period %', v_table_name, v_period;

 --CHECK TABLE IF EXISTS
 IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
           WHERE table_schema = v_schema 
             AND table_name = lower(arg_table_name)) THEN
   RAISE EXCEPTION 'Table % does not exists. Please check.', v_table_name;
 END IF;
 --CHECK MULTIPLE VALID TAX RATES
 IF EXISTS (WITH foo AS (
                          SELECT ROUND(REGEXP_REPLACE(tps_rate.RATE, '%', '')::numeric/100,5) AS tps,
                                 ROUND(REGEXP_REPLACE(tvq_rate.RATE, '%', '')::numeric/100,5) AS tvq,
                                 ROW_NUMBER() OVER(ORDER BY tps_rate.RATE, tvq_rate.RATE) as rownum
                            FROM cds_content.tps_rate tps_rate, cds_content.tvq_rate tvq_rate
                           WHERE tps_rate.obsolete_flag IS FALSE AND tps_rate.invalid_date IS NULL
                             AND tvq_rate.obsolete_flag IS FALSE AND tvq_rate.invalid_date IS NULL
                         ) 
            SELECT * FROM foo WHERE foo.rownum > 1) THEN
   RAISE EXCEPTION 'Canadian tax rates are impropely set up. Please check.';
 END IF;
 
 --=== TABLES/QUERIES NEEDED FOR THE XML PROCESS ===--
 --A TEMP TABLE WILL BE CREATED TO HOLD THE DATA FROM THE MECHANICAL ROYALTIES TABLE
 v_single_rh_table :=           
           'WITH tax AS (SELECT ROUND(REGEXP_REPLACE(tps_rate.RATE, ''%'', '''')::numeric/100,5) AS tps,
                                ROUND(REGEXP_REPLACE(tvq_rate.RATE, ''%'', '''')::numeric/100,5) AS tvq
                           FROM cds_content.tps_rate tps_rate, cds_content.tvq_rate tvq_rate
                          WHERE tps_rate.obsolete_flag IS FALSE AND tps_rate.invalid_date IS NULL
                            AND tvq_rate.obsolete_flag IS FALSE AND tvq_rate.invalid_date IS NULL
                        ) 
            SELECT 
                   a.rightsholderid,
                   a.rightsholder_name,
                   a.contractid,
                   ap.payeeid,
                   COALESCE(tp.company_name_1,(tp.first_name||'' ''||tp.last_name)) AS payee,
                   co.company_name_2 AS company_name, 
                   tp.language_preference,  
                   tp.street1 AS vendoraddress1,
                   tp.street2 AS vendoraddress2, 
                   tp.city AS vendorcity, 
                   tp.region AS vendorstate, 
                   tp.postal_code AS vendorpostal,
                   co.company_name_1 AS cie,
                   co.sub_gl AS gl, 
                   co.sub_codif AS codif,
                   ct.country_name AS vendorcountry, 
                   co.street1 AS stmtaddress1,
                   co.street2 AS stmtaddress2, 
                   co.city AS stmtcity,
                   co.region AS stmtstate,
                   co.country AS stmtcountry, 
                   co.postal_code AS stmtpostal,
                   ap.vendor_number , 
                   mrt.format_name,
                   a.catalog_number AS product_number,
                   a.configuration,
                   a.product_title,
                   a.track_id,
                   a.track_title AS music_work_title,
                   a.isrc,
                   a.duration as track_duration,
                   SUM(a.quantity) AS quantity_reported,
                   ROUND(SUM(case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then a.quantity::numeric * a.rate_used::numeric else a.download_am::numeric end),2) AS amount_reported,
                   ROUND(SUM(a.rate_used),5) AS mech_rate,
                   ROUND(SUM(a.ringtone_rate),5) AS mech_ringtone_rate,
                   ROUND(SUM(a.mech_perf_sync::numeric),5) AS your_share,
                   a.currency,
                   ROUND(SUM(a.final_rate),5) AS royalties_payable, 
                   ap.payment_hold_flag, 
                   cur.currency AS preferred_currency,
                   COALESCE(er.exchange_rate,1.00) AS exchange_rate,
                   ROUND((ROUND(SUM(a.final_rate),5)) * COALESCE(er.exchange_rate,1.00)::numeric(15,8),5) AS converted_royalty,
                   COALESCE(tp.tps_number,'''')::varchar as tps_number,
                   COALESCE(tp.tvq_number,'''')::varchar as tvq_number,
                   CASE WHEN nullif(trim(tp.tps_number),'''') IS NOT NULL THEN tax.tps ELSE NULL END AS tps,
                   CASE WHEN nullif(trim(tp.tvq_number),'''') IS NOT NULL THEN tax.tvq ELSE NULL END AS tvq,
                   0::numeric AS total_including_tax,
                   0::numeric AS total_tax				   
              FROM '||v_table_name||' a
              LEFT JOIN cds_content.mech_royalty_types mrt ON mrt.format_id = a.format_id AND mrt.invalid_date IS NULL
              LEFT JOIN cds_content.agree_pub ap ON ap.agree_pub_id = a.contractid AND ap.entitiesid = a.rightsholderid AND ap.invalid_date IS NULL
              LEFT JOIN cds_content.third_parties tp ON tp.third_party_id = ap.payeeid AND tp.invalid_date IS NULL
              LEFT JOIN cds_content.currencies cur_fr ON cur_fr.currency = a.currency AND cur_fr.invalid_Date IS NULL
              LEFT JOIN cds_content.currencies cur ON cur.currency_id = COALESCE(tp.currency_preference,cur_fr.currency_id) AND cur.invalid_Date IS NULL
              LEFT JOIN cds_content.exchange_rate er ON er.reportingperiodid = '||v_reporting_period||'
                                                     AND er.currency_from = cur_fr.currency_id
                                                     AND er.currency_to = tp.currency_preference
                                                     AND er.begin_date IS NULL
                                                     AND er.end_date IS NULL
              LEFT JOIN cds_content.companies co ON co.company_id = ap.signatoryid AND co.invalid_date IS NULL
              LEFT JOIN cds_content.countries ct ON ct.country_id = tp.country AND ct.invalid_date IS NULL                             
              CROSS JOIN tax
             WHERE a.society = ''N'' -- NOT (COALESCE(soc.society_id,0) = 27 AND soc.territory_id in (-1,405))
             GROUP BY a.rightsholderid,
                      a.rightsholder_name,
                      a.contractid,
                      ap.payeeid,
                      mrt.format_name,
                      a.catalog_number,
                      a.product_title,
                      a.configuration,
                      COALESCE(tp.company_name_1,(tp.first_name||'' ''||tp.last_name)),
                      co.company_name_2 ,
                      tp.language_preference,
                      tp.street1 ,
                      tp.street2 , 
                      tp.city , 
                      tp.region , 
                      tp.postal_code, 
                      ct.country_name, 
                      co.company_name_1 ,
                      co.sub_gl, 
                      co.sub_codif ,
                      co.street1,
                      co.street2, 
                      co.city ,
                      co.region ,
                      co.country , 
                      co.postal_code ,
                      co.country ,         
                      ap.vendor_number , 
                      a.track_id,
                      a.track_title,
                      a.isrc,
                      a.duration,         
                      a.currency, 
                      ap.payment_hold_flag, 
                      cur.currency,
                      er.exchange_rate,
                      tp.tps_number,
                      tp.tvq_number,
                      tax.tps,
                      tax.tvq
            HAVING case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then SUM(a.quantity)
                   else SUM(a.download_am) end > case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then 0 else 0.00 end         
             ORDER BY mrt.format_name, a.rightsholderid, a.contractid, ap.payeeid';
 v_sodrac_table := 
           'WITH tax AS (SELECT ROUND(REGEXP_REPLACE(tps_rate.RATE, ''%'', '''')::numeric/100,5) AS tps,
                                ROUND(REGEXP_REPLACE(tvq_rate.RATE, ''%'', '''')::numeric/100,5) AS tvq
                           FROM cds_content.tps_rate tps_rate, cds_content.tvq_rate tvq_rate
                          WHERE tps_rate.obsolete_flag IS FALSE AND tps_rate.invalid_date IS NULL
                            AND tvq_rate.obsolete_flag IS FALSE AND tvq_rate.invalid_date IS NULL
                        ) 
            SELECT 
                   a.rightsholderid,
                   a.rightsholder_name,
                   a.contractid,
                   s.payeeid,
                   COALESCE(tp.company_name_1,(tp.first_name||'' ''||tp.last_name)) AS payee,
                   co.company_name_2 AS company_name, 
                   tp.language_preference,  
                   tp.street1 AS vendoraddress1,
                   tp.street2 AS vendoraddress2, 
                   tp.city AS vendorcity, 
                   tp.region AS vendorstate, 
                   tp.postal_code AS vendorpostal,
                   co.company_name_1 AS cie,
                   co.sub_gl AS gl, 
                   co.sub_codif AS codif, 
                   ct.country_name AS vendorcountry, 
                   co.street1 AS stmtaddress1,
                   co.street2 AS stmtaddress2, 
                   co.city AS stmtcity,
                   co.region AS stmtstate,
                   co.country AS stmtcountry, 
                   co.postal_code AS stmtpostal,
                   ''400598''::character varying as vendor_number, 
                   mrt.format_name,
                   a.catalog_number AS product_number,
                   a.configuration,
                   p.product_title,
                   a.track_id,
                   a.track_title AS music_work_title,
                   a.isrc,
                   a.duration as track_duration,
                   SUM(a.quantity) AS quantity_reported,
                   ROUND(SUM(case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then a.quantity::numeric * a.rate_used::numeric else a.download_am::numeric end),2) AS amount_reported,
                   ROUND(SUM(a.rate_used),5) AS mech_rate,
                   ROUND(SUM(a.ringtone_rate),5) AS mech_ringtone_rate,
                   ROUND(SUM(a.mech_perf_sync::numeric),5) AS your_share,
                   a.currency,
                   ROUND(SUM(a.final_rate),5) AS royalties_payable,
                   false as payment_hold_flag, 
                   cur.currency AS preferred_currency,
                   COALESCE(er.exchange_rate,1.00) AS exchange_rate,
                   ROUND((ROUND(SUM(a.final_rate),5)) * COALESCE(er.exchange_rate,1.00)::numeric(15,8),5) AS converted_royalty,
                   COALESCE(tp.tps_number,'''')::varchar as tps_number, /* new requirement from Friday, April 29, 2016 10:53:05 AM, email Subject: RE: Red Mech Downloads 1Q16 Invoices ~Re*/
                   COALESCE(tp.tvq_number,'''')::varchar as tvq_number,
                   tax.tps,
                   tax.tvq,
                   0::numeric AS total_including_tax,
                   0::numeric AS total_tax
              FROM '||v_table_name||' a
              LEFT JOIN cds_content.mech_royalty_types mrt ON mrt.format_id = a.format_id AND mrt.invalid_date IS NULL
              LEFT JOIN cds_content.societies s ON s.society_id = a.society_id AND s.invalid_Date IS NULL 
              LEFT JOIN cds_content.third_parties tp ON tp.third_party_id = s.payeeid AND tp.invalid_date IS NULL
              LEFT JOIN cds_content.currencies cur ON cur.currency_id = COALESCE(tp.currency_preference,1) AND cur.invalid_Date IS NULL
              LEFT JOIN cds_content.currencies cur_fr ON cur_fr.currency = a.currency AND cur_fr.invalid_Date IS NULL
              LEFT JOIN cds_content.exchange_rate er ON er.reportingperiodid = '||v_reporting_period||' 
                                                     AND er.currency_from = cur_fr.currency_id
                                                     AND er.currency_to = tp.currency_preference
                                                     AND er.begin_date IS NULL
                                                     AND er.end_date IS NULL
              LEFT JOIN cds_content.companies co ON co.company_id = s.signatoryid AND co.invalid_date IS NULL
              LEFT JOIN cds_content.countries ct ON ct.country_id = tp.country AND ct.invalid_date IS NULL 
              LEFT JOIN cds_content.products p ON p.product_id = a.merchandiseid AND p.invalid_date is null
              CROSS JOIN tax
             WHERE a.society = ''Y'' -- COALESCE(soc.society_id,0) = 27 AND soc.territory_id in (-1,405)
             GROUP BY s.payeeid,
                      COALESCE(tp.company_name_1,(tp.first_name||'' ''||tp.last_name)), 
                      a.society_id,
                      a.rightsholderid,
                      a.rightsholder_name,
                      a.contractid,   
                      co.company_name_2, 
                      tp.language_preference, 
                      tp.street1 ,
                      tp.street2 , 
                      tp.city , 
                      tp.region , 
                      tp.postal_code,
                      tp.country, 
                      co.company_name_1, 
                      co.sub_gl, 
                      co.sub_codif, 
                      ct.country_name, 
                      co.street1, 
                      co.street2, 
                      co.city, 
                      co.region, 
                      co.country, 
                      co.postal_code, 
                      mrt.format_name,
                      a.catalog_number,
                      p.product_title,  
                      a.configuration,  
                      a.track_id,
                      a.track_title,
                      a.isrc,
                      a.duration,         
                      a.currency,
                      er.exchange_rate,
                      cur.currency,
                      tp.tps_number,
                      tp.tvq_number,
                      tax.tps,
                      tax.tvq
            HAVING case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then SUM(a.quantity)
                   else SUM(a.download_am) end > case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then 0 else 0.00 end         
             ORDER BY mrt.format_name,
                      a.rightsholderid';
 v_single_rh_query := 
       'SELECT XMLElement(name shows, 
                          XMLElement (name one_show, 
                                      XMLForest(generate_date, showscountriesid, rightsholder_name,
                                                payee, period, your_royalty, converted_royalty, converted_currency, 
                                                payable_company,'||case when arg_invoice_flag then 'invoice,' else '' end ||' memo, your_royalty_total, 
                                                your_converted_royalty_total, language_pref, tps, tvq, total_including_tax, total_tax, vendor_number), 
                                      XMLElement(name lineitems, 
                                                (SELECT XMLAGG(XMLElement(name lineitem, 
                                                                          XMLForest(configuration,
                                                                                    product_title, 
                                                                                    music_work_title,
                                                                                    isrc, 
                                                                                    track_duration, 
                                                                                    case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then quantity_reported else '''' end as quantity_reported,
                                                                                    case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then mech_rate else '''' end as mech_rate,
                                                                                    case when lower(configuration) ~* ''mastertone|ringback tone'' then amount_reported else '''' end as amount_reported,
                                                                                    case when lower(configuration) ~* ''mastertone|ringback tone'' then mech_ringtone_rate else '''' end as mech_ringtone_rate,
                                                                                    your_share,
                                                                                    rightsholder_name,
                                                                                    royalties_payable, 
                                                                                    exchange_rate, 
                                                                                    converted_royalty)
                                                                          )
                                                               ) 
                                                   FROM (SELECT 
                                                                product_number,
                                                                product_title,
                                                                configuration,
                                                                music_work_title,
                                                                isrc,
                                                                track_duration, 
                                                                rightsholder_name,
                                                                TRANSLATE(TO_CHAR(SUM(quantity_reported), ''FM999G999G999G999G990''),'',.'','' ,'') AS quantity_reported, 
                                                                TRANSLATE(TO_CHAR(SUM(amount_reported), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS amount_reported, 
                                                                TRANSLATE(TO_CHAR(ROUND(SUM(mech_rate),5), ''FM999G999G999G999G990D9990''),'',.'','' ,'') AS mech_rate, 
                                                                TRANSLATE(TO_CHAR(ROUND(SUM(mech_ringtone_rate),5), ''FM999G999G999G999G990D9990''),'',.'','' ,'') AS mech_ringtone_rate, 
                                                                TRANSLATE(TO_CHAR(ROUND(SUM(your_share),5), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_share, 
                                                                TRANSLATE(TO_CHAR(ROUND(SUM(royalties_payable),5), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS royalties_payable, 
                                                                TRANSLATE(TO_CHAR(exchange_rate,''FM999G999G999G999G990D999990''),'',.'','' ,'') AS exchange_rate,
                                                                TRANSLATE(TO_CHAR(ROUND(SUM(converted_royalty),5), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS converted_royalty,
                                                                preferred_currency 
                                                           FROM temp_table  
                                                          WHERE COALESCE(payee, ''Cirque Share'') = COALESCE(bar.payee, ''Cirque Share'') 
                                                            AND COALESCE(contractid, 1) = COALESCE(bar.contractid,1)
                                                            AND preferred_currency = bar.converted_currency 
                                                            AND rightsholderid = bar.rightsholderid     
                                                          GROUP BY payeeid, 
                                                                   contractid,
                                                                   rightsholder_name,
                                                                   product_number, 
                                                                   product_title,
                                                                   configuration,
                                                                   music_work_title,
                                                                   isrc,
                                                                   track_duration, 
                                                                   exchange_rate, 
                                                                   preferred_currency
                                                          ORDER BY configuration, product_title, music_work_title
                                                        ) foo 
                                               )
                                           )
                                     )
                         ) as XMLs, 
                   REGEXP_REPLACE(fn_ascii_convert(period||''_mech_royalty_''||rightsholder_name||''_''||COALESCE(contractid,''0'')), ''[^a-zA-Z0-9]'', '''', ''g'')||''_''||converted_currency||''.xml'' AS filename 
              FROM (SELECT REGEXP_REPLACE(cds_content.fn_date_format(current_date,1)::VARCHAR,''-'',''/'',''g'') AS generate_date, 
                           a.contractid, 
                           '''||v_reporting_period||'''::varchar AS showscountriesid, 
                           a.rightsholder_name,   
                           a.rightsholderid,    
                           COALESCE(a.payee,''Cirque Share'') AS payee, 
                           '''||v_period||'''::varchar AS period, 
                           TRANSLATE(TO_CHAR(SUM(a.royalties_payable), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_royalty, 
                           TRANSLATE(TO_CHAR(SUM(a.converted_royalty), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS converted_royalty, 
                           a.preferred_currency AS converted_currency, 
                           a.company_name AS payable_company, 
                          '||case when  arg_invoice_flag then 'a.invoice AS invoice,' else ' ' end ||'
                          ''If the amount is below 10.00$ (CAD), we shall cumulate this amount with the royalties in the next quarter.'' AS memo, 
                          TRANSLATE(TO_CHAR(SUM(quantity_reported), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS quantity_reported, 
                          TRANSLATE(TO_CHAR(ROUND(SUM(a.mech_rate),5), ''FM999G999G999G999G990D9990''),'',.'','' ,'') AS mech_rate, 
                          TRANSLATE(TO_CHAR(ROUND(SUM(a.your_share),5), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_share,          
                          TRANSLATE(TO_CHAR(SUM(a.royalties_payable), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_royalty_total, 
                          TRANSLATE(TO_CHAR(SUM(a.converted_royalty), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_converted_royalty_total, 
                          TRANSLATE(TO_CHAR(SUM(a.converted_royalty * a.tps), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS tps, 
                          TRANSLATE(TO_CHAR(SUM(a.converted_royalty * a.tvq), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS tvq, 						  
                          TRANSLATE(TO_CHAR(SUM(a.total_including_tax), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS total_including_tax,
                          TRANSLATE(TO_CHAR(SUM(a.total_tax), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS total_tax,
                          a.language_preference AS language_pref, 
                          a.vendor_number
                     FROM temp_table a 
                    GROUP BY generate_date, 
                             a.contractid, 
                             showscountriesid, 
                             a.rightsholder_name,
                             a.rightsholderid, 
                             a.payee, 
                             period, 
                             a.preferred_currency, 
                             a.company_name,'
                             ||case when  arg_invoice_flag then 'a.invoice, ' else ' ' end||'
                             a.language_preference, 
                             a.tps, a.tvq, 
                             a.vendor_number
                    ORDER BY COALESCE(a.payee,''Cirque Share''), a.contractid
                   ) bar';
 v_sodrac_query :=  
           'SELECT XMLElement(name shows, 
                              XMLElement (name one_show, 
                                          XMLForest(generate_date, showscountriesid, payee, period, your_royalty, converted_royalty, converted_currency, 
                                                    payable_company,'||case when  arg_invoice_flag then 'invoice,' else '' end ||' memo, your_royalty_total, 
                                                    your_converted_royalty_total, language_pref, tps, tvq, total_including_tax, total_tax, vendor_number), 
                                          XMLElement(name lineitems, 
                                                     (SELECT XMLAGG(XMLElement(name lineitem, 
                                                                               XMLForest(configuration,
                                                                                         product_title, 
                                                                                         music_work_title,
                                                                                         isrc,
                                                                                         track_duration, 
                                                                                         case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then quantity_reported else '''' end as quantity_reported,
                                                                                         case when lower(configuration) ~* ''digital audio track|digital audio longplay'' then mech_rate else '''' end as mech_rate,
                                                                                         case when lower(configuration) ~* ''mastertone|ringback tone'' then amount_reported else '''' end as amount_reported,
                                                                                         case when lower(configuration) ~* ''mastertone|ringback tone'' then mech_ringtone_rate else '''' end as mech_ringtone_rate,
                                                                                         your_share,
                                                                                         rightsholder_name,
                                                                                         royalties_payable, 
                                                                                         exchange_rate, 
                                                                                         converted_royalty
                                                                                        )
                                                                               )
                                                                    )
                                                        FROM (SELECT configuration,
                                                                     rightsholder_name, 
                                                                     product_title,                                                                     
                                                                     music_work_title,
                                                                     isrc, 
                                                                     track_duration, 
                                                                     TRANSLATE(TO_CHAR(SUM(quantity_reported), ''FM999G999G999G999G990''),'',.'','' ,'') AS quantity_reported, 
                                                                     TRANSLATE(TO_CHAR(SUM(amount_reported), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS amount_reported, 
                                                                     TRANSLATE(TO_CHAR(ROUND(SUM(mech_rate),5), ''FM999G999G999G999G990D9990''),'',.'','' ,'') AS mech_rate, 
                                                                     TRANSLATE(TO_CHAR(ROUND(SUM(mech_ringtone_rate),5), ''FM999G999G999G999G990D9990''),'',.'','' ,'') AS mech_ringtone_rate, 
                                                                     TRANSLATE(TO_CHAR(ROUND(SUM(your_share),5), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_share, 
                                                                     TRANSLATE(TO_CHAR(ROUND(SUM(royalties_payable),5), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS royalties_payable, 
                                                                     TRANSLATE(TO_CHAR(exchange_rate,''FM999G999G999G999G990D999990''),'',.'','' ,'') AS exchange_rate,
                                                                     TRANSLATE(TO_CHAR(ROUND(SUM(converted_royalty),5), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS converted_royalty,
                                                                     preferred_currency 
                                                                FROM temp_table  
                                                               WHERE COALESCE(payee, ''Cirque Share'') = COALESCE(bar.payee, ''Cirque Share'') 
                                                                 -- AND COALESCE(contractid, 1) = COALESCE(bar.contractid,1)
                                                                 AND preferred_currency = bar.converted_currency 
                                                                 -- AND rightsholderid=bar.rightsholderid     
                                                               GROUP BY payeeid, 
                                                                        contractid,
                                                                        configuration,
                                                                        rightsholder_name,
                                                                        product_number, 
                                                                        product_title,
                                                                        music_work_title,
                                                                        isrc,
                                                                        track_duration, 
                                                                        exchange_rate, 
                                                                        preferred_currency
                                                               ORDER BY configuration, product_title, music_work_title
                                                             ) foo 
                                                  )
                                             )
                                         )
                              ) as XMLs, 
                   REGEXP_REPLACE(fn_ascii_convert(period||''_mech_royalty_''||payee), ''[^a-zA-Z0-9]'', '''', ''g'')||''.xml'' AS filename 
              FROM (SELECT REGEXP_REPLACE(cds_content.fn_date_format(current_date,1)::VARCHAR,''-'',''/'',''g'') AS generate_date, 
                           '''||v_reporting_period||'''::varchar AS showscountriesid,        
                           COALESCE(a.payee,''Cirque Share'') AS payee, 
                           '''||v_period||'''::varchar AS period, 
                           TRANSLATE(TO_CHAR(SUM(a.royalties_payable), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_royalty, 
                           TRANSLATE(TO_CHAR(SUM(a.converted_royalty), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS converted_royalty, 
                           a.preferred_currency AS converted_currency, 
                           a.company_name AS payable_company,  
                           '||case when  arg_invoice_flag then 'a.invoice AS invoice,' else ' ' end ||'
                           ''If the amount is below 10.00$ (CAD), we shall cumulate this amount with the royalties in the next quarter.'' AS memo, 
                           TRANSLATE(TO_CHAR(SUM(quantity_reported), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS quantity_reported, 
                           TRANSLATE(TO_CHAR(ROUND(SUM(a.mech_rate),5), ''FM999G999G999G999G990D9990''),'',.'','' ,'') AS mech_rate, 
                           TRANSLATE(TO_CHAR(ROUND(SUM(a.your_share),5), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_share,          
                           TRANSLATE(TO_CHAR(SUM(a.royalties_payable), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_royalty_total, 
                           TRANSLATE(TO_CHAR(SUM(a.converted_royalty), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS your_converted_royalty_total, 
                           TRANSLATE(TO_CHAR(SUM(a.converted_royalty * a.tps), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS tps, 
                           TRANSLATE(TO_CHAR(SUM(a.converted_royalty * a.tvq), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS tvq, 						  
                           TRANSLATE(TO_CHAR(SUM(a.total_including_tax), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS total_including_tax,
                           TRANSLATE(TO_CHAR(SUM(a.total_tax), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS total_tax,
                           a.language_preference AS language_pref, 
                           a.vendor_number
                      FROM temp_table a 
                     GROUP BY generate_date, 
                              showscountriesid,  
                              a.payee, 
                              period, 
                              a.preferred_currency,
                              a.company_name, '
                              ||case when arg_invoice_flag then 'a.invoice, ' else ' ' end ||'
                              a.language_preference, 
                              a.tps, a.tvq, 
                              a.vendor_number
             ORDER BY COALESCE(a.payee,''Cirque Share'')) bar';
 v_invoice_query := 
           'SELECT XMLElement(name statements, 
                              XMLElement (name statement, 
                                          XMLForest(sd_royalty_invoice_master_id, sd_date, 
                                                    sd_company_name, sd_stmtaddress1, sd_stmtaddress2, 
                                                    sd_stmtcity, sd_stmtstate, sd_stmtcountry, 
                                                    sd_stmtpostal, sd_vendorname, sd_vendorid, 
                                                    sd_vendoraddress1, sd_vendoraddress2, sd_vendorcity, 
                                                    sd_vendorstate, sd_vendorpostal, sd_vendorcountry, 
                                                    sd_tps_number, sd_tvq_number,
                                                    sd_currency, sd_quantity_reported, sd_total,
                                                    sd_tps_total, sd_tvq_total,
                                                    sd_net_a_payer, sd_language_pref), 
                                          XMLElement(name details, 
                                                     (SELECT XMLAGG(XMLElement(name detail, XMLForest(description, cie, gl, codif, quantity, sum))) 
                                                        FROM (SELECT ''Royalties from '' ||format_name AS description, 
                                                                     a.cie, a.gl, a.codif, a.invoice,                                                  
                                                                     TRANSLATE(TO_CHAR(SUM(quantity_reported), ''FM999G999G999G999G990''),'',.'','' ,'') AS "quantity",                     
                                                                     TRANSLATE(TO_CHAR(ROUND(SUM(a.converted_royalty),5),''FM999G999G999G999G990D90''),'',.'','' ,'') AS "sum" 
                                                                FROM temp_table a 
                                                               WHERE payment_hold_flag = false 
                                                               GROUP BY a.invoice, a.format_name, a.cie, a.gl, a.codif 
                                                               ORDER BY a.invoice 
                                                              ) foo 
                                     WHERE COALESCE(foo.invoice,''1'') = COALESCE(bar.sd_royalty_invoice_master_id,''1'') )))) as XMLs, 
                   REGEXP_REPLACE(fn_ascii_convert(COALESCE(sd_vendorid,''Cirque_Share'')||''_facture_''||sd_royalty_invoice_master_id||''_''||period), ''[^a-zA-Z0-9_]'', '''', ''g'')||''.xml'' AS filename 
            FROM (SELECT a.invoice AS sd_royalty_invoice_master_id, 
                         '''||v_period||'''::varchar AS period, 
                         REGEXP_REPLACE(cds_content.fn_date_format(current_date,1)::varchar,''-'',''/'',''g'') AS sd_date,                          
                         a.company_name AS sd_company_name, a.stmtaddress1 AS sd_stmtaddress1, 
                         a.stmtaddress2 AS sd_stmtaddress2, a.stmtcity AS sd_stmtcity, 
                         a.stmtstate AS sd_stmtstate, a.stmtcountry AS sd_stmtcountry, 
                         a.stmtpostal AS sd_stmtpostal, COALESCE(a.payee,''Cirque Share'') AS sd_vendorname, 
                         a.vendor_number AS sd_vendorid, a.vendoraddress1 AS sd_vendoraddress1, 
                         a.vendoraddress2 AS sd_vendoraddress2, a.vendorcity AS sd_vendorcity, 
                         a.vendorstate AS sd_vendorstate, a.vendorpostal AS sd_vendorpostal, 
                         a.vendorcountry AS sd_vendorcountry, 
                         a.tps_number AS sd_tps_number, 
                         a.tvq_number AS sd_tvq_number,
                         a.preferred_currency AS sd_currency, 
                         TRANSLATE(TO_CHAR(SUM(quantity_reported), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS sd_quantity_reported, 
                         TRANSLATE(TO_CHAR(SUM(a.converted_royalty), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS sd_total, 
                         TRANSLATE(TO_CHAR(SUM(a.converted_royalty * a.tps), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS sd_tps_total, 
                         TRANSLATE(TO_CHAR(SUM(a.converted_royalty * a.tvq), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS sd_tvq_total, 						 
                         TRANSLATE(TO_CHAR(SUM(a.total_including_tax), ''FM999G999G999G999G990D90''),'',.'','' ,'') AS sd_net_a_payer, 
                         a.language_preference AS sd_language_pref 
                  FROM temp_table a 
                  WHERE payment_hold_flag = false 
                  GROUP BY a.invoice, sd_date, 
                           a.company_name, a.stmtaddress1, a.stmtaddress2, a.stmtcity, 
                           a.stmtstate, a.stmtcountry, a.stmtpostal, a.payee, 
                           a.vendor_number, a.vendoraddress1, a.vendoraddress2, 
                           a.vendorcity, a.vendorstate, a.vendorpostal, a.vendorcountry, 
                           a.tps_number, a.tvq_number, a.preferred_currency, 
                           a.tps, a.tvq, a.language_preference 
                  ORDER BY a.company_name, a.payee) bar';
   
 FOR i IN 1..array_upper(v_statement_type, 1) LOOP

   DROP TABLE IF EXISTS temp_table;    
   RAISE INFO 'Gathering data for % into temp table', v_statement_type[i];

   cmd := 'CREATE TEMP TABLE temp_table ON COMMIT DROP AS '||CASE WHEN i = 1 THEN v_single_rh_table ELSE v_sodrac_table END;
   RAISE INFO 'v_reporting_period = % ,v_table_name=%', v_reporting_period, v_table_name;
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;

   RAISE INFO 'Checking and Calculating Total + Tax per payee';
   cmd := 'UPDATE temp_table SET total_including_tax = converted_royalty + (converted_royalty * coalesce(tps, 0)) + (converted_royalty * coalesce(tvq, 0))';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;

   RAISE INFO 'Calculating TPS and TVQ total taxes';
   cmd := 'UPDATE temp_table SET total_tax = (converted_royalty * coalesce(tps, 0)) + (converted_royalty * coalesce(tvq, 0))';
   RAISE INFO 'cmd = %', cmd;
   EXECUTE cmd;
  
   RAISE INFO 'Checking if there''s a payee that has no tps number but has a tvq number';
   IF EXISTS (SELECT 1 FROM temp_table WHERE tps_number IS NULL AND tvq_number IS NOT NULL) THEN
     RAISE EXCEPTION 'Please check as there is a payee with no tps number but has a tvq number';
   ELSE
     RAISE INFO 'TPS and TVQ check for payees are cleared';
   END IF;

   IF arg_invoice_flag  THEN
   -- Assign Invoice Number per payee and agreement
     RAISE INFO 'Assigning Invoice Numbers';
     ALTER TABLE temp_table ADD COLUMN invoice character varying;
    
     cmd := 'UPDATE temp_table t
                SET invoice = lpad(w_seq.seq_val::varchar,9,''0''::varchar) 
               FROM (SELECT payeeid, nextval('''||v_seq_name||'''::regclass) AS seq_val 
                       FROM temp_table 
                      WHERE payment_hold_flag = false 
                      GROUP BY payeeid
                      ORDER BY payeeid) w_seq 
              WHERE COALESCE(t.payeeid, 9999) = COALESCE(w_seq.payeeid, 9999)';
     RAISE INFO 'cmd = %', cmd;
     EXECUTE cmd;
   END IF;    
   
   -- Generate Statment with/no invoice in XML from the temp table
   RAISE INFO 'Generating Statment in XML from the temp table';
   cmd := CASE WHEN i = 1 THEN v_single_rh_query ELSE v_sodrac_query END; 
   RAISE INFO 'cmd = %', cmd;
   
   FOR rec IN EXECUTE cmd LOOP
     v_file_name := v_file_path||rec.filename;
     cmd := 'COPY (select ''<?xml version="1.0" encoding="UTF-8"?>''||'||quote_literal(rec.XMLs)||') TO '||quote_literal(v_file_name);   
     EXECUTE cmd;
     RAISE INFO 'Statement XML FILE % CREATED IN %', rec.filename, v_file_name;
   END LOOP;

   IF arg_invoice_flag THEN
     -- Generate Invoice in XML from the temp table
     RAISE INFO 'Generating Invoice in XML from the temp table';
     cmd := v_invoice_query; 
     RAISE INFO 'cmd = %', cmd;

     FOR rec IN EXECUTE cmd LOOP
       v_file_name := v_file_path_in||rec.filename;
       cmd := 'COPY (select ''<?xml version="1.0" encoding="UTF-8"?>''||'||quote_literal(rec.XMLs)||') TO '||quote_literal(v_file_name);  
       EXECUTE cmd;
       RAISE INFO 'Invoice XML FILE % CREATED IN %', rec.filename, v_file_name;
     END LOOP;
   END IF;

 END LOOP;     
 
 RETURN 'Complete';

END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION cds_content.generate_mech_red_us_royalty_xml(character varying, boolean)
  OWNER TO ikamov;