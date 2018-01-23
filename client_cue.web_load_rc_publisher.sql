-- Function: client_cue.web_load_rc_publisher()

-- DROP FUNCTION client_cue.web_load_rc_publisher(INTEGER,BIGINT);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_publisher(
    IN arg_file_id INTEGER,
    IN arg_cuesong_id BIGINT,
    OUT publisher_id BIGINT,
    OUT publisher CHARACTER VARYING,
    OUT affiliation CHARACTER VARYING,
    OUT cae_code BIGINT,
    OUT address CHARACTER VARYING,
    OUT zip_code CHARACTER VARYING,
    OUT country INTEGER,
    OUT notes CHARACTER VARYING,
    OUT matched CHARACTER VARYING
)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns rapidcue composers
DECLARE
    v_cmd  VARCHAR;
BEGIN
    
    v_cmd := 'SELECT DISTINCT
                     p.row_id,
                     p.publisher_name,
                     p.affiliation,
                     p.cae_code,
                     p.address,
                     p.zip_code,
                     p.country,
                     p.notes,
                     CASE WHEN p.matched THEN ''Yes'' ELSE '''' END::varchar
              FROM client_cue.rc_publisher p
              LEFT JOIN client_cue.rc_cuesong_publisher cp ON cp.publisher_id = p.row_id
             WHERE p.file_id = '||arg_file_id||'
              '||CASE WHEN arg_cuesong_id is not null THEN 'AND cp.cuesong_id = '||arg_cuesong_id ELSE '' END||'
              ORDER BY 3,2';
    RETURN QUERY EXECUTE v_cmd;
    IF NOT FOUND THEN
      RETURN QUERY
      SELECT null::bigint, 
             null::varchar, 
             null::varchar, 
             null::bigint, 
             null::varchar,
             null::varchar,
             null::int,
             null::varchar,
             null::varchar;
    END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;