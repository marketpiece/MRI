-- Function: client_cue.web_load_rc_program_matched()

-- DROP FUNCTION client_cue.web_load_rc_program_matched(INTEGER);

CREATE OR REPLACE FUNCTION client_cue.web_load_rc_program_matched(
    IN arg_file_id INTEGER,
    OUT program CHARACTER VARYING,
    OUT program_id CHARACTER VARYING,
    OUT bin_program_id CHARACTER VARYING
)
  RETURNS SETOF record AS
$BODY$
-- $DESCRIPTION = Returns rapidcue programs
BEGIN
    
    RETURN QUERY
    SELECT p.title::varchar as program, 
           p.row_id::varchar as program_id,
           p.original_program_id::varchar bin_program_id
      FROM client_cue.rc_program p
     WHERE p.file_id = arg_file_id
       AND p.matched is true
     ORDER BY 1;
    IF NOT FOUND THEN
      RETURN QUERY
      SELECT null::varchar,
             null::varchar,
             null::varchar;
    END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;