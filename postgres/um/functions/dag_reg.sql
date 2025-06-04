-- DROP FUNCTION um.dag_reg();

CREATE OR REPLACE FUNCTION um.dag_reg()
RETURNS INT8
LANGUAGE plpgsql
AS $$
DECLARE 
  v_status VARCHAR;
  v_run_id INT8 := 0;
  v_str RECORD;
  v_already_started VARCHAR;
  v_result VARCHAR;
BEGIN

  FOR v_str IN (
    SELECT d.dag_name, s.sql_expression, d.params
	  FROM um.dag_schedules d
	  INNER JOIN um.schedules s ON s.schedule = d.schedule
  )
  LOOP

	  EXECUTE v_str.sql_expression INTO v_result;

    IF v_result IS NULL
	  THEN v_status := '';
         CONTINUE;
	  END IF;

	  EXECUTE 'SELECT MAX(l.flag)
			       FROM um.loading l
			       WHERE l.dag = $1
			       AND (l.oper_day = NOW()::DATE - 1)' 
    INTO v_already_started USING v_str.dag_name;

	  IF v_already_started IS NULL
	  THEN v_run_id := NEXTVAL('seq_run_id');
	       INSERT INTO um.loading (run_id, dag, flag, oper_day, params)
                         VALUES (v_run_id, v_str.dag_name, '0', NOW()::DATE - 1, v_str.params);
	  END IF;

  END LOOP;

  RETURN v_run_id;

EXCEPTION WHEN OTHERS THEN RETURN 0;
END;
$$
EXECUTE ON ANY;