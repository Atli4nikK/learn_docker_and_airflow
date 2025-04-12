-- DROP FUNCTION um.fail_proc(varchar, int8);

CREATE OR REPLACE FUNCTION um.fail_proc(p_dag_name VARCHAR, p_run_id INT8)
RETURNS INT8
LANGUAGE plpgsql
AS $$
DECLARE 
BEGIN
  
  UPDATE um.loading
  SET flag = '2'
  WHERE dag = p_dag_name
    AND run_id = p_run_id;

  RETURN 1;
  
EXCEPTION WHEN OTHERS THEN RETURN 0;
END;
$$
EXECUTE ON ANY;