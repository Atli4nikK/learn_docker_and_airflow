-- DROP FUNCTION um.delete_dag(varchar);

CREATE OR REPLACE FUNCTION um.delete_dag(p_dag_name VARCHAR)
RETURNS INT8
LANGUAGE plpgsql
AS $$
DECLARE
BEGIN

  DELETE FROM um.dag_schedules
  WHERE dag_name = p_dag_name;
        
  RETURN 1;

EXCEPTION WHEN OTHERS THEN RETURN 0;
END;
$$
EXECUTE ON ANY;