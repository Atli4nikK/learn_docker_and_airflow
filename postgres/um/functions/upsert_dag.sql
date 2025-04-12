-- DROP FUNCTION um.upsert_dag(varchar, int8, varchar);

CREATE OR REPLACE FUNCTION um.upsert_dag(p_dag_name VARCHAR
                                        ,p_schedule INT8
                                        ,p_params VARCHAR)
RETURNS INT8
LANGUAGE plpgsql
AS $$
DECLARE
BEGIN

  INSERT INTO um.dag_schedules (dag_name,schedule,params)
  VALUES (p_dag_name,p_schedule,p_params);
        
  RETURN 1;

EXCEPTION WHEN OTHERS THEN RETURN 0;
END;
$$
EXECUTE ON ANY;