DROP TABLE IF EXISTS public.newtable;

CREATE TABLE public.newtable (
	column1 varchar NULL,
	run_id int8 DEFAULT 0 NOT NULL
);

DROP TABLE IF EXISTS public.newtable_1;

CREATE TABLE public.newtable_1 (
	column1 varchar NULL,
	run_id int8 DEFAULT 0 NOT NULL
);

insert into public.newtable(column1)values('');
commit;

-- DROP SCHEMA um;
CREATE SCHEMA um AUTHORIZATION "admin";

-- DROP TABLE um.dag_schedules;

CREATE TABLE um.dag_schedules (
	dag_name varchar NULL,
	schedule varchar NULL,
	params varchar DEFAULT '{}'::character varying NULL
);

INSERT INTO um.dag_schedules (dag_name,schedule,params) VALUES
	 ('my_first_dag','daily','{}'),
	 ('openweather_dag','daily_8','{"City":"Krasnodar"}'),
	 ('s3_dag', 'monthly', '{}');

-- DROP TABLE um.loading;

CREATE TABLE um.loading (
	run_id int8 DEFAULT 0 NOT NULL,
	dag varchar NULL,
	flag bpchar(1) DEFAULT '1'::bpchar NOT NULL,
	oper_day date NULL,
	params varchar DEFAULT '{}'::character varying NULL
);

-- DROP TABLE um.log;

CREATE TABLE um.log (
	run_id int8 DEFAULT 0 NOT NULL,
	dag varchar NULL,
	ui_run_id varchar DEFAULT ''::character varying NOT NULL
);

-- DROP TABLE um.schedules;

CREATE TABLE um.schedules (
	schedule varchar NULL,
	sql_expression varchar NULL
);

INSERT INTO um.schedules (schedule,sql_expression) VALUES
	 ('daily','select case when now() > to_timestamp(to_char(now(), ''ddmmYYYY'') || '' 00:05'', ''ddmmYYYY hh24:mi'') then ''START'' else null end'),
	 ('daily_8','select case when now() > to_timestamp(to_char(now(), ''ddmmYYYY'') || '' 08:00'', ''ddmmYYYY hh24:mi'') then ''START'' else null end'),
	 ('monthly','select case when extract(day from now()) = 1 and now() > to_timestamp(to_char(now(), ''ddmmYYYY'') || '' 00:05'', ''ddmmYYYY hh24:mi'') then ''START'' else null end');

-- DROP SEQUENCE um.seq_run_id;

CREATE SEQUENCE um.seq_run_id
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

-- DROP FUNCTION um.dag_reg();

CREATE OR REPLACE FUNCTION um.dag_reg()
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
declare 
  v_status varchar;
  v_run_id int8 := 0;
  str record;
  v_already_started varchar;
  v_result varchar;
BEGIN
  for str in (
	select d.dag_name, s.sql_expression, d.params
	from um.dag_schedules d
	inner join um.schedules s on s.schedule = d.schedule
  )
  loop

	EXECUTE str.sql_expression INTO v_result;

    if v_result is null
	then v_status := '';
         continue;
	end if;

	execute 'select max(l.flag)
			from um.loading l
			where l.dag = $1
			and (l.oper_day = now()::date - 1) ' into v_already_started using str.dag_name;

	if v_already_started is null
    then v_run_id := nextval('seq_run_id');
         insert into loading values (v_run_id, str.dag_name, '0', now()::date - 1, str.params);
	end if;
	
  end loop;
  return v_run_id;
END;
$function$
;

-- DROP FUNCTION um.fail_proc(varchar, int8);

CREATE OR REPLACE FUNCTION um.fail_proc(v_dag_name character varying, v_run_id bigint)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
declare 
BEGIN
  update um.loading
  set flag = '2'
  where dag = v_dag_name
    and run_id = v_run_id;

  return 1;
END;
$function$
;

-- DROP FUNCTION um.success_proc(varchar, int8);

CREATE OR REPLACE FUNCTION um.success_proc(v_dag_name character varying, v_run_id bigint)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
declare 
BEGIN
  update um.loading
  set flag = '1'
  where dag = v_dag_name
    and run_id = v_run_id;

  return 1;
END;
$function$
;

