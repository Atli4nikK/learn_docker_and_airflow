DROP TABLE IF EXISTS public.newtable;
/
CREATE TABLE public.newtable (
	column1 varchar NULL,
	run_id int8 DEFAULT 0 NOT NULL
);
/
DROP TABLE IF EXISTS public.log;
/
CREATE TABLE public.log (
	dag varchar NULL,
	run_id int8 DEFAULT 0 NOT NULL,
	ui_run_id varchar DEFAULT '':character varying NOT NULL
);
/
