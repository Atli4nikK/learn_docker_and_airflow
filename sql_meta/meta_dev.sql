DROP TABLE IF EXISTS public.newtable;
/
CREATE TABLE public.newtable (
	column1 varchar NULL,
	run_id int8 DEFAULT 0 NOT NULL
);
/
DROP TABLE IF EXISTS public.newtable_1;
/
CREATE TABLE public.newtable_1 (
	column1 varchar NULL,
	run_id int8 DEFAULT 0 NOT NULL
);
/
insert into public.newtable(column1)values('');
commit;
/