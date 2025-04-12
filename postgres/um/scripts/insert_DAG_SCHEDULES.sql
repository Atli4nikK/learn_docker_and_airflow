INSERT INTO um.dag_schedules (dag_name, schedule, params)
VALUES ('my_first_dag','daily','{}'),
	   ('openweather_dag','daily_8','{"City":"Krasnodar"}'),
	   ('s3_dag', 'monthly', '{}'),
	   ('dag_autotrend_load_stg_autoru_offers','weekly_7th_00:00', '{}'),
	   ('dag_autotrend_autoru_regions','weekly_1th_00:00','{}');