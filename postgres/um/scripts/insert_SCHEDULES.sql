INSERT INTO um.schedules (schedule, sql_expression) 
VALUES ('daily','select case when now() > to_timestamp(to_char(now(), ''ddmmYYYY'') || '' 00:00'', ''ddmmYYYY hh24:mi'') then ''START'' else null end'),
	   ('daily_8','select case when now() > to_timestamp(to_char(now(), ''ddmmYYYY'') || '' 08:00'', ''ddmmYYYY hh24:mi'') then ''START'' else null end'),
	   ('monthly','select case when extract(day from now()) = 1 and now() > to_timestamp(to_char(now(), ''ddmmYYYY'') || '' 00:00'', ''ddmmYYYY hh24:mi'') then ''START'' else null end'),
	   ('weekly_7th_00:00','select case when extract(dow from now()) in (0, 7) and now() > to_timestamp(to_char(now(), ''ddmmyyyy'') || '' 00:00'', ''ddmmyyyy hh24:mi'') then ''START'' else null end'),
	   ('weekly_1th_00:00','select case when extract(dow from now()) = 1 and now() > to_timestamp(to_char(now(), ''ddmmyyyy'') || '' 00:00'', ''ddmmyyyy hh24:mi'') then ''START'' else null end'),
	   ('N/A', 'select null');
