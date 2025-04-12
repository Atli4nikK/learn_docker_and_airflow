-- DROP TABLE um.schedules;

CREATE TABLE um.schedules (
  schedule VARCHAR NULL,
  sql_expression VARCHAR NULL
);

COMMENT ON TABLE um.schedules IS 'Таблица для хранения расписаний DAG';

COMMENT ON COLUMN um.schedules.schedule IS 'Имя расписания';
COMMENT ON COLUMN um.schedules.sql_expression IS 'SQL выражение для расписания';
