-- DROP TABLE um.dag_schedules;

CREATE TABLE um.dag_schedules (
  dag_name VARCHAR(255) NULL,
  schedule VARCHAR(255) NULL,
  params VARCHAR(255) DEFAULT '{}'::character varying NULL
);

COMMENT ON TABLE um.dag_schedules IS 'Список DAG-ов и их расписаний';

COMMENT ON COLUMN um.dag_schedules.dag_name IS 'Имя DAG-а';
COMMENT ON COLUMN um.dag_schedules.schedule IS 'Расписание';
COMMENT ON COLUMN um.dag_schedules.params IS 'Параметры';
