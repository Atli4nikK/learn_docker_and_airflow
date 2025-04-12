-- DROP TABLE um.loading;

CREATE TABLE um.loading (
  run_id INT8 DEFAULT 0 NOT NULL,
  dag VARCHAR NULL,
  flag CHAR(1) DEFAULT '1'::CHAR NOT NULL,
  oper_day DATE NULL,
  params VARCHAR DEFAULT '{}'::VARCHAR NULL
);

COMMENT ON TABLE um.loading IS 'Таблица для хранения информации о загрузках';

COMMENT ON COLUMN um.loading.run_id IS 'Идентификатор запуска DAG';
COMMENT ON COLUMN um.loading.dag IS 'Имя DAG';
COMMENT ON COLUMN um.loading.flag IS 'Флаг запуска DAG (0 - не запущен, 1 - успех, 2 - ошибка)';
COMMENT ON COLUMN um.loading.oper_day IS 'Операционный день текущая дата - 1 день';
COMMENT ON COLUMN um.loading.params IS 'Дополнительные параметры для запуска DAG';

