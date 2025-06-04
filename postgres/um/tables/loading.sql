-- DROP TABLE um.loading;

CREATE TABLE um.loading (
  run_id INT8 DEFAULT 0 NOT NULL,
  dag VARCHAR NULL,
  flag CHAR(1) DEFAULT '1'::CHAR NOT NULL,
  oper_day DATE NULL,
  params VARCHAR DEFAULT '{}'::VARCHAR NULL,
  start_date TIMESTAMP,
  end_date TIMESTAMP
);

COMMENT ON TABLE um.loading IS 'Таблица для хранения информации о загрузках';

COMMENT ON COLUMN um.loading.run_id IS 'Идентификатор запуска DAG';
COMMENT ON COLUMN um.loading.dag IS 'Имя DAG';
COMMENT ON COLUMN um.loading.flag IS 'Флаг запуска DAG (0 - не запущен, 1 - успех, 2 - ошибка)';
COMMENT ON COLUMN um.loading.oper_day IS 'Операционный день текущая дата - 1 день';
COMMENT ON COLUMN um.loading.params IS 'Дополнительные параметры для запуска DAG';
COMMENT ON COLUMN um.loading.start_date IS 'Регистрация запуска дага';
COMMENT ON COLUMN um.loading.end_date IS 'Регистрация окончания работы дага';


ALTER TABLE um.loading ADD CONSTRAINT pk_loading_run_id PRIMARY KEY (run_id);

CREATE INDEX idx_loading_flag ON um.loading(flag) WHERE flag = '0';
CREATE INDEX idx_loading_dag_oper_day ON um.loading(dag, oper_day);
CREATE INDEX idx_loading_dag_run_id ON um.loading(dag, run_id);
CREATE INDEX idx_loading_flag_2 ON um.loading(flag) WHERE flag = '2';
