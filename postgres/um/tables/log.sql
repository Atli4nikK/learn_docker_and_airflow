-- DROP TABLE um.log;

CREATE TABLE um.log (
	run_id INT8 DEFAULT 0 NOT NULL,
	dag VARCHAR NULL,
	ui_run_id VARCHAR DEFAULT ''::VARCHAR NOT NULL
);

COMMENT ON TABLE um.log IS 'Таблица для хранения логов запусков DAG';

COMMENT ON COLUMN um.log.run_id IS 'Идентификатор запуска DAG';
COMMENT ON COLUMN um.log.dag IS 'Имя DAG';
COMMENT ON COLUMN um.log.ui_run_id IS 'Идентификатор запуска DAG в UI';


CREATE INDEX idx_log_run_id ON um.log (run_id);
