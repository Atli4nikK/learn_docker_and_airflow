-- DROP SEQUENCE um.seq_run_id;

CREATE SEQUENCE um.seq_run_id
  INCREMENT BY 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1
  NO CYCLE;

COMMENT ON SEQUENCE um.seq_run_id IS 'Последовательность для идентификаторов запусков';
