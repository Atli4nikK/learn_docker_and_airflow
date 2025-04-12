DROP TABLE IF EXISTS public.newtable;

CREATE TABLE public.newtable (
	column1 varchar NULL,
	run_id int8 DEFAULT 0 NOT NULL
);

CREATE SCHEMA tweet AUTHORIZATION admin2;

CREATE TABLE tweet.tweets (
	"user" varchar NULL,
	"text" varchar NULL,
	favorite_count int8 NULL,
	retweet_count int8 NULL,
	created_at timestamptz NULL
);
