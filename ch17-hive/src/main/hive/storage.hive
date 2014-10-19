DROP TABLE IF EXISTS users;

CREATE TABLE users (id INT, name STRING);

LOAD DATA LOCAL INPATH 'input/hive/tables/users.txt'
OVERWRITE INTO TABLE users;

DROP TABLE IF EXISTS users_seqfile;

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.DeflateCodec;
SET mapreduce.output.fileoutputformat.compress.type=BLOCK;
CREATE TABLE users_seqfile STORED AS SEQUENCEFILE
AS
SELECT id, name FROM users;

SELECT * from users_seqfile;

DROP TABLE IF EXISTS users_avro;

SET hive.exec.compress.output=true;
-- SET avro.output.codec=snappy;
CREATE TABLE users_avro (id INT, name STRING)
STORED AS AVRO;
INSERT OVERWRITE TABLE users_avro
SELECT * FROM users;

SELECT * from users_avro;

DROP TABLE IF EXISTS users_parquet;

CREATE TABLE users_parquet STORED AS PARQUET
AS
SELECT * FROM users;

SELECT * from users_parquet;

DROP TABLE IF EXISTS users_orc;

CREATE TABLE users_orc STORED AS ORCFILE
AS
SELECT * FROM users;

SELECT * from users_orc;
