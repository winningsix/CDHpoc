SET hive.optimize.ppd=true;
SET hive.optimize.index.filter=true;
SET parquet.bloom.filter.enable.column.names=id,l,s,d;
SET parquet.bloom.filter.expected.entries=1000,1000,1000,1000;

CREATE TABLE bfTbl_staging(
id int, f float, l bigint, s string, d double
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
NULL DEFINED AS '';

CREATE TABLE bfTbl(
id int, f float, l bigint, s string, d double
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/bf_parquet.txt' OVERWRITE INTO TABLE bfTbl_staging;

INSERT OVERWRITE TABLE bfTbl SELECT * FROM bfTbl_staging;

SELECT * FROM bfTbl WHERE id=39;
SELECT * FROM bfTbl WHERE id=390;

SELECT * FROM bfTbl WHERE l=9082485254049298703;
SELECT * FROM bfTbl WHERE l=9082485254049298704;

SELECT * FROM bfTbl WHERE s="nop";
SELECT * FROM bfTbl WHERE s="bbb";

SELECT * FROM bfTbl WHERE d=520505137931974;
SELECT * FROM bfTbl WHERE d=520505137931971;