# evn-etl-java


# prerun 

- mkdir data and move data into

```declarative
long@hello:~/IdeaProjects/evn-etl-java$ tree data -L 2
data
├── 05.HRMS_NPC
│      └── HRMS_NPC
├── 1.Imis1
│      └── 1. Imis1
├── 3.Pmis
│      ├── PMIS_EVNNPC
│      ├── PMIS_EVNNPC_Attach
│      ├── PMIS_EVNNPC_WAREHOUSE
│      ├── PMIS_GENDATA
│      └── PMIS_TICHHOP
├── 4.Oms
│      └── oms_output_data
└── 9.CMIS
    └── 10.CMIS

15 directories, 0 files
```

- mkdir iceberg (only mkdir, data will bo gen by spark local mode)

```declarative
long@hello:~/IdeaProjects/evn-etl-java$ tree iceberg -L 4
iceberg
└── warehouse
    └── raw_zone
        └── NS_LLNS
            ├── data
            └── metadata

```

# run

- config mapping data simple in config/std/tcns/TCNS_thong_tin_nhan_su.csv
- run 

```declarative
/home/long/Downloads/spark-3.1.1-bin-hadoop3.2/bin/spark-sql \
  --master local[2] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:0.14.0 \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=file:///home/long/IdeaProjects/evn-etl-java/iceberg/warehouse \
  --conf spark.sql.defaultCatalog=local \
  --conf spark.sql.cli.print.header=true

```

```sqlite-psql
spark-sql> SHOW CREATE TABLE local.raw_zone.Vie_CHUCDANH;
Error in query: SHOW CREATE TABLE is not supported for v2 tables.
spark-sql> DESCRIBE TABLE local.raw_zone.Vie_CHUCDANH;
VTRI_ID	bigint	
DONVI_ID	int	
TEN_VTRICDANH	string	
CDANH_QLY	string	
STT	string	
MOTA	string	
		
# Partitioning		
Not partitioned		
Time taken: 0.24 seconds, Fetched 9 row(s)
spark-sql> SELECT * FROM local.raw_zone.Vie_CHUCDANH.history;
2025-08-21 23:03:41.135	2699276928240967639	NULL	true
Time taken: 0.18 seconds, Fetched 1 row(s)
spark-sql> SELECT * FROM local.raw_zone.Vie_CHUCDANH.files;
0	file:/home/long/IdeaProjects/evn-etl-java/iceberg/warehouse/raw_zone/Vie_CHUCDANH/data/00000-0-d5e308ba-2f1b-4ac9-9a72-ff90f4b8b50d-00001.parquet	PARQUET	0	582	10098{1:1413,2:306,3:5080,4:187,5:814,6:653}	{1:582,2:582,3:582,4:582,5:582,6:582}	{1:0,2:0,3:0,4:0,5:22,6:398}	{}	{1:Иԯq,2:},3: Phó Trưởng ban ,4:False,5:1,6:01_CV}	{1:bp_�ѝ,2:�,3:Đội trưởng/đội q,4:True,5:999,6:ph_CD}	NULL	[4]	NULL	0
Time taken: 0.258 seconds, Fetched 1 row(s)

```