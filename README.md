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