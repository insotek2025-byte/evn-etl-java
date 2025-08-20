package com.evn.lake.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobConfig {
    public String job_id;
    public String src_table;
    public String tar_table;
    public String data;
    public String schema;
    public List<ColumnMapping> mapping;


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ColumnMapping {
        public String src_column;
        public String tar_column;

    }
}



