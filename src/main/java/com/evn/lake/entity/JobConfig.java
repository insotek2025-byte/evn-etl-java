package com.evn.lake.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobConfig {
    public String job_id;
    public String src_system;
    public String src_schema;
    public String src_table;
    public String tar_system;
    public String tar_schema;
    public String tar_table;
    public List<ColumnMapping> mapping;
    public String sql;
    public String align;
    public String mode;

    public String data;
    public String schema;


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ColumnMapping {
        public String src_column;
        public String tar_column;
        public String cast_from ;
        public String cast_to ;
        public String default_value;

    }
}



