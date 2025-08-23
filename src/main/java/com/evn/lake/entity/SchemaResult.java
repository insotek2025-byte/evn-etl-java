package com.evn.lake.entity;

import org.apache.spark.sql.types.StructType;

import java.util.List;

public class SchemaResult {
    public StructType schema;
    public List<ColumnMeta> metas;

    public SchemaResult(StructType schema, List<ColumnMeta> metas) {
        this.schema = schema;
        this.metas = metas;
    }

    @Override
    public String toString() {
        return String.format("SchemaResult{\nschema=%s,\nmetas=%s\n}", schema.prettyJson(), metas);
    }
}