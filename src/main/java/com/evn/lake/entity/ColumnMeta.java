package com.evn.lake.entity;

public class ColumnMeta {
    public String name;
    public String dataType;

    public ColumnMeta(String name, String dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    @Override
    public String toString() {
        return String.format("{name='%s', dataType='%s'}", name, dataType);
    }
}
