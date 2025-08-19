package com.evn.lake.entity;


import com.opencsv.CSVReader;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;


public class MappingRaw2Gold {
    public String targetField;
    public String system;
    public String schema;
    public String table;
    public String sourceField;

    public MappingRaw2Gold(String targetField, String system, String schema, String table, String sourceField) {
        this.targetField = targetField;
        this.system = system;
        this.schema = schema;
        this.table = table;
        this.sourceField = sourceField;
    }

    @Override
    public String toString() {
        return String.format("Target: %s  <-  %s.%s.%s.%s",
                targetField, system, schema, table, sourceField);
    }




    public static List<MappingRaw2Gold> readMapping(String filePath) {
        List<MappingRaw2Gold> mappings = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> rows = reader.readAll();

            // bỏ header
            for (int i = 1; i < rows.size(); i++) {
                String[] row = rows.get(i);
                mappings.add(new MappingRaw2Gold(row[0], row[1], row[2], row[3], row[4]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mappings;
    }

}
