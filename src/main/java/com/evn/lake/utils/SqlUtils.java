package com.evn.lake.utils;

import com.evn.lake.entity.JobConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class SqlUtils {

    public static String[] genSelectExps(JobConfig targetJob){
        List<String> selectExprs = targetJob.mapping.stream()
                .map(row -> {
                    if (row.src_column == null || row.src_column.isEmpty()) {
                        return row.default_value + " AS " + row.tar_column;
                    } else if (row.cast_to != null && !row.cast_to.isEmpty()) {
                        return "CAST(" + row.src_column + " AS " + row.cast_to + ") AS " + row.tar_column;
                    } else {
                        return row.src_column + " AS " + row.tar_column;
                    }
                })
                .collect(Collectors.toList());
        return selectExprs.toArray(new String[0]);
    }

    public static String getSqlSelect(String path){
        try {
            return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
