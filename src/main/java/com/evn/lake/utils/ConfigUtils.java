package com.evn.lake.utils;

import com.evn.lake.entity.JobConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ConfigUtils {

    public static String schemaRaw = "raw_zone";
    public static String catalog = "local";
    public static String schemaGold = "gold_zone";

    static ObjectMapper mapper = new ObjectMapper();

    // Đọc JSON file thành List<JobConfig>
    public static List<JobConfig> jobs;

    static {
        try {
            jobs = mapper.readValue(
                    new File("config/job/config.json"), new TypeReference<List<JobConfig>>(){}
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ConfigUtils() throws IOException {
    }
}
