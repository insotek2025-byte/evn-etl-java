package com.evn.lake.utils;

import com.evn.lake.entity.JobConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ConfigUtils {

    public static String catalog = "local";
    public static String schemaRaw = "raw_zone";
    public static String schemaGold = "gold_zone";

    public static ObjectMapper mapper = new ObjectMapper();


    public ConfigUtils() throws IOException {
    }
}
