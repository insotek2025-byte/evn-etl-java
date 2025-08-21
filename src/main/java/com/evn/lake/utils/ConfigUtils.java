package com.evn.lake.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ConfigUtils {

    public static String catalog = "local";
    public static String schemaRaw = "raw_zone";
    public static String schemaGold = "gold_zone";

    public static ObjectMapper mapper = new ObjectMapper();


    public ConfigUtils() throws IOException {
    }
}
