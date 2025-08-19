package com.evn.lake.utils;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;


public class SparkUtils  {


    public static SparkSession getSession(){
        return getSession("test", false, "dev");
    }


    public static SparkSession getSession(String nameJob, boolean log, String env) {
        SparkSession session;

        if (!log) {
            Logger.getLogger("org").setLevel(Level.OFF);
            Logger.getLogger("akka").setLevel(Level.OFF);
        }

        String catalog =  "local";
        String warehousePath = new java.io.File("iceberg/warehouse").getAbsolutePath();

        if(env == "dev"){
            System.out.println("init dev spark");
            session = SparkSession.builder()
                    .appName("CSV to Iceberg")
                    .master("local[2]")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                    .config("spark.sql.catalog.spark_catalog.type", "hive")

                    // Catalog local dùng Hadoop warehouse
                    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog.local.type", "hadoop")
                    .config("spark.sql.catalog.local.warehouse", "file://" + warehousePath)

                    // Default catalog
                    .config("spark.sql.defaultCatalog", "local")
                    .getOrCreate();
        }else {

            session = SparkSession
                    .builder()
                    .appName(nameJob)
                    .config("spark.debug.maxToStringFields", 100)
                    .config("spark.speculation", "true")
                    .config("spark.executor.extraJavaOptions", "-Xss512m")
                    .config("spark.sql.parquet.binaryAsString", "true")
                    .getOrCreate();
        }
        return session;
    }

}