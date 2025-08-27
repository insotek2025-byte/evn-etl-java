package com.evn.lake.etl;

import com.evn.lake.entity.JobConfig;
import com.evn.lake.utils.ConfigUtils;
import com.evn.lake.utils.MartDimFact;
import com.evn.lake.utils.SparkUtils;
import com.evn.lake.utils.SqlUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.evn.lake.utils.ConfigUtils.mapper;

import static com.evn.lake.utils.MartDimFact.writeDf2Oracle;

public class SimpleETL {

    List<JobConfig> jobConfigList;
    public SimpleETL(String etlPath){
        try {
            jobConfigList =  mapper.readValue(new File(etlPath), new TypeReference<List<JobConfig>>(){});

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public String etlZone2Zone(JobConfig targetJob) {

        System.out.println("Start process ETL from src table " + targetJob.src_table + " to table " + targetJob.tar_table);

        SparkSession spark = SparkUtils.getSession();
        Dataset<Row> destDf = spark.table(targetJob.tar_system + "." + targetJob.tar_schema + "." + targetJob.tar_table);
        System.out.println("Target schema");
        destDf.printSchema();

        Dataset<Row> targetDf;

        if (targetJob.mapping != null && !targetJob.mapping.isEmpty()) {

            Dataset<Row> sourceDf = spark.table(targetJob.src_system + "." + targetJob.src_schema + "." + targetJob.src_table);
            System.out.println("Source schema");
            sourceDf.printSchema();
            sourceDf.show(3);

            String[] select = SqlUtils.genSelectExps(targetJob);
            System.out.println("Mapping rule" + Arrays.toString(select));
            targetDf = sourceDf.selectExpr(select);
            System.out.println("Start write to " + targetJob.tar_system + "."  +targetJob.tar_schema+"."+ targetJob.tar_table);
            targetDf.write()
                    .format("iceberg")
                    .mode("overwrite").save(targetJob.tar_system + "." + targetJob.tar_schema + "." + targetJob.tar_table);

        } else if (targetJob.sql != null && !targetJob.sql.isEmpty()) {
            String sqlQuery = SqlUtils.getSqlSelect(targetJob.sql);
            targetDf = spark.sql(sqlQuery);
            System.out.println("Execute sql " + sqlQuery);
        } else {
            throw new RuntimeException("No mapping or SQL provided for targetJob: " + targetJob.job_id);
        }

        System.out.println("Read data after write to " + targetJob.tar_system + "."  +targetJob.tar_schema+"."+ targetJob.tar_table);
        Dataset<Row> check = spark.sql("SELECT * FROM " + targetJob.tar_system + "."  +targetJob.tar_schema+"."+ targetJob.tar_table + " limit 10" );
        check.show(3);
        spark.stop();
        return targetJob.src_schema;

    }


    public void etlZone2Mart(JobConfig targetJob) {

        // TODO: jinja support to replate pattern in sql
        SparkSession spark = SparkUtils.getSession();

        Dataset<Row> targetDf;

        if (targetJob.mapping != null && !targetJob.mapping.isEmpty()) {

            String[] select = SqlUtils.genSelectExps(targetJob);
            Dataset<Row> sourceDf = spark.table(targetJob.src_system + "." + targetJob.src_schema + "." + targetJob.src_table);
            System.out.println("Source data " + targetJob.src_system + "." + targetJob.src_schema + "." + targetJob.src_table);
            sourceDf.show(5);
            System.out.println("Mapping rule" + Arrays.toString(select));
            targetDf = sourceDf.selectExpr(select);

        } else if (targetJob.sql != null && !targetJob.sql.isEmpty()) {
            String sqlQuery = SqlUtils.getSqlSelect(targetJob.sql);
            System.out.println("Execute sql " + sqlQuery);
            targetDf = spark.sql(sqlQuery);
            targetDf.show(3);

        } else {
            throw new RuntimeException("No mapping or SQL provided for targetJob: " + targetJob.job_id);
        }

        targetDf.show(3);

        System.out.println("write to " + targetJob.tar_schema + "."+ targetJob.tar_table );
        writeDf2Oracle(targetDf, targetJob.tar_schema, targetJob.tar_table);

        System.out.println("data in oracle after write " + targetJob.tar_schema +"." + targetJob.tar_table);
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.headTable( targetJob.tar_schema, targetJob.tar_table);
        martDimFact.closeConn();

    }

    public JobConfig simpleZoneEtl(String jobId) {
        JobConfig targetJob = jobConfigList.stream()
                .filter(j -> jobId.equals(j.job_id))
                .findFirst()
                .orElse(null);

        assert targetJob != null;
        if(targetJob.tar_schema != null && targetJob.tar_schema.equals("gold_zone")){
            etlZone2Zone(targetJob);
        }else {
            throw new RuntimeException("target must be gold zone, but targetJob: "  + targetJob);
        }

        return targetJob;
    }

    public JobConfig simpleMartEtl(String jobId) {
        JobConfig targetJob = jobConfigList.stream()
                .filter(j -> jobId.equals(j.job_id))
                .findFirst()
                .orElse(null);

        assert targetJob != null;
        if(targetJob.tar_system != null && targetJob.tar_system.equals("oracle")){
            etlZone2Mart(targetJob);
        }else {
            throw new RuntimeException("tar_system must be oracle, but targetJob: "  + targetJob);
        }

        return targetJob;
    }

    public void etlAllRaw2GoldByFileConfig() {
        jobConfigList.forEach(this::etlZone2Zone);
    }



}
