package com.evn.lake.etl;

import com.evn.lake.entity.JobConfig;
import com.evn.lake.utils.ConfigUtils;
import com.evn.lake.utils.SparkUtils;
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
import java.util.List;
import java.util.stream.Collectors;

import static com.evn.lake.utils.ConfigUtils.mapper;
import static com.evn.lake.utils.MartDimFact.headTable;
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
        Dataset<Row> sourceDf = spark.table(targetJob.src_system + "." + targetJob.src_schema + "." + targetJob.src_table);
        System.out.println("Source schema");
        sourceDf.printSchema();
        sourceDf.show(3);
        Dataset<Row> destDf = spark.table(targetJob.tar_system + "." + targetJob.tar_schema + "." + targetJob.tar_table);
        System.out.println("Target schema");
        destDf.printSchema();

        Dataset<Row> targetDf;

        if (targetJob.mapping != null && !targetJob.mapping.isEmpty()) {
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
            System.out.println("Mapping rule" + selectExprs.toString());

            targetDf = sourceDf.selectExpr(selectExprs.toArray(new String[0]));

            System.out.println("Start write to " + targetJob.tar_system + "."  +targetJob.tar_schema+"."+ targetJob.tar_table);
            targetDf.write()
                    .format("iceberg")
                    .mode("overwrite").save(targetJob.tar_system + "." + targetJob.tar_schema + "." + targetJob.tar_table);

        } else if (targetJob.sql != null && !targetJob.sql.isEmpty()) {
            String sqlQuery = null;
            try {
                sqlQuery = new String(Files.readAllBytes(Paths.get(targetJob.sql)), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            targetDf = spark.sql(sqlQuery);
            System.out.println("Execute sql " + sqlQuery);
            targetDf.show(3);

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
        String sqlQuery = null;
        try {
            sqlQuery = new String(Files.readAllBytes(Paths.get(targetJob.sql)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SparkSession spark = SparkUtils.getSession();

        System.out.println("Execute sql " + sqlQuery);

        Dataset<Row> targetDf = spark.sql(sqlQuery);
        targetDf.show(3);

        System.out.println("write to" + targetJob.tar_table + targetJob.tar_schema);
        writeDf2Oracle(targetDf, targetJob.tar_schema, targetJob.tar_table);

        System.out.println("data after write");
        headTable( targetJob.tar_schema, targetJob.tar_table);

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

    public void etlAllRaw2GoldByLimitConfig(){
        Field[] fields = ConfigUtils.EtlTCNS.GoldTable.class.getDeclaredFields();
        for (Field field : fields) {
            if (field.getType().equals(String.class)) {
                try {
                    String jobId  = (String) field.get(null); // vì static nên get(null)
                    simpleZoneEtl(jobId);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
    }



}
