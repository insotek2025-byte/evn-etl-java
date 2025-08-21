package com.evn.lake.raw2gold;

import com.evn.lake.entity.JobConfig;
import com.evn.lake.utils.SparkUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.evn.lake.utils.ConfigUtils.mapper;
import static com.evn.lake.utils.RawIceberg.genDdlCreateTableIceberg;
import static com.evn.lake.utils.RawIceberg.importCsv2Iceberg;

public class SimpleRaw2Gold {


    public static String raw2Gold(JobConfig targetJob) {

        System.out.println("Start process ETL from src table " + targetJob.src_table + " to table " + targetJob.tar_table);
        List<String> selectExprs = targetJob.mapping.stream()
                .map(row -> row.src_column + " as " + row.tar_column)
                .collect(Collectors.toList());
        System.out.println("Mapping rule" + selectExprs.toString());
        SparkSession spark = SparkUtils.getSession();
        Dataset<Row> sourceDf = spark.table(targetJob.src_system + "." + targetJob.src_schema + "." + targetJob.src_table);
        System.out.println("Source schema");
        sourceDf.printSchema();
        sourceDf.show(3);
        Dataset<Row> destDf = spark.table(targetJob.tar_system + "." + targetJob.tar_schema + "." + targetJob.tar_table);
        System.out.println("Target schema");
        destDf.printSchema();
        Dataset<Row> targetDf = sourceDf.selectExpr(selectExprs.toArray(new String[0]));
        targetDf.write()
                .format("iceberg")
                .mode("overwrite").save(targetJob.tar_system + "." + targetJob.tar_schema + "." + targetJob.tar_table);
        spark.stop();
        return targetJob.src_schema;

    }

    public static JobConfig etlRaw2Gold(String jobId, List<JobConfig> jobConfigs) {
        JobConfig targetJob = jobConfigs.stream()
                .filter(j -> jobId.equals(j.job_id))
                .findFirst()
                .orElse(null);

        assert targetJob != null;
        raw2Gold(targetJob);
        return targetJob;
    }

    public static void genDataRawByName(String jobId, List<JobConfig> jobConfigs) throws IOException {
        JobConfig targetJob = jobConfigs.stream()
                .filter(j -> jobId.equals(j.job_id))
                .findFirst()
                .orElse(null);

        assert targetJob != null;
        genDataRaw(targetJob);

    }


    public static void genDataRaw(JobConfig targetJob) {

        importCsv2Iceberg(targetJob.data, targetJob.schema, targetJob.tar_table);
    }

    public static void main(String[] args) throws IOException {
//         bước 1 tạo bảng gold . idempotence
        genDdlCreateTableIceberg("config/gold/gold_ddl.json");

//         bước 2 tạo bảng raw. idempoten
        List<JobConfig> rawTableConfig = mapper.readValue(new File("config/raw/gen_data_raw.json"), new TypeReference<List<JobConfig>>() {
        });
        rawTableConfig.forEach(SimpleRaw2Gold::genDataRaw);

        // bước 3 etl to gold
        List<JobConfig> raw2GoldJobsConfig = mapper.readValue(new File("config/gold/raw2gold.json"), new TypeReference<List<JobConfig>>() {
        });
    }

}
