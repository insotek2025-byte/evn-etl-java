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
import java.util.Objects;
import java.util.stream.Collectors;

import static com.evn.lake.utils.ConfigUtils.*;
import static com.evn.lake.utils.RawIceberg.genDdlCreateTableIceberg;
import static com.evn.lake.utils.RawIceberg.importCsv2Iceberg;

public class SimpleRaw2Gold {


    public static String raw2Gold(String tableNameInRaw, String tableNameInGold,  List<JobConfig.ColumnMapping> mapping){

        System.out.println(tableNameInRaw);
        List<String> selectExprs = mapping.stream()
                .map(row -> row.src_column + " as " + row.tar_column)
                .collect(Collectors.toList());
        System.out.println(selectExprs.toString());
        SparkSession spark = SparkUtils.getSession();
        Dataset<Row> sourceDf = spark.table(catalog + "." + schemaRaw + "."+ tableNameInRaw);
        Dataset<Row> targetDf = sourceDf.selectExpr(selectExprs.toArray(new String[0]));
        targetDf.writeTo(catalog + "."  +schemaGold+"." + tableNameInGold).createOrReplace();
        spark.stop();
        return tableNameInGold;

    }

    public static void etlRaw2Gold(String jobId, List<JobConfig> jobConfigs){
        JobConfig targetJob = jobConfigs.stream()
                .filter(j -> jobId.equals(j.job_id))
                .findFirst()
                .orElse(null);

        assert targetJob != null;
        raw2Gold(targetJob.src_table, targetJob.tar_table, targetJob.mapping);
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
        importCsv2Iceberg(targetJob.data,  targetJob.schema, targetJob.tar_table);
    }

    public static void main(String[] args) throws IOException {

        List<JobConfig> raw2GoldJobsConfig =  mapper.readValue(new File("config/job/config_job_raw2gold_mapping.json"), new TypeReference<List<JobConfig>>(){});

//        etlRaw2Gold("r2g_tcns_TCNS_thong_tin_nhan_su", raw2GoldJobsConfig);


    }

}
