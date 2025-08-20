package com.evn.lake.tcns.raw2gold;

import com.evn.lake.entity.JobConfig;
import com.evn.lake.utils.SparkUtils;
import com.evn.lake.utils.StandardETL;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static com.evn.lake.utils.ConfigUtils.*;
import static com.evn.lake.utils.ConvertCsv2Iceberg.csv2Iceberg;

public class StandardTableTest {

    public static void testGenData(JobConfig targetJob) throws IOException {
        csv2Iceberg(targetJob.data,  targetJob.schema, targetJob.tar_table);
    }

    public static void testRaw2GoldStd(JobConfig targetJob){

        String tableNameInGold = StandardETL.raw2Gold(targetJob.src_table, targetJob.tar_table , targetJob.mapping);
        SparkSession spark = SparkUtils.getSession();
        Dataset<Row> check = spark.sql("SELECT * FROM " + catalog + "."  +schemaGold+"." + tableNameInGold );
        check.show(false);
    }

    public static void test( String jobId) throws IOException {
        // Lấy ra job có id = r2g_tcns_TCNS_thong_tin_nhan_su
        JobConfig targetJob = jobs.stream()
                .filter(j -> jobId.equals(j.job_id))
                .findFirst()
                .orElse(null);

        assert targetJob != null;

        if (Objects.equals(jobId.split("_")[0], "raw")){
            testGenData(targetJob);
        } else if (Objects.equals(jobId.split("_")[0], "r2g")) {
            testRaw2GoldStd(targetJob);
        } else {
            return;
        }

    }

    public static void main(String[] args) throws IOException {

        test("raw_NS_LLNS");
        test("r2g_tcns_TCNS_thong_tin_nhan_su");

    }
}
