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

import static com.evn.lake.raw2gold.SimpleRaw2Gold.genDataRaw;
import static com.evn.lake.utils.ConfigUtils.*;
import static com.evn.lake.utils.RawIceberg.genDdlCreateTableIceberg;
import static com.evn.lake.utils.RawIceberg.importCsv2Iceberg;

public class SimpleRaw2GoldTest {



    public static void testRaw2GoldStd(JobConfig targetJob){
        String tableNameInGold = SimpleRaw2Gold.raw2Gold(targetJob.src_table, targetJob.tar_table , targetJob.mapping);
        SparkSession spark = SparkUtils.getSession();
        Dataset<Row> check = spark.sql("SELECT * FROM " + catalog + "."  +schemaGold+"." + tableNameInGold );
        check.show(false);
    }


    public static void main(String[] args) throws IOException {
        //        genDdlCreateTableIceberg("config/gold/gold_ddl.json");
        List<JobConfig> rawTableConfig =  mapper.readValue(new File("config/raw/gen_data_raw.json"), new TypeReference<List<JobConfig>>(){});
        rawTableConfig.forEach(SimpleRaw2Gold::genDataRaw);

    }

}
