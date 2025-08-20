package com.evn.lake.gold2mart;

import com.evn.lake.entity.JobConfig;
import com.evn.lake.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

import static com.evn.lake.utils.ConfigUtils.*;

public class Standard2Mart {

    public static String raw2Gold(String tableNameInGold, String tableNameInOracle,  List<JobConfig.ColumnMapping> mapping){


        SparkSession spark = SparkUtils.getSession();

        Dataset<Row> sourceDf = spark.table(catalog + "." + schemaGold + "."+ tableNameInGold);
//        Dataset<Row> targetDf = sourceDf.selectExpr(selectExprs.toArray(new String[0]));

        sourceDf.write()
                .mode("append")
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@//118.70.49.45:1521/orcl")
                .option("dbtable", tableNameInOracle +"test")
                .option("user", "envnpc_tochucnhansu")
                .option("password", "root123")
                .save();

        spark.stop();

        return tableNameInOracle;

    }

    public static void main(String[] args) {
        raw2Gold("TCNS_thong_tin_nhan_su", "TCNS_thong_tin_nhan_su", null);
    }

}
