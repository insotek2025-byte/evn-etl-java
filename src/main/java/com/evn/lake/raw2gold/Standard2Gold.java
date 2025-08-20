package com.evn.lake.raw2gold;

import com.evn.lake.entity.JobConfig;
import com.evn.lake.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

import static com.evn.lake.utils.ConfigUtils.*;

public class Standard2Gold {

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

    public static void main(String[] args) {

    }

}
