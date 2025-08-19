package com.evn.lake.tcns.raw2gold;

import com.evn.lake.entity.MappingRaw2Gold;
import com.evn.lake.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

import static com.evn.lake.entity.MappingRaw2Gold.readMapping;

public class StandardTable {

    private static void raw2Gold(){
        List<MappingRaw2Gold> mappings = readMapping("config/std/tcns/TCNS_thong_tin_nhan_su.csv");

        String schemaInRaw = "raw_zone";
        String tableNameInRaw = mappings.get(0).table;

        System.out.println(tableNameInRaw);

        String schemaGold = "gold_zone";
        String tableNameInGold = "TCNS_thong_tin_nhan_su";
        String catalog = "local";

        List<String> selectExprs = mappings.stream()
                .map(row -> row.sourceField + " as " + row.targetField)
                .collect(Collectors.toList());

        System.out.println(selectExprs.toString());

        SparkSession spark = SparkUtils.getSession();

        Dataset<Row> sourceDf = spark.table(catalog + "." + schemaInRaw + "."+ tableNameInRaw);
        Dataset<Row> targetDf = sourceDf.selectExpr(selectExprs.toArray(new String[0]));
        targetDf.writeTo(catalog + "."  +schemaGold+"." + tableNameInGold).createOrReplace();

        spark.stop();

        spark = SparkUtils.getSession();
        Dataset<Row> check = spark.sql("SELECT * FROM " + catalog + "."  +schemaGold+"." + tableNameInGold );
        check.show(false);

        spark.stop();

    }

    public static void main(String[] args) {
        raw2Gold();
    }

}
