package com.evn.lake.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Column;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class ConvertCsv2Iceberg {

    public static class ColumnMeta {
        public String name;
        public String dataType;
        public Integer length;
        public boolean nullable;

        public ColumnMeta(String name, String dataType) {
            this.name = name;
            this.dataType = dataType;
        }
    }

    public static class SchemaResult {
        public StructType schema;
        public List<ColumnMeta> metas;

        public SchemaResult(StructType schema, List<ColumnMeta> metas) {
            this.schema = schema;
            this.metas = metas;
        }
    }

    // === Load schema từ file CSV ===
    public static SchemaResult loadSchemaFromCsv(String path) throws IOException {
        List<ColumnMeta> metas = new ArrayList<>();
        List<StructField> fields = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            boolean isHeader = true;
            while ((line = br.readLine()) != null) {
                if (isHeader) { // bỏ header
                    isHeader = false;
                    continue;
                }
                String[] cols = line.split("\\|", -1);
                String colName = cols[0].trim();
                String dataType = cols[1].trim();


                metas.add(new ColumnMeta(colName, dataType));

                DataType sparkType;
                switch (dataType.toLowerCase()) {
                    case "int":
                    case "integer":
                        sparkType = DataTypes.IntegerType;
                        break;
                    case "bigint":
                        sparkType = DataTypes.LongType;
                        break;
                    case "numeric":
                    case "decimal":
                        sparkType = DataTypes.LongType;
                        break;
                    case "float":
                        sparkType = DataTypes.FloatType;
                        break;
                    case "double":
                        sparkType = DataTypes.DoubleType;
                        break;
                    case "nvarchar":
                    case "varchar":
                    case "text":
                        sparkType = DataTypes.StringType;
                        break;
                    case "date":
                    case "timestamp":
                        // đọc tạm là String, sẽ convert sau
                        sparkType = DataTypes.StringType;
                        break;
                    default:
                        sparkType = DataTypes.StringType;
                }

                fields.add(DataTypes.createStructField(colName, sparkType, true));
            }
        }
        StructType schema = DataTypes.createStructType(fields);
        return new SchemaResult(schema, metas);
    }

    // === Convert date/timestamp cột về đúng kiểu sau khi đọc DataFrame ===
    public static Dataset<Row> convertDates(Dataset<Row> df, List<ColumnMeta> metas) {
        Dataset<Row> tmp = df;
        for (ColumnMeta meta : metas) {
            switch (meta.dataType.toLowerCase()) {
                case "date":
                    tmp = tmp.withColumn(meta.name,
                            functions.to_date(new Column(meta.name), "dd-MMM-yy"));
                    break;
                case "timestamp":
                    tmp = tmp.withColumn(meta.name,
                            functions.to_timestamp(new Column(meta.name), "dd-MMM-yy HH:mm:ss"));
                    break;
                default:
                    // không làm gì
            }
        }
        return tmp;
    }

    public static void csv2Iceberg(String inputPath,  String schemaPath, String tableNameInRaw) throws IOException {
        SparkSession spark = SparkUtils.getSession();

        SchemaResult schemaResult = loadSchemaFromCsv(schemaPath);

        Dataset<Row> dfRaw = spark.read()
                .option("header", "true")
                .option("inferSchema", "false")   // schema sẽ tự define
                .option("delimiter", "|")         // dấu phân tách cột
                .option("quote", "\"")            // giá trị bao trong dấu "
                .option("escape", "\"")           // escape cho dấu "
                .option("multiLine", "true")      // cho phép dữ liệu xuống dòng
                .schema(schemaResult.schema)
                .csv(inputPath);

        Dataset<Row> df = convertDates(dfRaw, schemaResult.metas);

        df.printSchema();
        df.show(3, true);

        df.writeTo("local.raw_zone."+tableNameInRaw).createOrReplace();

        Dataset<Row> check = spark.sql("SELECT * FROM local.raw_zone."+tableNameInRaw);
        check.show(false);

        spark.stop();
    }

    public static void testRead(String tableName){
        SparkSession spark = SparkUtils.getSession();
        Dataset<Row> check = spark.sql("SELECT * FROM local.raw_zone."+tableName);
        check.show(false);

        spark.stop();
    }



    // === Demo main ===
    public static void main(String[] args) throws IOException {


        String input = "data/05.HRMS_NPC/HRMS_NPC/dbo.NS_LLNS.data.csv";
        String schema = "data/05.HRMS_NPC/HRMS_NPC/dbo.NS_LLNS.schema.csv";

        csv2Iceberg(input,  schema, "NS_LLNS");

        testRead("NS_LLNS");


    }
}

