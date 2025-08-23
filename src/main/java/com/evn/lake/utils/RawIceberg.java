package com.evn.lake.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.evn.lake.utils.ConfigUtils.catalog;
import static com.evn.lake.utils.ConfigUtils.schemaRaw;

public class RawIceberg {

    public static class ColumnMeta {
        public String name;
        public String dataType;

        public ColumnMeta(String name, String dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        @Override
        public String toString() {
            return String.format("{name='%s', dataType='%s'}", name, dataType);
        }
    }

    public static class SchemaResult {
        public StructType schema;
        public List<ColumnMeta> metas;

        public SchemaResult(StructType schema, List<ColumnMeta> metas) {
            this.schema = schema;
            this.metas = metas;
        }

        @Override
        public String toString() {
            return String.format("SchemaResult{\nschema=%s,\nmetas=%s\n}", schema.prettyJson(), metas);
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
                    case "smallint":
                    case "tinyint":
                    case "bit":
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
                    case "smalldatetime":
                    case "datetime":
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
                case "smalldatetime":
                    tmp = tmp.withColumn(meta.name,
                            functions.to_timestamp(new Column(meta.name), "yyyy-MM-dd HH:mm"));
                    break;
                case "datetime":
                    tmp = tmp.withColumn(meta.name,
                            functions.to_timestamp(new Column(meta.name), "M/d/yyyy h:mm:ss a"));  // datetime have am/pm 12/28/2013 12:00:00 AM
                    break;
                default:
                    // không làm gì
            }
        }
        return tmp;
    }

    public static String genDropTableIceberg(JsonNode job) {

        String catalog = job.get("catalog").asText();
        String schema = job.get("schema").asText();
        String table = job.get("table").asText();

        return String.format(
                "DROP TABLE IF EXISTS %s.%s.%s ",
                catalog, schema, table
        );
    }

    public static String genDdlCreateTableIceberg(JsonNode job) {
        String jobId = job.get("job_id").asText();
        String catalog = job.get("catalog").asText();
        String schema = job.get("schema").asText();
        String table = job.get("table").asText();

        // Build phần column definitions
        StringBuilder cols = new StringBuilder();
        Iterator<JsonNode> it = job.get("columns").elements();
        while (it.hasNext()) {
            JsonNode col = it.next();
            String colName = col.get("name").asText();
            String colType = col.get("type").asText();
            cols.append(colName).append(" ").append(colType);
            if (it.hasNext()) {
                cols.append(", ");
            }
        }

        return String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s.%s (%s) USING iceberg",
                catalog, schema, table, cols.toString()
        );
    }

    public static void createTableIceberg(String pathConfig) {
        // 2. Đọc file JSON config
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        try {
            root = mapper.readTree(new File(pathConfig));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        SparkSession spark = SparkUtils.getSession();
        for (JsonNode job : root) {
            String ddl = genDdlCreateTableIceberg(job);
            System.out.println("Executing ddl: " + ddl);
            spark.sql(ddl);

        }
    }

    public static void recreateTableIceberg(String pathConfig, String table) {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        try {
            root = mapper.readTree(new File(pathConfig));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        SparkSession spark = SparkUtils.getSession();
        for (JsonNode job : root) {
            String ddl = genDdlCreateTableIceberg(job);

            if(table != null && job.get("job_id").asText().contains(table)){

                String del = genDropTableIceberg(job);
                System.out.println("Drop table: " + del);
                spark.sql(del);

                System.out.println("Executing ddl: " + ddl);
                spark.sql(ddl);
                return;
            }
        }
    }


    public static void importCsv2Iceberg(String dataPath, String schemaPath, String tableNameInRaw) {
        SparkSession spark = SparkUtils.getSession();

        SchemaResult schemaResult = null;
        try {
            schemaResult = loadSchemaFromCsv(schemaPath);
            System.out.println(schemaResult);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Gen Data Table" + tableNameInRaw);

        Dataset<Row> dfRaw = spark.read()
                .option("header", "true")
                .option("inferSchema", "false")   // schema sẽ tự define
                .option("delimiter", "|")         // dấu phân tách cột
                .option("quote", "\"")            // giá trị bao trong dấu "
                .option("escape", "\"")           // escape cho dấu "
                .option("multiLine", "true")      // cho phép dữ liệu xuống dòng
                .schema(schemaResult.schema)
                .csv(dataPath);

        System.out.println("data raw");
        dfRaw.show(3, true);

        Dataset<Row> df = convertDates(dfRaw, schemaResult.metas);
        System.out.println("write data to " + catalog + "." + schemaRaw + "." + tableNameInRaw);
        df.writeTo(catalog + "." + schemaRaw + "." + tableNameInRaw).createOrReplace();

        Dataset<Row> check = spark.sql("SELECT * FROM " + catalog + "." + schemaRaw + "." + tableNameInRaw);
        check.show(3, true);

        spark.stop();
    }

    public static void testRead(String tableName) {
        SparkSession spark = SparkUtils.getSession();
        Dataset<Row> check = spark.sql("SELECT * FROM local.raw_zone." + tableName);
        check.show(false);

        spark.stop();
    }


    // === Demo main ===
    public static void main(String[] args) throws IOException {


        String input = "data/05.HRMS_NPC/HRMS_NPC/dbo.NS_LLNS.data.csv";
        String schema = "data/05.HRMS_NPC/HRMS_NPC/dbo.NS_LLNS.schema.csv";

        importCsv2Iceberg(input, schema, "NS_LLNS");

        testRead("NS_LLNS");


    }
}

