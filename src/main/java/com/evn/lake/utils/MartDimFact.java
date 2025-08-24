package com.evn.lake.utils;

import com.evn.lake.entity.JobConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import java.util.List;

import static com.evn.lake.utils.ConfigUtils.EtlTCNS.url;
import static com.evn.lake.utils.ConfigUtils.EtlTCNS.user;
import static com.evn.lake.utils.ConfigUtils.EtlTCNS.password;
import static com.evn.lake.utils.ConfigUtils.catalog;
import static com.evn.lake.utils.ConfigUtils.schemaGold;

public class MartDimFact {

    public static boolean tableExists(Connection conn, String schema, String table) throws Exception {
        String sql = "SELECT COUNT(*) FROM all_tables WHERE owner = '" + schema.toUpperCase() +
                "' AND table_name = '" + table.toUpperCase() + "'";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    public static void headTable(String schema, String table) {
        int headCount = 5;
        String sql = "SELECT * FROM " + schema + "." + table + " WHERE ROWNUM <=  " +headCount;
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                int rowNum = 0;
                while (rs.next() && rowNum < headCount) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(meta.getColumnName(i) + "=" + rs.getString(i) + " ");
                    }
                    System.out.println();
                    rowNum++;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * Tạo bảng Oracle từ JobConfig
     */
    public static void createTable(Connection conn, JsonNode job, boolean dropIfExists) throws Exception {
        try (Statement stmt = conn.createStatement()) {

            String jobId = job.get("job_id").asText();
            String schema = job.get("schema").asText();
            String table = job.get("table").asText();
            boolean isTableExist = tableExists(conn, schema, table );

            // Drop nếu tồn tại
            if (dropIfExists && isTableExist) {
                String dropSql = "DROP TABLE " + schema + "." + table;
                System.out.println("Dropping existing table: " + dropSql);
                stmt.execute(dropSql);
            } else if (!isTableExist) {

                // Build câu lệnh CREATE TABLE
                StringBuilder sql = new StringBuilder();
                sql.append("CREATE TABLE ").append(schema).append(".").append(table).append(" (");

                Iterator<JsonNode> it = job.get("columns").elements();
                while (it.hasNext()) {
                    JsonNode col = it.next();
                    String colName = col.get("name").asText();
                    String colType = col.get("type").asText();

                    sql.append(colName).append(" ").append(colType);
                    if (it.hasNext()) {
                        sql.append(", ");
                    }
                }
                sql.append(")");

                System.out.println("Executing: " + sql);
                stmt.execute(sql.toString());
                System.out.println("Table " + schema + "." + table + " created successfully.");
            }else {
                System.out.println("Table " + schema + "." + table + " has Existed");
            }
        }
    }

    public static void createTableOracle(String pathConfig, boolean dropIfExists) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root;
        try {
            root = mapper.readTree(new File(pathConfig));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (Connection conn = DriverManager.getConnection(url, user, password)) {

            for (JsonNode job : root) {
                createTable(conn, job, dropIfExists);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static String writeFromLake2Oracle(String tableNameInGold, String tableNameInOracle, List<JobConfig.ColumnMapping> mapping) {
        SparkSession spark = SparkUtils.getSession();
        Dataset<Row> sourceDf = spark.table(catalog + "." + schemaGold + "." + tableNameInGold);
//        Dataset<Row> targetDf = sourceDf.selectExpr(selectExprs.toArray(new String[0]));

        sourceDf.write()
                .mode("append")
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@//118.70.49.45:1521/orcl")
                .option("dbtable", tableNameInOracle)
                .option("user", "envnpc_tochucnhansu2")
                .option("password", "root123")
                .save();
        spark.stop();
        return tableNameInOracle;

    }

    public static String writeDf2Oracle(Dataset<Row> sourceDf , String schema,  String tableNameInOracle) {
        sourceDf.write()
                .mode("overwrite")
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@//118.70.49.45:1521/orcl")
                .option("dbtable", tableNameInOracle)
                .option("user", schema)
                .option("password", "root123")
                .save();
        return tableNameInOracle;
    }
}
