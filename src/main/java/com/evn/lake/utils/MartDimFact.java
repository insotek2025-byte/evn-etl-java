package com.evn.lake.utils;

import com.evn.lake.entity.JobConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;

import static com.evn.lake.utils.ConfigUtils.EtlTCNS.url;
import static com.evn.lake.utils.ConfigUtils.EtlTCNS.user;
import static com.evn.lake.utils.ConfigUtils.EtlTCNS.password;

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
}
