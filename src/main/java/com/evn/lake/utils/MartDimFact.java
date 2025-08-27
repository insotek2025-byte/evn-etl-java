package com.evn.lake.utils;

import com.evn.lake.entity.JobConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static com.evn.lake.utils.ConfigUtils.catalog;
import static com.evn.lake.utils.ConfigUtils.schemaGold;

public class MartDimFact {

    Connection conn;

    public MartDimFact(String configPath){
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            // Load file .properties
            props.load(fis);

            String url = props.getProperty("url");
            String user = props.getProperty("user");
            String password = props.getProperty("password");

            System.out.println("Kết nối tới: " + url +" user: " +  user);
            // Load driver Oracle (nếu dùng JDBC 4 trở lên thì không bắt buộc)
//            Class.forName("oracle.jdbc.OracleDriver");

            // Kết nối DB
            conn = DriverManager.getConnection(url, user, password) ;
            System.out.println("Kết nối Oracle thành công!");

        } catch (IOException e) {
            System.err.println("Lỗi đọc file config: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeConn(){
        try {
            conn.close();
            System.out.println("Close Oracle thành công!");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean tableExists( String schema, String table) throws Exception {
        String sql = "SELECT COUNT(*) FROM all_tables WHERE owner = '" + schema.toUpperCase() +
                "' AND table_name = '" + table.toUpperCase() + "'";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    public void headTable(String schema, String table) {
        int headCount = 5;
        String sql = "SELECT * FROM " + schema + "." + table + " WHERE ROWNUM <=  " +headCount;

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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Tạo bảng Oracle từ JobConfig
     */
    public void createTable(Connection conn, JsonNode job, boolean dropIfExists) throws Exception {
        try (Statement stmt = conn.createStatement()) {

            String jobId = job.get("job_id").asText();
            String schema = job.get("schema").asText();
            String table = job.get("table").asText();
            boolean isTableExist = tableExists(schema, table );

            // nếu tồn tại + drop -> xóa bảng
            if (dropIfExists && isTableExist) {
                String dropSql = "DROP TABLE " + schema + "." + table;
                System.out.println("Dropping existing table: " + dropSql);
                stmt.execute(dropSql);
            }

            // nếu tồn tại + khong drop -> ignore
            if (isTableExist && !dropIfExists) {
                System.out.println("Table has exited " + table );
                return;
            }


            // Tao bang
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE TABLE ").append(schema).append(".").append(table).append(" (");

            Iterator<JsonNode> it = job.get("columns").elements();
            while (it.hasNext()) {
                JsonNode col = it.next();
                String colName = col.get("name").asText();
                String colType = col.get("type").asText();
                String optional = col.has("optional") ? col.get("optional").asText() : "";

                sql.append(colName).append(" ").append(colType).append(" ").append(optional);
                if (it.hasNext()) {
                    sql.append(", ");
                }
            }
            sql.append(")");

            System.out.println("Executing DDL: " + sql);
            stmt.execute(sql.toString());
            System.out.println("Table " + schema + "." + table + " created successfully.");

            String init = job.has("init") ? job.get("init").asText() : "";
            if(!Objects.equals(init, "")){
                String initSql = SqlUtils.getSqlSelect(init);
                System.out.println("Executing Init data: " + initSql);
                stmt.execute(initSql);
            }

        }
    }

    public  void createTableOracle(String pathConfig, String tableName, boolean dropIfExists) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root;
        try {
            root = mapper.readTree(new File(pathConfig));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try{
            for (JsonNode job : root) {
                if (tableName == null) {
                    createTable(conn, job, dropIfExists);
                } else if (tableName.equals(job.get("table").asText())) {
                    createTable(conn, job, dropIfExists);
                    return;
                }

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
                .option("truncate", "true")  // k biet tac dung jh
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@//118.70.49.45:1521/orcl")
                .option("dbtable", tableNameInOracle)
                .option("user", schema)
                .option("password", "root123")
                .save();
        return tableNameInOracle;
    }
}