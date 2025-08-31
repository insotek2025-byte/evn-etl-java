package com.evn.lake.app;

import com.evn.lake.etl.SimpleETL;
import com.evn.lake.utils.ConfigUtils;
import com.evn.lake.utils.MartDimFact;

import java.io.IOException;
import java.lang.reflect.Field;

import static com.evn.lake.utils.RawIceberg.*;


public class KtvhTest {


    public static void test_KTVH_Phien_lam_viec_PhieuCongTac() throws IOException {
//        genDataRawByName(ConfigUtils.EtlKTVH.RawTable.plv_PhieuCongTac, ConfigUtils.EtlKTVH.rawPath);
//        SimpleETL simpleRaw2Gold = new SimpleETL(ConfigUtils.EtlKTVH.etlRaw2GoldPath);
//        recreateTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath, ConfigUtils.EtlKTVH.GoldTable.KTVH_Phien_lam_viec_PhieuCongTac);
//        simpleRaw2Gold.simpleZoneEtl(ConfigUtils.EtlKTVH.GoldTable.KTVH_Phien_lam_viec_PhieuCongTac);
    }

    public static void etlAllRaw2GoldByLimitConfig(){
        Field[] fields = ConfigUtils.EtlKTVH.GoldTableJobId.class.getDeclaredFields();
        SimpleETL simpleRaw2Gold = new SimpleETL(ConfigUtils.EtlKTVH.etlRaw2GoldPath);
        for (Field field : fields) {
            if (field.getType().equals(String.class)) {
                try {
                    String jobId  = (String) field.get(null); // vì static nên get(null)
                    System.out.println("run etl gold" + jobId);
                    simpleRaw2Gold.simpleZoneEtl(jobId);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void test_F_KTVH_SL_TBNN_SNAPSHOT() {
        // Create Oracle table for mart layer
//        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
//        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_SL_TBNN_SNAPSHOT, true);
        
        // Run ETL from Gold to Mart
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_SL_TBNN_SNAPSHOT);
        
//        martDimFact.closeConn();
    }

    public static void test_D_KTVH_Thoigian() {
        // Create Oracle table for mart layer
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.DimTable.D_KTVH_Thoigian, true);
        
        martDimFact.closeConn();
    }

    public static void main(String[] args) throws IOException {

        // Test F_KTVH_SL_TBNN_SNAPSHOT ETL
        test_F_KTVH_SL_TBNN_SNAPSHOT();

        //  run all
//        genAllDataRaw(ConfigUtils.EtlKTVH.rawPath);
//        createTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath);

//        recreateTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath, "KTVH_May_Bien_Ap");
//        etlAllRaw2GoldByLimitConfig();

//        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
//        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, null, true);
//        martDimFact.closeConn();

    }
}
