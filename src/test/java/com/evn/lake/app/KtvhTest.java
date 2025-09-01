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

    public static void test_F_KTVH_SL_QuanTracMoiTruong() {
        // Create Oracle table for mart layer
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_SL_QuanTracMoiTruong, true);
        
        // Run ETL from Gold to Mart
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_SL_QuanTracMoiTruong);
        
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_PhienLamViec() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_PhienLamViec, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_PhienLamViec);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_TH_Phien_VeSinhCachDien_SNAPSHOT() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_TH_Phien_VeSinhCachDien_SNAPSHOT, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_TH_Phien_VeSinhCachDien_SNAPSHOT);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_TH_Phien_SuaChuaHotline_SNAPSHOT() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_TH_Phien_SuaChuaHotline_SNAPSHOT, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_TH_Phien_SuaChuaHotline_SNAPSHOT);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_SuCoTrungAp_TRANSACTION() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_SuCoTrungAp_TRANSACTION, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_SuCoTrungAp_TRANSACTION);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_SoVuSuCo_TheoLoai_SNAPSHOT() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_SoVuSuCo_TheoLoai_SNAPSHOT, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_SoVuSuCo_TheoLoai_SNAPSHOT);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_SoVuSuCo_TheoTaiSan_SNAPSHOT() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_SoVuSuCo_TheoTaiSan_SNAPSHOT, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_SoVuSuCo_TheoTaiSan_SNAPSHOT);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_SoVuSuCo_NguyenNhanHanhLang_SNAPSHOT() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_SoVuSuCo_NguyenNhanHanhLang_SNAPSHOT, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_SoVuSuCo_NguyenNhanHanhLang_SNAPSHOT);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_ChiTieuSuCo_TRANSACTION() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_ChiTieuSuCo_TRANSACTION, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_ChiTieuSuCo_TRANSACTION);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_ViPham_HLATLD_SNAPSHOT() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_ViPham_HLATLD_SNAPSHOT, true);
//        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
//        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_ViPham_HLATLD_SNAPSHOT);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_DTCCCD_TRANSACTION() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_DTCCCD_TRANSACTION, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_DTCCCD_TRANSACTION);
        martDimFact.closeConn();
    }

    public static void test_F_KTVH_MatDienKhachHang_TRANSACTION() {
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, ConfigUtils.EtlKTVH.FactTable.F_KTVH_MatDienKhachHang_TRANSACTION, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlKTVH.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlKTVH.FactTable.F_KTVH_MatDienKhachHang_TRANSACTION);
        martDimFact.closeConn();
    }

    public static void test_KTVH_Duong_Day_from_ZAG_0000D40_DZ() {

        recreateTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath, "KTVH_Duong_Day");
        SimpleETL simpleRaw2Gold = new SimpleETL(ConfigUtils.EtlKTVH.etlRaw2GoldPath);
        simpleRaw2Gold.simpleZoneEtl(ConfigUtils.EtlKTVH.GoldTableJobId.KTVH_Duong_Day_from_ZAG_0000D40_DZ);

    }

    public static void test_KTVH_Tu_bu() throws IOException {
        genDataRawByName("RPT_CAPACITOR_OPERATION", ConfigUtils.EtlKTVH.rawPath);
        SimpleETL simpleRaw2Gold = new SimpleETL(ConfigUtils.EtlKTVH.etlRaw2GoldPath);
        recreateTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath, ConfigUtils.EtlKTVH.GoldTableJobId.KTVH_Tu_bu);
        simpleRaw2Gold.simpleZoneEtl(ConfigUtils.EtlKTVH.GoldTableJobId.KTVH_Tu_bu);
    }

    public static void test_KTVH_KL_THIET_BI_LUOI() throws IOException {
        genDataRawByName(ConfigUtils.EtlKTVH.RawTable.RPT_TBL_THONG_KE_THIET_BI_TREN_LUOI_BIEU_10_B4, ConfigUtils.EtlKTVH.rawPath);
        SimpleETL simpleRaw2Gold = new SimpleETL(ConfigUtils.EtlKTVH.etlRaw2GoldPath);
        recreateTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath, ConfigUtils.EtlKTVH.GoldTableJobId.KTVH_KL_THIET_BI_LUOI);
        simpleRaw2Gold.simpleZoneEtl(ConfigUtils.EtlKTVH.GoldTableJobId.KTVH_KL_THIET_BI_LUOI);
    }

    public static void main(String[] args) throws IOException {

        // Test F_KTVH_SL_TBNN_SNAPSHOT ETL
//        test_D_KTVH_Thoigian();
//        test_F_KTVH_SL_QuanTracMoiTruong();
//        test_F_KTVH_PhienLamViec();
//        test_F_KTVH_TH_Phien_VeSinhCachDien_SNAPSHOT();
//        test_F_KTVH_TH_Phien_SuaChuaHotline_SNAPSHOT();
//        test_F_KTVH_SuCoTrungAp_TRANSACTION();
//        test_F_KTVH_SoVuSuCo_TheoLoai_SNAPSHOT();
//        test_F_KTVH_SoVuSuCo_TheoTaiSan_SNAPSHOT();
//        test_F_KTVH_SoVuSuCo_NguyenNhanHanhLang_SNAPSHOT();
//        test_F_KTVH_ChiTieuSuCo_TRANSACTION();
//        test_F_KTVH_ViPham_HLATLD_SNAPSHOT();
//        test_F_KTVH_DTCCCD_TRANSACTION();
//        test_F_KTVH_MatDienKhachHang_TRANSACTION();

//        test_KTVH_Duong_Day_from_ZAG_0000D40_DZ();
        test_KTVH_KL_THIET_BI_LUOI();
        //  run all
//        genAllDataRaw(ConfigUtils.EtlKTVH.rawPath);
//        createTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath);

//        recreateTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath, "KTVH_May_Bien_Ap");
//        etlAllRaw2GoldByLimitConfig();

//        test_KTVH_Duong_Day();
//        test_KTVH_Tu_bu();
//        test_KTVH_KL_THIET_BI_LUOI();

//        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlKTVH.oracleConfig);
//        martDimFact.createTableOracle(ConfigUtils.EtlKTVH.martDDLPath, null, true);
//        martDimFact.closeConn();

    }
}
