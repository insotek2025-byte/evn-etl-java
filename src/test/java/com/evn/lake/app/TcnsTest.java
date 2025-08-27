package com.evn.lake.app;

import com.evn.lake.etl.SimpleETL;
import com.evn.lake.utils.ConfigUtils;
import com.evn.lake.utils.MartDimFact;

import java.io.IOException;

import static com.evn.lake.utils.RawIceberg.genDataRawByName;
import static com.evn.lake.utils.RawIceberg.recreateTableIceberg;

public class TcnsTest {


    public static void test_D_TCNS_hop_dong(){
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlTCNS.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlTCNS.DimTable.D_TCNS_hop_dong);
    }

    public static void test_D_TCNS_danh_muc_vi_tri(){
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlTCNS.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlTCNS.DimTable.D_TCNS_danh_muc_vi_tri);
    }

    public static void test_D_TCNS_phong_ban(){
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlTCNS.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlTCNS.martDDLPath, ConfigUtils.EtlTCNS.DimTable.D_TCNS_phong_ban, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlTCNS.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlTCNS.DimTable.D_TCNS_phong_ban);

        martDimFact.closeConn();
    }

    public static void test_D_TCNS_Danh_Muc_Cong_Viec(){
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlTCNS.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlTCNS.DimTable.D_TCNS_Danh_Muc_Cong_Viec);
    }

    public static void test_D_TCNS_Nhan_su(){
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlTCNS.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlTCNS.DimTable.D_TCNS_Nhan_su);
    }

    public static void test_D_TCNS_Ngaythang(){
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlTCNS.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlTCNS.martDDLPath, ConfigUtils.EtlTCNS.DimTable.D_TCNS_Ngaythang, true);
        martDimFact.closeConn();
    }

    public static void test_F_TCNS_tong_so_hop_dong_ky_moi(){
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlTCNS.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlTCNS.martDDLPath, ConfigUtils.EtlTCNS.FactTable.F_TCNS_tong_so_hop_dong_ky_moi, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlTCNS.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlTCNS.FactTable.F_TCNS_tong_so_hop_dong_ky_moi);
        martDimFact.closeConn();
    }

    public static void test_D_TCNS_Don_vi(){
        MartDimFact martDimFact = new MartDimFact(ConfigUtils.EtlTCNS.oracleConfig);
        martDimFact.createTableOracle(ConfigUtils.EtlTCNS.martDDLPath, ConfigUtils.EtlTCNS.DimTable.D_TCNS_Don_vi, true);
        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlTCNS.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlTCNS.DimTable.D_TCNS_Don_vi);
        martDimFact.closeConn();
    }

    public static void test_TCNS_Ky_hop_dong() throws IOException {
        SimpleETL simpleRaw2Gold = new SimpleETL(ConfigUtils.EtlTCNS.etlRaw2GoldPath);
        genDataRawByName(ConfigUtils.EtlTCNS.RawTable.NS_HDLDONG, ConfigUtils.EtlTCNS.rawPath);
        genDataRawByName(ConfigUtils.EtlTCNS.RawTable.NS_QTLAMVIEC, ConfigUtils.EtlTCNS.rawPath);
        recreateTableIceberg(ConfigUtils.EtlTCNS.goldDDLPath, ConfigUtils.EtlTCNS.GoldTable.TCNS_Ky_hop_dong);
        simpleRaw2Gold.simpleZoneEtl(ConfigUtils.EtlTCNS.GoldTable.TCNS_Ky_hop_dong);
    }

    public static void main(String[] args) throws IOException {

        test_D_TCNS_Don_vi();

    }
}
