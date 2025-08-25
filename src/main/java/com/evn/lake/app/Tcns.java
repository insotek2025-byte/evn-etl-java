package com.evn.lake.app;

import com.evn.lake.etl.SimpleETL;
import com.evn.lake.utils.ConfigUtils;

import java.io.IOException;

public class Tcns {
    public static void main(String[] args) throws IOException {

        SimpleETL simpleRaw2Gold = new SimpleETL(ConfigUtils.EtlTCNS.etlRaw2GoldPath);

       //  run all
//        genAllDataRaw(ConfigUtils.EtlTCNS.rawPath);
//        createTableIceberg(ConfigUtils.EtlTCNS.goldPath);
//        simpleRaw2Gold.etlAllRaw2GoldByLimitConfig();

         // run once
//        genDataRawByName(ConfigUtils.EtlTCNS.RawTable.S_ORGANIZATION, ConfigUtils.EtlTCNS.rawPath);
//        recreateTableIceberg(ConfigUtils.EtlTCNS.goldPath, ConfigUtils.EtlTCNS.GoldTable.TCNS_cay_don_vi);
//        simpleRaw2Gold.simpleEtlRaw2Gold(ConfigUtils.EtlTCNS.GoldTable.TCNS_cay_don_vi);

//        recreateTableIceberg(ConfigUtils.EtlTCNS.goldPath, ConfigUtils.EtlTCNS.GoldTable.TCNS_Ky_hop_dong);
//        simpleRaw2Gold.simpleZoneEtl(ConfigUtils.EtlTCNS.GoldTable.TCNS_Co_cau_tuyen_dung);

//        createTableOracle(martDDLPath, false);

        SimpleETL simpleGold2Mart = new SimpleETL(ConfigUtils.EtlTCNS.etlGold2MartPath);
        simpleGold2Mart.simpleMartEtl(ConfigUtils.EtlTCNS.DimTable.D_TCNS_Nhan_su);

    }
}
