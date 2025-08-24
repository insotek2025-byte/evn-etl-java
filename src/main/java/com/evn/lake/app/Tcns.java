package com.evn.lake.app;

import com.evn.lake.etl.SimpleETL;
import com.evn.lake.utils.ConfigUtils;

import java.io.IOException;

import static com.evn.lake.utils.RawIceberg.*;

public class Tcns {
    public static void main(String[] args) throws IOException {

        SimpleETL simpleRaw2Gold = new SimpleETL(ConfigUtils.EtlTCNS.etlPath);

       //  run all
//        genAllDataRaw(ConfigUtils.EtlTCNS.rawPath);
//        createTableIceberg(ConfigUtils.EtlTCNS.goldPath);
//        simpleRaw2Gold.etlAllRaw2GoldByLimitConfig();

         // run once
//        genDataRawByName(ConfigUtils.EtlTCNS.RawTable.S_ORGANIZATION, ConfigUtils.EtlTCNS.rawPath);
//        recreateTableIceberg(ConfigUtils.EtlTCNS.goldPath, ConfigUtils.EtlTCNS.GoldTable.TCNS_cay_don_vi);
//        simpleRaw2Gold.simpleEtlRaw2Gold(ConfigUtils.EtlTCNS.GoldTable.TCNS_cay_don_vi);

//        recreateTableIceberg(ConfigUtils.EtlTCNS.goldPath, ConfigUtils.EtlTCNS.GoldTable.TCNS_Ky_hop_dong);
        simpleRaw2Gold.simpleEtlRaw2Gold(ConfigUtils.EtlTCNS.GoldTable.TCNS_Ky_hop_dong);

//        createTableOracle(martPath, false);

    }
}
