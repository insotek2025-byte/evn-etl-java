package com.evn.lake.app;

import com.evn.lake.etl.SimpleETL;
import com.evn.lake.utils.ConfigUtils;

import java.io.IOException;

import static com.evn.lake.utils.RawIceberg.genAllDataRaw;
import static com.evn.lake.utils.RawIceberg.recreateTableIceberg;

public class KtvhTest {
    public static void main(String[] args) throws IOException {


        //  run all
        genAllDataRaw(ConfigUtils.EtlKTVH.rawPath);

//        SimpleETL simpleRaw2Gold = new SimpleETL(ConfigUtils.EtlTCNS.etlRaw2GoldPath);

//        recreateTableIceberg(ConfigUtils.EtlKTVH.goldPath, ConfigUtils.EtlTCNS.GoldTable.TCNS_Ky_hop_dong);
    }
}
