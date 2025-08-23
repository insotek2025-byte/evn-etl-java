package com.evn.lake.app;

import com.evn.lake.raw2gold.SimpleRaw2Gold;
import com.evn.lake.utils.ConfigUtils;

import java.io.IOException;

import static com.evn.lake.utils.RawIceberg.createTableIceberg;
import static com.evn.lake.utils.RawIceberg.genAllDataRaw;

public class Tcns {
    public static void main(String[] args) throws IOException {

        SimpleRaw2Gold simpleRaw2Gold = new SimpleRaw2Gold(ConfigUtils.EtlTCNS.etlPath);

        // run all
//        genAllDataRaw(ConfigUtils.EtlTCNS.rawPath);
//        createTableIceberg(ConfigUtils.EtlTCNS.goldPath);
        simpleRaw2Gold.etlAllRaw2GoldByLimitConfig();

        // run once
//        genDataRawByName(ConfigUtils.EtlTCNS.RawTable.S_ORGANIZATION, ConfigUtils.EtlTCNS.rawPath);
//        recreateTableIceberg(ConfigUtils.EtlTCNS.goldPath, ConfigUtils.EtlTCNS.GoldTable.TCNS_cay_don_vi);
//        simpleRaw2Gold.etlRaw2Gold(ConfigUtils.EtlTCNS.GoldTable.TCNS_cay_don_vi);

    }
}
