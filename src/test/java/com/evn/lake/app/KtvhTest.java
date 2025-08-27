package com.evn.lake.app;

import com.evn.lake.etl.SimpleETL;
import com.evn.lake.utils.ConfigUtils;

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
        Field[] fields = ConfigUtils.EtlKTVH.GoldTable.class.getDeclaredFields();
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

    public static void main(String[] args) throws IOException {


        //  run all
//        genAllDataRaw(ConfigUtils.EtlKTVH.rawPath);
//        createTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath);
//        recreateTableIceberg(ConfigUtils.EtlKTVH.goldDDLPath, ConfigUtils.EtlKTVH.GoldTable.KTVH_Do_tin_cay_loai_mat_dien);

        etlAllRaw2GoldByLimitConfig();


    }
}
