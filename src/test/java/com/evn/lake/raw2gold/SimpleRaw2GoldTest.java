package com.evn.lake.raw2gold;

import com.evn.lake.entity.JobConfig;
import com.evn.lake.utils.SparkUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.evn.lake.raw2gold.SimpleRaw2Gold.genDataRawByName;
import static com.evn.lake.utils.ConfigUtils.mapper;
import static com.evn.lake.utils.RawIceberg.createTableIceberg;
import static com.evn.lake.utils.RawIceberg.recreateTableIceberg;

public class SimpleRaw2GoldTest {


    public static void testRaw2GoldStd(String jobId, List<JobConfig> raw2GoldJobsConfig){
        JobConfig targetJob = SimpleRaw2Gold.etlRaw2Gold(jobId, raw2GoldJobsConfig);
        SparkSession spark = SparkUtils.getSession();
        Dataset<Row> check = spark.sql("SELECT * FROM " + targetJob.tar_system + "."  +targetJob.tar_schema+"."+ targetJob.tar_table + " limit 10" );
        check.show(false);
    }


    public static void main(String[] args) throws IOException {
        // bước 1 tạo bảng gold . idempotence
//        createTableIceberg("config/gold/gold_ddl.json");

        // bước 2 tạo bảng raw. idempoten
        List<JobConfig> rawTableConfig =  mapper.readValue(new File("config/raw/gen_data_raw.json"), new TypeReference<List<JobConfig>>(){});
//        rawTableConfig.forEach(SimpleRaw2Gold::genDataRaw);

        // bước 3 etl to gold
        List<JobConfig> raw2GoldJobsConfig =  mapper.readValue(new File("config/gold/raw2gold.json"), new TypeReference<List<JobConfig>>(){});
//        testRaw2GoldStd("r2g_tcns_TCNS_cay_don_vi", raw2GoldJobsConfig);
//        testRaw2GoldStd("r2g_tcns_TCNS_thong_tin_nhan_su", raw2GoldJobsConfig);

        // 6. TCNS_BC_Chi_Tieu_EVN02
//        recreateTableIceberg("config/gold/gold_ddl.json", "TCNS_BC_Chi_Tieu_EVN02");
//        genDataRawByName("raw_BC_EVN02b_gui", rawTableConfig);
//        testRaw2GoldStd("r2g_tcns_TCNS_BC_Chi_Tieu_EVN02", raw2GoldJobsConfig);

        // 7. TCNS_hop_dong
//        recreateTableIceberg("config/gold/gold_ddl.json", "create_TCNS_hop_dong");
//        genDataRawByName("raw_NS_HDLDONG", rawTableConfig);
//        testRaw2GoldStd("r2g_tcns_TCNS_hop_dong", raw2GoldJobsConfig);

        // 8.
//        recreateTableIceberg("config/gold/gold_ddl.json", "create_TCNS_BC_Chi_Tieu_EVN02");
//        genDataRawByName("raw_BC_EVN01_gui", rawTableConfig);
//        testRaw2GoldStd("r2g_tcns_TCNS_bao_cao_chi_tieu_evn01", raw2GoldJobsConfig);

        recreateTableIceberg("config/gold/gold_ddl.json", "create_TCNS_ky_kham_suc_khoe");
        testRaw2GoldStd("r2g_tcns_TCNS_ky_kham_suc_khoe", raw2GoldJobsConfig);





    }

}
