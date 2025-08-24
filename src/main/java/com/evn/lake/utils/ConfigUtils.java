package com.evn.lake.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ConfigUtils {

    public static String catalog = "local";
    public static String schemaRaw = "raw_zone";
    public static String schemaGold = "gold_zone";

    public static ObjectMapper mapper = new ObjectMapper();



    public static class EtlTCNS {
        public static String rawPath =  "config/tcns/gen_data_raw.json";
        public static String goldPath =  "config/tcns/gold_ddl.json";
        public static String etlPath =  "config/tcns/raw2gold.json";
        public static String martPath =  "config/tcns/dim_ddl.json";

        public static String url = "jdbc:oracle:thin:@//118.70.49.45:1521/orcl";
        public static String user = "evnnpc_tochucnhansu2";
        public static String password = "root123";

        public static class RawTable{
            public static String S_ORGANIZATION = "S_ORGANIZATION";
            public static String NS_LLNS =	"NS_LLNS";
            public static String zNS_VTRI =	"zNS_VTRI";
            public static String NS_PHONGTO =	"NS_PHONGTO";
            public static String Vie_CHUCDANH =	"Vie_CHUCDANH";
            public static String S_DEPARTMENT =	"S_DEPARTMENT";
            public static String BC_EVN02b_gui =	"BC_EVN02b_gui";
            public static String NS_HDLDONG =	"NS_HDLDONG";
            public static String BC_EVN01_gui =	"BC_EVN01_gui";
            public static String BC_HDTV_ThoiDoiThucHienTuyenDung =	"BC_HDTV_ThoiDoiThucHienTuyenDung";
            public static String NS_QTLAMVIEC =	"NS_QTLAMVIEC";
            public static String SK_HOSOKHAM =	"SK_HOSOKHAM";
        }

        public static class GoldTable{
            public static String TCNS_cay_don_vi = "TCNS_cay_don_vi";   // from S_ORGANIZATION
            public static String TCNS_thong_tin_nhan_su = "TCNS_thong_tin_nhan_su";  // from NS_LLNS
//            public static String TCNS_Vi_tri_nhan_su = "TCNS_Vi_tri_nhan_su";  // zNS_VTRI and NS_PHONGTO  -> dang loi
            public static String TCNS_danh_muc_vi_tri = "TCNS_danh_muc_vi_tri";  // Vie_CHUCDANH
            public static String TCNS_danh_muc_phong_ban = "TCNS_danh_muc_phong_ban";  // S_DEPARTMENT
            public static String TCNS_BC_Chi_Tieu_EVN02 = "TCNS_BC_Chi_Tieu_EVN02"; // BC_EVN02b_gui
            public static String TCNS_hop_dong = "TCNS_hop_dong";  // NS_HDLDONG
            public static String TCNS_bao_cao_chi_tieu_evn01 = "TCNS_bao_cao_chi_tieu_evn01";  // BC_EVN01_gui
            public static String TCNS_Co_cau_tuyen_dung = "TCNS_Co_cau_tuyen_dung";  // BC_HDTV_ThoiDoiThucHienTuyenDung
            public static String TCNS_Ky_hop_dong = "TCNS_Ky_hop_dong";  // NS_HDLDONG and NS_QTLAMVIEC
            public static String TCNS_ky_kham_suc_khoe = "TCNS_ky_kham_suc_khoe";  // SK_HOSOKHAM
        }

        public static class FactTable{
            public static String F_TCNS_tong_so_hop_dong_ky_moi ="F_TCNS_tong_so_hop_dong_ky_moi";
            public static String F_TCNS_tong_co_cau_tuyen_dung ="F_TCNS_tong_co_cau_tuyen_dung";
            public static String F_TCNS_tong_Lao_dong_dien ="F_TCNS_tong_Lao_dong_dien";
            public static String F_TCNS_Lao_dong_phong_ban ="F_TCNS_Lao_dong_phong_ban";
            public static String F_TCNS_BC_Chi_Tieu_EVN01 ="F_TCNS_BC_Chi_Tieu_EVN01";
            public static String F_TCNS_suc_khoe ="F_TCNS_suc_khoe";
            public static String F_TCNS_BC_Chi_Tieu_EVN02 ="F_TCNS_BC_Chi_Tieu_EVN02";
            public static String F_TCNS_Cong_Lao_Dong_Dinh_Muc_SXKD_Dien ="F_TCNS_Cong_Lao_Dong_Dinh_Muc_SXKD_Dien";
            public static String F_TCNS_TH_Dinh_Bien ="F_TCNS_TH_Dinh_Bien";
            public static String F_TCNS_thua_thieu_Ld ="F_TCNS_thua_thieu_Ld";
            public static String F_TCNS_CBNV_theo_do_tuoi ="F_TCNS_CBNV_theo_do_tuoi";
        }

        public static class DimTable{
            public static String D_TCNS_Don_vi = "D_TCNS_Don_vi";
            public static String D_TCNS_Nhan_su = "D_TCNS_Nhan_su";
            public static String D_TCNS_hop_dong = "D_TCNS_hop_dong";
            public static String D_TCNS_danh_muc_vi_tri = "D_TCNS_danh_muc_vi_tri";
            public static String D_TCNS_phong_ban = "D_TCNS_phong_ban";
            public static String D_TCNS_Danh_Muc_Cong_Viec = "D_TCNS_Danh_Muc_Cong_Viec";
            public static String D_TCNS_Danh_Muc_Vung_Mien = "D_TCNS_Danh_Muc_Vung_Mien";
        }



    }


    public ConfigUtils() throws IOException {
    }
}
