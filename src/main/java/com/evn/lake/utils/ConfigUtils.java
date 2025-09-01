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
        public static String goldDDLPath =  "config/tcns/gold_ddl.json";
        public static String etlRaw2GoldPath =  "config/tcns/raw2gold.json";
        public static String martDDLPath =  "config/tcns/mart_ddl.json";
        public static String etlGold2MartPath =  "config/tcns/gold2mart.json";
        public static String oracleConfig =  "config/oracle/tcns.properties";

        public static class RawTable {
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
            public static String F_TCNS_tong_so_hop_dong_ky_moi ="F_TCNS_tong_so_hop_dong_ky_moi";   // done
            public static String F_TCNS_tong_co_cau_tuyen_dung ="F_TCNS_tong_co_cau_tuyen_dung";   // du lieu khong co gi can xem lai
            public static String F_TCNS_tong_Lao_dong_dien ="F_TCNS_tong_Lao_dong_dien";   //
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
            public static String D_TCNS_Ngaythang ="D_TCNS_Ngaythang";
        }



    }

    public static class EtlKTVH {
        public static String rawPath = "config/ktvh/gen_data_raw.json";
        public static String goldDDLPath = "config/ktvh/gold_ddl.json";
        public static String etlRaw2GoldPath =  "config/ktvh/raw2gold.json";
        public static String martDDLPath =  "config/ktvh/mart_ddl.json";
        public static String etlGold2MartPath =  "config/ktvh/gold2mart.json";
        public static String oracleConfig =  "config/oracle/ktvh.properties";

        public static class RawTable{
            public static String plv_PhieuCongTac = "plv_PhieuCongTac";
            public static String plv_TinhChatPhien = "plv_TinhChatPhien";
            public static String plv_TrangThaiPhien = "plv_TrangThaiPhien";
            public static String d_LoaiCongViec = "d_LoaiCongViec";
            public static String hlat_CongTrinh = "hlat_CongTrinh";
            public static String hlat_PhaDat = "hlat_PhaDat";
            public static String sc_TaiNanSuCo = "sc_TaiNanSuCo";
            public static String sc_LoaiSuCo = "sc_LoaiSuCo";
            public static String sc_ChiTieuSoVuSuCo = "sc_ChiTieuSoVuSuCo";
            public static String tbnn_ThietBiNghiemNgat = "tbnn_ThietBiNghiemNgat";
            public static String tbnn_SoTheoDoiTBNN = "tbnn_SoTheoDoiTBNN";
        }

        public static class GoldTableJobId {
//            public static String KTVH_Phien_lam_viec_PhieuCongTac = "KTVH_Phien_lam_viec_PhieuCongTac";
//            public static String KTVH_Loai_Phien = "KTVH_Loai_Phien";
//            public static String KTVH_Trang_Thai_Phien = "KTVH_Trang_Thai_Phien";
//            public static String KTVH_Danh_Muc_Loai_Cong_Viec = "KTVH_Danh_Muc_Loai_Cong_Viec";
//            public static String KTVH_Hlat_vi_pham_CongTrinh = "KTVH_Hlat_vi_pham_CongTrinh";
//            public static String KTVH_Hlat_vi_pham_PhaDat = "KTVH_Hlat_vi_pham_PhaDat";
//            public static String KTVH_Su_co_trung_ap = "KTVH_Su_co_trung_ap";
//            public static String KTVH_Su_co_trung_ap_LoaiSuCo = "KTVH_Su_co_trung_ap_LoaiSuCo";
//            public static String KTVH_Su_co_trung_ap_ChiTieuSuCo = "KTVH_Su_co_trung_ap_ChiTieuSuCo";
//            public static String KTVH_Thiet_bi_nghiem_ngat = "KTVH_Thiet_bi_nghiem_ngat";
//            public static String KTVH_So_theo_doi_TBNN = "KTVH_So_theo_doi_TBNN";
//            public static String KTVH_Thong_tin_mat_dien_KH = "KTVH_Thong_tin_mat_dien_KH";
//            public static String KTVH_Do_tin_cay_nhom_nguyen_nhan = "KTVH_Do_tin_cay_nhom_nguyen_nhan";
//            public static String KTVH_Do_tin_cay_loai_mat_dien = "KTVH_Do_tin_cay_loai_mat_dien";
//            public static String KTVH_Danh_muc_yeu_to_quan_trac = "KTVH_Danh_muc_yeu_to_quan_trac";
//            public static String KTVH_Ket_qua_quan_trac_moi_truong = "KTVH_Ket_qua_quan_trac_moi_truong";

            // union TBA
//            public static String KTVH_Tram_Bien_Ap = "KTVH_Tram_Bien_Ap";
//            public static String KTVH_Tram_Bien_Ap_from_ZAG_0110D00_TBA = "KTVH_Tram_Bien_Ap_from_ZAG_0110D00_TBA";
//            public static String KTVH_Tram_Bien_Ap_from_D_TRAM = "KTVH_Tram_Bien_Ap_from_D_TRAM";
//            public static String KTVH_Tram_Bien_Ap_from_ZAG_0035D00_TBA = "KTVH_Tram_Bien_Ap_from_ZAG_0035D00_TBA";
//            public static String KTVH_Tram_Bien_Ap_from_ZAG_0022D00_TBA = "KTVH_Tram_Bien_Ap_from_ZAG_0022D00_TBA";

            // TODO
            public static String KTVH_May_Bien_Ap = "KTVH_May_Bien_Ap";
//            public static String KTVH_May_Bien_Ap_from_ZAG_0110D00_MBA = "KTVH_May_Bien_Ap_from_ZAG_0110D00_MBA";
//            public static String KTVH_May_Bien_Ap_from_ZAG_0022D00_MBA = "KTVH_May_Bien_Ap_from_ZAG_0022D00_MBA";
//            public static String KTVH_May_Bien_Ap_from_ZAG_0035D00_MBA = "KTVH_May_Bien_Ap_from_ZAG_0035D00_MBA";

//            public static String KTVH_Do_tin_cay_xuat_tuyen = "KTVH_Do_tin_cay_xuat_tuyen";
//            public static String KTVH_Cap_Dien_Ap = "KTVH_Cap_Dien_Ap";
//            public static String KTVH_Duong_Day = "KTVH_Duong_Day";

//            public static String KTVH_Thiet_bi_tren_luoi = "KTVH_Thiet_bi_tren_luoi";
//            public static String KTVH_Tu_bu = "KTVH_Tu_bu";

              // không biết dùng không
//            public static String KTVH_KL_DZ = "KTVH_KL_DZ";
//            public static String KTVH_KL_THIET_BI_LUOI = "KTVH_KL_THIET_BI_LUOI";
        }

        public static class FactTable {
            public static String F_KTVH_SL_TBNN_SNAPSHOT = "F_KTVH_SL_TBNN_SNAPSHOT";
            public static String F_KTVH_SL_QuanTracMoiTruong = "F_KTVH_SL_QuanTracMoiTruong";
            public static String F_KTVH_PhienLamViec = "F_KTVH_PhienLamViec";
            public static String F_KTVH_TH_Phien_VeSinhCachDien_SNAPSHOT = "F_KTVH_TH_Phien_VeSinhCachDien_SNAPSHOT";
            public static String F_KTVH_TH_Phien_SuaChuaHotline_SNAPSHOT = "F_KTVH_TH_Phien_SuaChuaHotline_SNAPSHOT";
            public static String F_KTVH_SuCoTrungAp_TRANSACTION = "F_KTVH_SuCoTrungAp_TRANSACTION";
            public static String F_KTVH_SoVuSuCo_TheoLoai_SNAPSHOT = "F_KTVH_SoVuSuCo_TheoLoai_SNAPSHOT";
            public static String F_KTVH_SoVuSuCo_TheoTaiSan_SNAPSHOT = "F_KTVH_SoVuSuCo_TheoTaiSan_SNAPSHOT";
            public static String F_KTVH_SoVuSuCo_NguyenNhanHanhLang_SNAPSHOT = "F_KTVH_SoVuSuCo_NguyenNhanHanhLang_SNAPSHOT";
            public static String F_KTVH_ChiTieuSuCo_TRANSACTION = "F_KTVH_ChiTieuSuCo_TRANSACTION";
            public static String F_KTVH_ViPham_HLATLD_SNAPSHOT = "F_KTVH_ViPham_HLATLD_SNAPSHOT";
            public static String F_KTVH_DTCCCD_TRANSACTION = "F_KTVH_DTCCCD_TRANSACTION";
            public static String F_KTVH_MatDienKhachHang_TRANSACTION = "F_KTVH_MatDienKhachHang_TRANSACTION";
            public static String F_TuBu_SNAPSHOT = "F_TuBu_SNAPSHOT";
            public static String F_KTVH_KhoiLuongTBA_SNAPSHOT = "F_KTVH_KhoiLuongTBA_SNAPSHOT";
            public static String F_KTVH_KhoiLuongMBA_SNAPSHOT = "F_KTVH_KhoiLuongMBA_SNAPSHOT";
            public static String F_KTVH_KhoiLuongDuongDay = "F_KTVH_KhoiLuongDuongDay";
            public static String F_KTVH_KhoiLuongThietBi_Luoi = "F_KTVH_KhoiLuongThietBi_Luoi";
        }

        public static class DimTable {
            public static String D_KTVH_TBNN = "D_KTVH_TBNN";
            public static String D_KTVH_CapDienAp = "D_KTVH_CapDienAp";
            public static String D_KTVH_TramBienAp = "D_KTVH_TramBienAp";
            public static String D_KTVH_MayBienAp = "D_KTVH_MayBienAp";
            public static String D_KTVH_DuongDay = "D_KTVH_DuongDay";
            public static String D_KTVH_ThietBiTrenLuoi = "D_KTVH_ThietBiTrenLuoi";
            public static String D_KTVH_LoaiSuCo = "D_KTVH_LoaiSuCo";
            public static String D_KTVH_NguyenNhanMatDien = "D_KTVH_NguyenNhanMatDien";
            public static String D_KTVH_LoaiMatDien = "D_KTVH_LoaiMatDien";
            public static String D_KTVH_DM_LoaiCongViec = "D_KTVH_DM_LoaiCongViec";
            public static String D_KTVH_LoaiPhien = "D_KTVH_LoaiPhien";
            public static String D_KTVH_TrangThaiPhien = "D_KTVH_TrangThaiPhien";
            public static String D_KTVH_DanhMucYeuToQuanTrac = "D_KTVH_DanhMucYeuToQuanTrac";
            public static String D_KTVH_Thoigian = "D_KTVH_Thoigian";
        }
    }

    public static class EtlKinhDoanh {
        public static String rawPath = "config/kinhdoanh/gen_data_raw.json";
        public static String goldDDLPath = "config/kinhdoanh/gold_ddl.json";
        public static String etlRaw2GoldPath = "config/kinhdoanh/raw2gold.json";
        public static String martDDLPath = "config/kinhdoanh/mart_ddl.json";
        public static String etlGold2MartPath = "config/kinhdoanh/gold2mart.json";
        public static String oracleConfig = "config/oracle/kinhdoanh.properties";

        public static class RawTable {
            public static String DV_YEU_CAU = "DV_YEU_CAU";
            public static String DV_TIEN_TRINH = "DV_TIEN_TRINH";
            public static String HDG_KHACH_HANG = "HDG_KHACH_HANG";
            public static String DV_HDONG_DTHAO = "DV_HDONG_DTHAO";
            public static String HDG_HOP_DONG = "HDG_HOP_DONG";
            public static String hdn_hdon = "hdn_hdon";
            public static String CN_THEODOI_NO = "CN_THEODOI_NO";
            public static String CN_HDON_GDICH = "CN_HDON_GDICH";
            public static String D_DVI_QLY = "D_DVI_QLY";
            public static String bc_sldgnhan = "bc_sldgnhan";
            public static String BC_TPTHEOTGDA = "BC_TPTHEOTGDA";
            public static String TT_DBTT_LDD = "TT_DBTT_LDD";
            public static String TT_DBTT_T = "TT_DBTT_T";
            public static String D_CLOAI_CTO = "D_CLOAI_CTO";
            public static String D_LOAI_YCAU = "D_LOAI_YCAU";
            public static String D_NHOM_YCAU = "D_NHOM_YCAU";
            public static String D_NHOM_NN = "D_NHOM_NN";
            public static String D_MA_NN = "D_MA_NN";
            public static String D_CONG_VIEC = "D_CONG_VIEC";
            public static String D_HTHUC_THANH_TOAN = "D_HTHUC_THANH_TOAN";
            public static String HDG_HDONG_DVK = "HDG_HDONG_DVK";
        }

        public static class GoldTable {
            public static String KD_Yeu_cau_dich_vu = "KD_Yeu_cau_dich_vu";
            public static String KD_Tien_trinh_cung_cap = "KD_Tien_trinh_cung_cap";
            public static String KD_Khach_hang = "KD_Khach_hang";
            public static String KD_Hoa_don = "KD_Hoa_don";
            public static String KD_Thanh_toan = "KD_Thanh_toan";
            public static String KD_SL_Dien = "KD_SL_Dien";
            public static String KD_Ton_that_duong_day = "KD_Ton_that_duong_day";
            public static String KD_Ton_that_tram = "KD_Ton_that_tram";
            public static String KD_So_luong_cong_to = "KD_So_luong_cong_to";
            public static String KD_Phieu_yeu_cau = "KD_Phieu_yeu_cau";
            public static String KD_Loai_yeu_cau = "KD_Loai_yeu_cau";
            public static String KD_Nhom_nganh = "KD_Nhom_nganh";
            public static String KD_nganh = "KD_nganh";
            public static String TCNS_Don_vi = "TCNS_Don_vi";
            public static String KD_Danh_muc_Cong_viec = "KD_Danh_muc_Cong_viec";
            public static String KD_Danh_muc_HTTT = "KD_Danh_muc_HTTT";
            public static String KD_Nhom_khach_hang = "KD_Nhom_khach_hang";
        }
    }

    public ConfigUtils() throws IOException {
    }
}
