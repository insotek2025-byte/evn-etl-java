-- F_KTVH_PhienLamViec: Fact table for work session transactions
-- This table stores work session details for maintenance activities

SELECT
    CAST(pct.ID_Phieu_Cong_Tac AS BIGINT) AS ID_Phieu_Cong_Tac,
    pct.So_Phieu,
    CAST(pct.ID_Don_Vi AS BIGINT) AS ID_Don_Vi,
    CAST(pct.Ma_Loai_Phien AS BIGINT) AS ID_Tinh_Chat,
    CAST(pct.Ma_Trang_Thai AS BIGINT) AS ID_Trang_Thai,
    CAST(pct.ID_Loai_Cong_Viec AS BIGINT) AS ID_Loai_Cong_Viec,
    pct.Ngay_Tao,
    pct.Ngay_Duyet,
    pct.ID_Don_Vi_Thuc_Hien AS Ma_Yeu_Cau_CRM
FROM local.gold_zone.KTVH_Phien_lam_viec_PhieuCongTac pct
WHERE pct.ID_Phieu_Cong_Tac IS NOT NULL