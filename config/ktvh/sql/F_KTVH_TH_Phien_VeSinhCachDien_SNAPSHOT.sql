-- F_KTVH_TH_Phien_VeSinhCachDien_SNAPSHOT: Fact table for insulation cleaning session snapshot
-- This table stores monthly count of insulation cleaning sessions by organization unit

SELECT
    CAST(CONCAT(YEAR(pct.Ngay_Tao), LPAD(MONTH(pct.Ngay_Tao), 2, '0')) AS BIGINT) AS ID_ThoiGian,
    CAST(pct.ID_Don_Vi AS BIGINT) AS ID_Don_Vi,
    CAST(COUNT(pct.ID_Phieu_Cong_Tac) AS BIGINT) AS So_Phien_VSCD
FROM local.gold_zone.KTVH_Phien_lam_viec_PhieuCongTac pct
WHERE pct.ID_Don_Vi IS NOT NULL 
    AND pct.Ngay_Tao IS NOT NULL
    AND pct.ID_Loai_Cong_Viec IN (
        -- Assuming these are IDs for insulation cleaning work types
        -- Need to be updated with actual IDs from D_KTVH_DM_LoaiCongViec
        SELECT ID FROM local.gold_zone.KTVH_Danh_Muc_Loai_Cong_Viec 
        WHERE Ten_Loai_Cong_Viec LIKE '%vệ sinh cách điện%' 
           OR Ten_Loai_Cong_Viec LIKE '%VSCD%'
    )
GROUP BY 
    pct.ID_Don_Vi,
    CONCAT(YEAR(pct.Ngay_Tao), LPAD(MONTH(pct.Ngay_Tao), 2, '0'))