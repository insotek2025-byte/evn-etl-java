-- F_KTVH_TH_Phien_SuaChuaHotline_SNAPSHOT: Fact table for hotline repair session snapshot  
-- This table stores monthly count of hotline repair sessions by organization unit and voltage level

SELECT
    CAST(CONCAT(YEAR(pct.Ngay_Tao), LPAD(MONTH(pct.Ngay_Tao), 2, '0')) AS BIGINT) AS ID_ThoiGian,
    CAST(pct.ID_Don_Vi AS BIGINT) AS ID_Don_Vi,
    CAST(22 AS BIGINT) AS Cap_Dien_Ap, -- Need to determine from actual data
    CAST(COUNT(pct.ID_Phieu_Cong_Tac) AS BIGINT) AS So_Phien_Hotline
FROM local.gold_zone.KTVH_Phien_lam_viec_PhieuCongTac pct
WHERE pct.ID_Don_Vi IS NOT NULL 
    AND pct.Ngay_Tao IS NOT NULL
    AND pct.ID_Loai_Cong_Viec IN (
        -- Assuming these are IDs for hotline repair work types
        -- Need to be updated with actual IDs from D_KTVH_DM_LoaiCongViec
        SELECT ID FROM local.gold_zone.KTVH_Danh_Muc_Loai_Cong_Viec 
        WHERE Ten_Loai_Cong_Viec LIKE '%hotline%' 
           OR Ten_Loai_Cong_Viec LIKE '%sửa chữa%'
    )
GROUP BY 
    pct.ID_Don_Vi,
    CONCAT(YEAR(pct.Ngay_Tao), LPAD(MONTH(pct.Ngay_Tao), 2, '0'))