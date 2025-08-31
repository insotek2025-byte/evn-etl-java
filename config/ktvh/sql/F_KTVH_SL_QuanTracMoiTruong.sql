-- F_KTVH_SL_QuanTracMoiTruong: Fact table for environmental monitoring quantity snapshot
-- This table stores monthly snapshot of environmental monitoring samples by organization unit and monitoring factor

SELECT
    CAST(CONCAT(YEAR(kt.Ngay_Quan_Trac), LPAD(MONTH(kt.Ngay_Quan_Trac), 2, '0')) AS BIGINT) AS ID_thoigian,
    CAST(kt.ID_Don_Vi AS BIGINT) AS ID_Don_Vi,
    CAST(kt.ID_Yeu_To AS BIGINT) AS ID_Yeu_To,
    CAST(COUNT(*) AS BIGINT) AS Tong_So_Mau,
    CAST(COUNT(CASE WHEN kt.So_Mau_khong_Dat > 0 THEN 1 END) AS BIGINT) AS So_Mau_Khong_Dat
FROM local.gold_zone.KTVH_Ket_qua_quan_trac_moi_truong kt
WHERE kt.ID_Don_Vi IS NOT NULL 
    AND kt.ID_Yeu_To IS NOT NULL
    AND kt.Ngay_Quan_Trac IS NOT NULL
GROUP BY 
    kt.ID_Don_Vi,
    kt.ID_Yeu_To,
    CONCAT(YEAR(kt.Ngay_Quan_Trac), LPAD(MONTH(kt.Ngay_Quan_Trac), 2, '0'))