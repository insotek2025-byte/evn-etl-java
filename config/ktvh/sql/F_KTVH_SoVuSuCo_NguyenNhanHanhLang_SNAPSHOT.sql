-- F_KTVH_SoVuSuCo_NguyenNhanHanhLang_SNAPSHOT: Fact table for incident count by corridor cause snapshot
-- This table stores monthly count of incidents by device, corridor cause, and organization unit

SELECT
    CAST(CONCAT(YEAR(sc.Thoi_Gian_Xuat_Hien), LPAD(MONTH(sc.Thoi_Gian_Xuat_Hien), 2, '0')) AS BIGINT) AS ID_Thoigian,
    CAST(sc.ID_Don_Vi AS BIGINT) AS ID_Don_Vi,
    CAST(sc.ID_Thiet_Bi_Su_Co AS BIGINT) AS ID_Thiet_Bi_Su_Co,
    CAST(sc.ID_Hanh_Lang AS BIGINT) AS ID_Hanh_Lang,
    CAST(COUNT(sc.ID) AS BIGINT) AS So_Vu_Su_Co
FROM local.gold_zone.KTVH_Su_co_trung_ap sc
WHERE sc.ID_Don_Vi IS NOT NULL 
    AND sc.ID_Thiet_Bi_Su_Co IS NOT NULL
    AND sc.ID_Hanh_Lang IS NOT NULL
    AND sc.Thoi_Gian_Xuat_Hien IS NOT NULL
GROUP BY 
    sc.ID_Don_Vi,
    sc.ID_Thiet_Bi_Su_Co,
    sc.ID_Hanh_Lang,
    CONCAT(YEAR(sc.Thoi_Gian_Xuat_Hien), LPAD(MONTH(sc.Thoi_Gian_Xuat_Hien), 2, '0'))