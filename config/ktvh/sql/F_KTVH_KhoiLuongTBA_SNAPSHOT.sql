SELECT
    CAST(CONCAT(YEAR(CURRENT_DATE()), LPAD(MONTH(CURRENT_DATE()), 2, '0')) AS BIGINT) AS ID_ThoiGian,
    CAST(kt.Don_Vi_Van_Hanh AS BIGINT) AS ID_Don_Vi,
    CAST(kt.Loai_Tram AS BIGINT) AS Loai_Tram,
    CAST(COUNT(kt.ID_Thuoc_Tinh) AS BIGINT) AS So_Luong,
    CAST(kt.ID_Cap_Dien_Ap AS BIGINT) AS Cap_Dien_Ap
FROM local.gold_zone.KTVH_Tram_Bien_Ap kt
GROUP BY
    CAST(CONCAT(YEAR(CURRENT_DATE()), LPAD(MONTH(CURRENT_DATE()), 2, '0')) AS BIGINT),
    CAST(kt.Don_Vi_Van_Hanh AS BIGINT),
    CAST(kt.Loai_Tram AS BIGINT),
    CAST(kt.ID_Cap_Dien_Ap AS BIGINT)