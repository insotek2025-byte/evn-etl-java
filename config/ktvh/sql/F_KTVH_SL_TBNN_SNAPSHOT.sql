-- F_KTVH_SL_TBNN_SNAPSHOT: Fact table for TBNN (Thiết bị nghiêm ngặt) quantity snapshot
-- This table stores monthly snapshot of strict equipment quantities by organization unit

SELECT
    CAST(CONCAT(YEAR(CURRENT_DATE), LPAD(MONTH(CURRENT_DATE), 2, '0')) AS BIGINT) AS ID_thoigian,
    CAST(tb.ID_Don_Vi_Quan_Ly AS BIGINT) AS ID_Don_Vi,
    CAST(COUNT(tb.ID_Thiet_Bi) AS BIGINT) AS SL_Khai_Bao,
    CAST(COUNT(CASE WHEN st.Ket_Qua = 1 THEN tb.ID_Thiet_Bi END) AS BIGINT) AS SL_Kiem_Duyet,
    CAST(COUNT(CASE WHEN st.Ket_Qua = 0 OR st.Ket_Qua IS NULL THEN tb.ID_Thiet_Bi END) AS BIGINT) AS SL_Chua_Kiem_Duyet
FROM local.gold_zone.KTVH_Thiet_bi_nghiem_ngat tb
LEFT JOIN local.gold_zone.KTVH_So_theo_doi_TBNN st 
    ON tb.ID_Thiet_Bi = st.ID_Thiet_Bi
    AND YEAR(st.Ngay_Kiem_Tra) = YEAR(CURRENT_DATE)
    AND MONTH(st.Ngay_Kiem_Tra) = MONTH(CURRENT_DATE)
WHERE tb.ID_Don_Vi_Quan_Ly IS NOT NULL
GROUP BY 
    tb.ID_Don_Vi_Quan_Ly;