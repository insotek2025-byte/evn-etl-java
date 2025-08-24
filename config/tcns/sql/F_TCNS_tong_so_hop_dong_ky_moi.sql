SELECT
    ID_don_vi,
    Thoi_gian_bc,
    SUM(CASE WHEN Ky_moi = 0 THEN 1 ELSE 0 END) AS SL_HD_luan_chuyen,
    SUM(CASE WHEN Ky_moi = 1 THEN 1 ELSE 0 END) AS SL_HD_tuyen_moi,
    COUNT(ID_hop_dong) AS Tong_so_hop_dong_ky_moi
FROM
    local.gold_zone.TCNS_Ky_hop_dong
GROUP BY
    ID_don_vi,
    Thoi_gian_bc;
