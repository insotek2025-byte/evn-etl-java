-- F_KTVH_ViPham_HLATLD_SNAPSHOT: Fact table for corridor safety violation snapshot
-- This table stores violation data from both construction and excavation violations

SELECT
    CAST(CONCAT(hlat_ct.Nam, LPAD(hlat_ct.Thang, 2, '0')) AS BIGINT) AS ID_ThoiGian,
    CAST(hlat_ct.ID_Don_Vi AS BIGINT) AS ID_Don_Vi,
    CAST(hlat_ct.ID_Cap_Dien_Ap AS BIGINT) AS ID_Cap_Dien_Ap,
    CAST(hlat_ct.Tong_Dau_Nam AS BIGINT) AS Tong_Dau_Nam,
    CAST(hlat_ct.Tong_Luy_Ke AS BIGINT) AS Tong_Luy_Ke,
    CAST(hlat_ct.TangGiam_PhatSinhMoi AS BIGINT) AS TangGiam_PhatSinhMoi,
    CAST(hlat_ct.TangGiam_GiamDoCaiTao AS BIGINT) AS TangGiam_GiamDoCaiTao,
    CAST(hlat_ct.TangGiam_GiamDoPhoiHopDiaPhuong AS BIGINT) AS TangGiam_GiamDoPhoiHopDiaPhuong,
    CAST(hlat_ct.PhanLoai_Khoan1 AS BIGINT) AS PhanLoai_Khoan1,
    CAST(hlat_ct.PhanLoai_Khoan2 AS BIGINT) AS PhanLoai_Khoan2,
    CAST(hlat_ct.PhanLoai_Khoan3 AS BIGINT) AS PhanLoai_Khoan3,
    CAST(hlat_ct.PhanLoai_Khoan5 AS BIGINT) AS PhanLoai_Khoan5,
    CAST(hlat_ct.Cap_Ngam AS BIGINT) AS Cap_Ngam,
    CAST(COALESCE(hlat_pd.Tong_Luy_Ke, 0) AS BIGINT) AS Pha_Dat,
    hlat_ct.Ghi_Chu
FROM local.gold_zone.KTVH_Hlat_vi_pham_CongTrinh hlat_ct
LEFT JOIN local.gold_zone.KTVH_Hlat_vi_pham_PhaDat hlat_pd 
    ON hlat_ct.ID_Don_Vi = hlat_pd.ID_Don_Vi 
    AND hlat_ct.ID_Cap_Dien_Ap = hlat_pd.ID_Cap_Dien_Ap
    AND hlat_ct.Nam = hlat_pd.Nam 
    AND hlat_ct.Thang = hlat_pd.Thang
WHERE hlat_ct.ID_Don_Vi IS NOT NULL 
    AND hlat_ct.ID_Cap_Dien_Ap IS NOT NULL
    AND hlat_ct.Nam IS NOT NULL
    AND hlat_ct.Thang IS NOT NULL