-- F_KTVH_ChiTieuSuCo_TRANSACTION: Fact table for incident target transactions
-- This table stores incident target and actual count data by organization unit and incident type

SELECT
    CAST(ct.ID AS BIGINT) AS ID,
    ct.Nam,
    ct.Thang,
    CAST(ct.ID_Don_Vi AS BIGINT) AS ID_Don_Vi,
    CAST(ct.ID_Loai_Su_Co AS BIGINT) AS ID_Loai_Su_Co,
    CAST(ct.Chi_Tieu_Nam AS BIGINT) AS Chi_Tieu_Nam,
    CAST(ct.Chi_Tieu_Thang AS BIGINT) AS Chi_Tieu_Thang,
    CAST(ct.So_Vu_Sau_MienTru AS BIGINT) AS So_Vu_Sau_MienTru,
    CAST(ct.So_Vu_Truoc_MienTru AS BIGINT) AS So_Vu_Truoc_MienTru,
    CAST(ct.NPC_Chi_Tieu_Nam AS BIGINT) AS NPC_Chi_Tieu_Nam
FROM local.gold_zone.KTVH_Su_co_trung_ap_ChiTieuSuCo ct
WHERE ct.ID IS NOT NULL