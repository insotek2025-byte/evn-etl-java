INSERT OVERWRITE TABLE local.gold_zone.KTVH_Cap_Dien_Ap (ID_Cap_Dien_Ap, Cap_Dien_Ap, Thu_Tu, Trang_Thai)
SELECT
    CAST(ID AS BIGINT) AS ID_Cap_Dien_Ap,
    CapDienAp AS Cap_Dien_Ap,
    CAST(ThuTu AS BIGINT) AS Thu_Tu,
    CAST(TrangThai AS BIGINT) AS Trang_Thai
FROM local.raw_zone.hlat_CapDienAP
UNION ALL
SELECT
    CAST(ULEVELID AS BIGINT) AS ID_Cap_Dien_Ap,
    ULEVELDESC AS Cap_Dien_Ap,
    CAST(ULEVEL_NUM AS BIGINT) AS Thu_Tu,
    CAST(ACTIVE AS BIGINT) AS Trang_Thai
FROM local.raw_zone.CDS_A_LST_ULEVEL
UNION ALL
SELECT
    CAST(MA_CAPDA AS BIGINT) AS ID_Cap_Dien_Ap,
    TEN_CAPDA  AS Cap_Dien_Ap,
    CAST(STT_HTHI AS BIGINT) AS Thu_Tu,
    CAST(TRANG_THAI AS BIGINT) AS Trang_Thai
FROM local.raw_zone.D_CAP_DAP;

