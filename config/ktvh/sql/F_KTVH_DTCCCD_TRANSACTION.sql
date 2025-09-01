-- F_KTVH_DTCCCD_TRANSACTION: Fact table for reliability supply transaction
-- This table stores reliability indicators and power outage statistics

SELECT
    CAST(dtc.ID_Don_Vi_Quan_Ly AS BIGINT) AS ID_Don_Vi_Quan_Ly,
    CAST(dtc.ID_May_Cat AS BIGINT) AS ID_May_Cat,
    dtc.Ma_May_Cat,
    dtc.Ten_May_Cat,
    CAST(dtc.So_KH AS BIGINT) AS So_KH,
    dtc.Nhom_Ma,
    CAST(dtc.Nam AS BIGINT) AS Nam,
    CAST(dtc.Thang AS BIGINT) AS Thang,
    CAST(dtc.Tong_so_lan AS BIGINT) AS Tong_so_lan,
    CAST(dtc.Keo_dai_tong_so_lan AS BIGINT) AS Keo_dai_tong_so_lan,
    CAST(dtc.Keo_dai_tong_so_lan_xuat_tuyen AS BIGINT) AS Keo_dai_tong_so_lan_xuat_tuyen,
    CAST(dtc.Keo_dai_tong_so_lan_DZ AS BIGINT) AS Keo_dai_tong_so_lan_DZ,
    CAST(dtc.Thoang_qua_tong_so_lan AS BIGINT) AS Thoang_qua_tong_so_lan,
    CAST(dtc.Thoang_qua_tong_so_lan_xuat_tuyen AS BIGINT) AS Thoang_qua_tong_so_lan_xuat_tuyen,
    CAST(dtc.Thoang_qua_tong_so_lan_DZ AS BIGINT) AS Thoang_qua_tong_so_lan_DZ,
    CAST(dtc.MAIFI AS BIGINT) AS MAIFI,
    CAST(dtc.SAIDI AS BIGINT) AS SAIDI,
    CAST(dtc.SAIFI AS BIGINT) AS SAIFI,
    CAST(dtc.CAIDI AS BIGINT) AS CAIDI,
    CAST(dtc.CAIFI AS BIGINT) AS CAIFI,
    CAST(dtc.MAIFI_Cong_Ty AS BIGINT) AS MAIFI_Cong_Ty,
    CAST(dtc.SAIDI_Cong_Ty AS BIGINT) AS SAIDI_Cong_Ty,
    CAST(dtc.SAIFI_Cong_Ty AS BIGINT) AS SAIFI_Cong_Ty,
    CAST(dtc.MAIFI_Tong_Cong_Ty AS BIGINT) AS MAIFI_Tong_Cong_Ty,
    CAST(dtc.SAIDI_Tong_Cong_Ty AS BIGINT) AS SAIDI_Tong_Cong_Ty,
    CAST(dtc.SAIFI_Tong_Cong_Ty AS BIGINT) AS SAIFI_Tong_Cong_Ty,
    CAST(dtc.ID_Don_Vi_Quan_ly_Tinh AS BIGINT) AS ID_Don_Vi_Quan_ly_Tinh
FROM local.gold_zone.KTVH_Do_tin_cay_xuat_tuyen dtc
WHERE dtc.ID_Don_Vi_Quan_Ly IS NOT NULL
    AND dtc.Nam IS NOT NULL
    AND dtc.Thang IS NOT NULL