-- F_KTVH_SuCoTrungAp_TRANSACTION: Fact table for medium voltage incident transactions
-- This table stores detailed incident records with timing information

SELECT
    CAST(sc.ID AS BIGINT) AS ID,
    CAST(sc.ID_Don_Vi AS BIGINT) AS ID_Don_Vi,
    CAST(sc.ID_Cap_Dien_Ap AS BIGINT) AS ID_Cap_Dien_Ap,
    CAST(sc.ID_Thiet_Bi_Su_Co AS BIGINT) AS ID_Thiet_Bi_Su_Co,
    sc.Ten_Thiet_Bi_Su_Co,
    sc.Dien_bien_Su_Co,
    sc.Thoi_Gian_Xuat_Hien,
    sc.Thoi_Gian_Bat_Dau_Khac_Phuc,
    sc.Thoi_Gian_Khac_Phuc_Xong,
    sc.Thoi_Gian_Khoi_Phuc,
    CAST(sc.T_XuatHienBatDauKhacPhuc AS BIGINT) AS T_XuatHienBatDauKhacPhuc,
    CAST(sc.T_BatDauDenKhacPhucXong AS BIGINT) AS T_BatDauDenKhacPhucXong,
    CAST(sc.T_KhacPhucXongDenKhoiPhuc AS BIGINT) AS T_KhacPhucXongDenKhoiPhuc,
    CAST(sc.T_TongThoiGianMatDien AS BIGINT) AS T_TongThoiGianMatDien,
    CAST(sc.ID_Phieu_Cong_Tac AS BIGINT) AS ID_Phieu_Cong_Tac,
    CAST(sc.ID_Loai_Su_Co AS BIGINT) AS ID_Loai_Su_Co,
    CAST(sc.ID_Nguyen_Nhan AS BIGINT) AS ID_Nguyen_Nhan,
    CAST(sc.ID_Tinh_Chat AS BIGINT) AS ID_Tinh_Chat,
    CAST(sc.ID_Hanh_Lang AS BIGINT) AS ID_Hanh_Lang,
    CAST(sc.Hanh_Lang AS BIGINT) AS Hanh_Lang,
    CAST(sc.Su_Co_Tai_San AS BIGINT) AS Su_Co_Tai_San
FROM local.gold_zone.KTVH_Su_co_trung_ap sc
WHERE sc.ID IS NOT NULL