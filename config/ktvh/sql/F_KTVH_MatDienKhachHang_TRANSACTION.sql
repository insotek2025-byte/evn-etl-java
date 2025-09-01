-- F_KTVH_MatDienKhachHang_TRANSACTION: Fact table for customer power outage transactions
-- This table stores customer power outage details with cause and duration

SELECT
    CAST(md.Don_vi_quan_ly AS BIGINT) AS ID_Don_Vi_Quan_Ly,
    md.Don_vi_quan_ly_DoTinCay,
    md.Ma_Lich_Mat_Dien,
    md.Ma_Lich_Dong_Dien,
    CAST(md.ID_Nguyen_Nhan AS BIGINT) AS ID_Nguyen_Nhan,
    md.Ngay_Mat_Dien,
    CAST(md.Thang AS BIGINT) AS Thang,
    CAST(md.Nam AS BIGINT) AS Nam,
    CAST(md.Thoi_Gian_Mat_Dien AS BIGINT) AS Thoi_Gian_Mat_Dien,
    CAST(md.So_Khach_Hang AS BIGINT) AS So_Khach_Hang
FROM local.gold_zone.KTVH_Thong_tin_mat_dien_KH md
WHERE md.Don_vi_quan_ly IS NOT NULL
    AND md.Ngay_Mat_Dien IS NOT NULL
    AND md.Nam IS NOT NULL
    AND md.Thang IS NOT NULL