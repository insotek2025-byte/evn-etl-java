-- (tuỳ chọn) tạo namespace
CREATE NAMESPACE IF NOT EXISTS local.gold_zone;

-- Bảng staging: TCNS_Ky_hop_dong
CREATE TABLE IF NOT EXISTS local.gold_zone.TCNS_Ky_hop_dong (
  ID           BIGINT,
  ID_don_vi    BIGINT,
  Thang        INT,
  ID_hop_dong  BIGINT,
  Ky_moi       INT
)
USING iceberg
PARTITIONED BY (Thang);

INSERT INTO local.gold_zone.TCNS_Ky_hop_dong (ID, ID_don_vi, Thang, ID_hop_dong, Ky_moi)
SELECT
    hd.HDLDONG_ID        AS ID,              -- hoặc sequence/generator nếu cần
    hd.DONVI_ID          AS ID_don_vi,
    EXTRACT(MONTH FROM hd.NGAY_HIEU_LUC) AS Thang,  -- giả sử có cột NGAY_HIEU_LUC trong hợp đồng
    hd.HDLDONG_ID        AS ID_hop_dong,
    CASE
        WHEN qtl.QTLAMVIEC_ID IS NULL THEN 1   -- không có quá trình làm việc => ký mới
        ELSE 0                                 -- có quá trình làm việc => luân chuyển
    END AS Ky_moi
FROM local.raw_zone.NS_HDLDONG hd
LEFT JOIN local.raw_zone.NS_QTLAMVIEC qtl
       ON hd.NHAN_SU_ID = qtl.NHAN_SU_ID;   -- join theo nhân sự
