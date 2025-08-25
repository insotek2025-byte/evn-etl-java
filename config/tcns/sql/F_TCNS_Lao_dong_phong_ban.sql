SELECT
    ID_phong_ban,
    ID_don_vi,
    Id_thang,
    COUNT(DISTINCT ID_nhan_su) AS SL_LD_lao_dong_thuy_dien
FROM  local.gold_zone.TCNS_Vi_tri_nhan_su
WHERE Trang_Thai_Lam_Viec = 'Đang làm'
  AND (
        -- Thanh Hóa
        (ID_don_vi = 'THANHHOA' AND ID_phong_ban IN ('Doi_SX_VH_TD_ThachBan'))
     OR -- Sơn La
        (ID_don_vi = 'SONLA' AND ID_phong_ban IN ('Phong_Ky_Thuat','NhaMay_SoVin','NhaMay_ChiengNgam'))
     OR -- Điện Biên
        (ID_don_vi = 'DIENBIEN' AND ID_phong_ban = 'PX_Phat_Dien')
     OR -- Cao Bằng + Lai Châu
        (ID_don_vi IN ('CAOBANG','LAICHAU') AND ID_phong_ban = 'Phong_Thuy_Dien')
     OR -- Hà Giang
        (ID_don_vi = 'HAGIANG' AND ID_phong_ban = 'DienLuc_TP')
      )
GROUP BY
    ID_phong_ban,
    ID_don_vi,
    Id_thang;