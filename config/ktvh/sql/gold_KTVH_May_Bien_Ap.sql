INSERT OVERWRITE TABLE local.gold_zone.KTVH_May_Bien_Ap (ID_MBA, ORGID,Cong_Suat, Idm, Hang_San_Suat,Cap_dien_ap)
(
select a.ATTRDATAID as ID_MBA, b.ORGID as ID_Don_vi, a.P as Cong_Suat, a.IDM as Idm, a.HSX as Hang_San_Suat, 22 as Cap_dien_ap
from local.raw_zone.ZAG_0022D00_MBA a
join local.raw_zone.A_ASSET b
on a.ATTRDATAID = b.ASSETID
)
UNION ALL
(
select a.ATTRDATAID as ID_MBA, b.ORGID as ID_Don_vi, a.P as Cong_Suat, a.IDM as Idm, a.HSX as Hang_San_Suat, 35 as Cap_dien_ap
from local.raw_zone.ZAG_0035D00_MBA a
join local.raw_zone.A_ASSET b
on a.ATTRDATAID = b.ASSETID
)
UNION ALL
(
select a.ATTRDATAID as ID_MBA, b.ORGID as ID_Don_vi, a.P as Cong_Suat, a.IDM as Idm, a.HSX as Hang_San_Suat, 110 as Cap_dien_ap
from local.raw_zone.ZAG_0110D00_MBA a
join local.raw_zone.A_ASSET b
on a.ATTRDATAID = b.ASSETID
)