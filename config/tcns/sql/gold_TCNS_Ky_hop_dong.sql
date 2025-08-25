insert OVERWRITE TABLE  local.gold_zone.TCNS_Ky_hop_dong (ID, ID_don_vi, ID_Thoigian, ID_hop_dong, Ky_moi)
select
    hd.HDLDONG_ID        as ID,
    hd.DONVI_ID          as ID_don_vi,
    CAST(date_format(current_timestamp(), 'yyyyMMdd') AS BIGINT)  as ID_Thoigian,
    hd.HDLDONG_ID        as ID_hop_dong,
    case
        when qtl.QTLAMVIEC_ID is null then 1   -- không có quá trình làm việc => ký mới
        else 0                                 -- có quá trình làm việc => luân chuyển
    end as Ky_moi
from local.raw_zone.NS_HDLDONG hd
left join local.raw_zone.NS_QTLAMVIEC qtl
       on hd.NS_ID = qtl.NS_ID