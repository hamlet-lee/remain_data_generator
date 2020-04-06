--------------------------- 事务事实表1 ----------------
drop table if exists temp.dw_vendor_user_visit;
create table temp.dw_vendor_user_visit as
select day, userid, vendor from (
select 
  concat(
   regexp_extract(raw, "^(\\d{4})(\\d{2})(\\d{2})", 1) , '-',
   regexp_extract(raw, "^(\\d{4})(\\d{2})(\\d{2})", 2) , '-',
   regexp_extract(raw, "^(\\d{4})(\\d{2})(\\d{2})", 3) 
 ) as day,
  regexp_extract(raw, "\tuserid=(\\S+)(\t|$)", 1)
 as userid,
  regexp_extract(raw, "\tvendor=(\\S+)(\t|$)", 1)
 as vendor
from  temp.ods_remain_data ) t0
where vendor is not null and day is not null and userid is not null
group by day, userid, vendor;



--------------------------- 事务事实表2 ----------------
drop table if exists temp.dw_vendor_user_first_visit;
create table temp.dw_vendor_user_first_visit as
   select day, userid, vendor 
   from (
     select day, userid, vendor, ROW_NUMBER() over (partition by userid order by day) as r from temp.dw_vendor_user_visit 
   ) t0 where r = 1;

-----------------------------渠道维度表--------------------------------------
drop table if exists temp.dim_vendors;
create table temp.dim_vendors as select vendor from temp.dw_vendor_user_first_visit group by vendor;

-----------------------------周期快照事实表1-------------------------------
set hive.mapred.mode=nonstrict;
drop table if exists temp.dw_vendor_new_user;
create table temp.dw_vendor_new_user as
  select tbl_pk.day, tbl_pk.vendor, count(*) as new_user from 
          (
           select day, vendor from	
                temp.dim_date join  temp.dim_vendors on (1=1) 
                where day between '2020-01-01' and '2020-02-28'
          ) tbl_pk
          left join temp.dw_vendor_user_first_visit t2 on (tbl_pk.day = t2.day and tbl_pk.vendor = t2.vendor)
  group by tbl_pk.day, tbl_pk.vendor;

-----------------------------周期快照事实表2-------------------------------
drop table if exists temp.dw_vendor_remain30_user;
create table temp.dw_vendor_remain30_user as
 select tbl_pk.day, tbl_pk.vendor, coalesce(remain30_user, 0) as remain30_user  from 
          (
           select day, vendor from
                temp.dim_date join  temp.dim_vendors on (1=1) 
                where day between '2020-01-01' and '2020-01-29'
          ) tbl_pk
          left join 
          (  
          select fv.day, fv.vendor, count(*) as remain30_user from temp.dw_vendor_user_visit v join temp.dw_vendor_user_first_visit fv
          on (v.userid = fv.userid and v.vendor = fv.vendor) where date_add( fv.day, 30) = v.day
          group by fv.day, fv.vendor
          ) tbl_data
         on (tbl_pk.day = tbl_data.day and tbl_pk.vendor = tbl_data.vendor);


----------------------累计快照事实表 ------------------------
drop table if exists temp.dw_acc_vendor_stat;
create table temp.dw_acc_vendor_stat as
  select tbl_new.day,
            tbl_new.vendor,
            new_user,
            remain30_user
  from
       temp.dw_vendor_new_user tbl_new left join 
       temp.dw_vendor_remain30_user tbl_remain30
  on (tbl_new.day = tbl_remain30.day and tbl_new.vendor = tbl_remain30.vendor);


---------------------应用层表----------------------------------
drop table if exists temp.app_vendor_stat;
create table temp.app_vendor_stat as
  select day,
            vendor,
            new_user,
            remain30_user,
           if ( new_user is null or remain30_user is null, null,  (remain30_user / new_user) * 100 ) as remain30_user_ratio_per
  from
        temp.dw_acc_vendor_stat;
