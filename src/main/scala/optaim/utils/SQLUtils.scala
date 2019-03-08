package optaim.utils

object SQLUtils {
  val useDatabase="""use cdp"""
  val modeSet="""set hive.exec.dynamic.partition.mode=nonstrict"""
  val originalData=
    """insert into table original_table
      |select event_type,resource_type,
      |coupon_id,product_id,source,
      |jice_userid,mac,idfv,idfa,
      |ip,region_id,city,province,
      |country,time,net,operator,
      |ap_name,ap_mac,os,osv,
      |brand,model,cp,md,
      |pl,ct,kw,sourcetype,
      |shoulv_userid,user_mobile,
      |user_birthday,user_gender,
      |user_createtime,user_employee,
      |user_source,product_name,businesstype,
      |ent_id,ent_name,store_id,store_name,
      |active_id,active_name,coupon_from,order_id,
      |verification_id,resource_score,service_score,
      |score,openid,user_type,wxid,order_count,
      |order_amount,order_points,sale_price,
      |sale_points,upload_points,upload_amount,
      |actiontype,analysis_date
      |from original_data""".stripMargin
  val brandInc=
    """insert overwrite table brand_table_inc
      |select distinct x.brandid,x.name
      |from
      |(select max(ent_id) as brandid,ent_name as name
      |from original_data
      |where ent_id is not null
      |group by ent_name) x
      |where x.brandid not in (select brandid from before_brandid_table)""".stripMargin
  val brandParti=
    """insert into table brand_table
      |select distinct x.brandid,x.name,x.time
      |from
      |(select max(ent_id) as brandid,ent_name as name,max(analysis_date) as time
      |from original_data where ent_id is not null group by ent_name) x
      |where x.brandid not in (select brandid from before_brandid_table)""".stripMargin
  val braandId=
    """insert overwrite table before_brandid_table
      |select brandid from brand_table""".stripMargin

  /*
  val consumerInfoInc1=
    """insert overwrite table consumer_base_info_inc_one
      |select distinct shoulv_userid as userid,
      |from_unixtime(user_createtime/1000,'yyyy-MM-dd') as createtime,
      |city,user_mobile as phone,province,sourcetype,country,
      |from_unixtime(user_birthday/1000,'yyyy-MM-dd') as birthday,
      |user_gender as sex,null as channel
      |from original_data_one where shoulv_userid not in
      |(select userid from before_userid_table)""".stripMargin
    */

  val consumerInfoInc=
    """insert overwrite table consumer_base_info_inc
      |select max(distinct x.userid) as userid ,
      |max(x.createtime) as createtime ,
      |max(x.city) as city,
      |max(x.phone) as phone,
      |max(x.province) as province,
      |max(x.sourcetype) as sourcetype,
      |max(x.country) as country,
      |max(x.birthday) as birthday,
      |max(x.sex) as sex,
      |max(x.channel) as channel
      |from
      |(select distinct shoulv_userid as userid,
      |from_unixtime(user_createtime/1000,'yyyy-MM-dd') as createtime,
      |city,user_mobile as phone,province,sourcetype,country,
      |from_unixtime(user_birthday/1000,'yyyy-MM-dd') as birthday,
      |user_gender as sex,null as channel
      |from original_data_one where shoulv_userid not in
      |(select userid from before_userid_table)
      |) x group by x.userid""".stripMargin

  /*
  val consumerInfoParti=
    """insert into table consumer_base_info
      |select distinct shoulv_userid as userid,
      |from_unixtime(user_createtime/1000,'yyyy-MM-dd') as createtime,
      |city,user_mobile as phone,province,sourcetype,country,
      |from_unixtime(user_birthday/1000,'yyyy-MM-dd') as birthday,
      |user_gender as sex,null as channel,analysis_date as time
      |from original_data_one
      |where shoulv_userid not in
      |(select userid from before_userid_table)""".stripMargin
   */

  val consumerInfoParti=
    """insert into table consumer_base_info
      |select max(distinct x.userid) as userid ,
      |max(x.createtime) as createtime ,
      |max(x.city) as city,
      |max(x.phone) as phone,
      |max(x.province) as province,
      |max(x.sourcetype) as sourcetype,
      |max(x.country) as country,
      |max(x.birthday) as birthday,
      |max(x.sex) as sex,
      |max(x.channel) as channel,
      |max(x.time) as time
      |from
      |(select distinct shoulv_userid as userid,
      |from_unixtime(user_createtime/1000,'yyyy-MM-dd') as createtime,
      |city,user_mobile as phone,province,sourcetype,country,
      |from_unixtime(user_birthday/1000,'yyyy-MM-dd') as birthday,
      |user_gender as sex,null as channel,analysis_date as time
      |from original_data_one where shoulv_userid not in
      |(select userid from before_userid_table)
      |) x group by x.userid""".stripMargin
  val userId=
    """insert overwrite table before_userid_table
      |select userid from consumer_base_info""".stripMargin
  val actionInfoInc=
    """insert overwrite table total_action_info_one_inc
      |select distinct shoulv_userid as userid,
      |businesstype as primaryKind,actiontype as actionKind,
      |case when user_source='online' then '1' else '2' end as sourceKind,
      |ifnull(ent_id,0) as brandid,
      |ifnull(order_amount,0.00) as cost,analysis_date as time
      |from original_data""".stripMargin
  val actionInfoParti=
    """insert into table total_action_info_one
      |select distinct shoulv_userid as userid,
      |businesstype as primaryKind,actiontype as actionKind,
      |case when user_source='online' then '1' else '2' end as sourceKind,
      |ifnull(ent_id,0) as brandid,
      |ifnull(order_amount,0.00) as cost,analysis_date as time
      |from original_data""".stripMargin
  val actionCalcuTotal=
    """insert into table total_action_info_two
      |select shoulv_userid as userid,
      |businesstype as primaryKind,actiontype as actionKind,
      |ifnull(order_amount,0.00) as cost,analysis_date as time
      |from original_data""".stripMargin

  //todo---已经优化，都需要测
  val actionGroupCalcu=
    """select userid,primaryKind,actionKind,
      |count(*) as num,
      |sum(cost) as totalcost,
      |max(time) as lasttime,
      |min(time) as originaltime,
      |datediff(max(time),min(time)) timediff
      |from (
      |select * from total_action_info_two where time>date_sub(CURRENT_DATE(),180)
      |) x
      |group by x.userid,x.primaryKind,x.actionKind""".stripMargin
  //todo---待测
  val calcuFre=
    """select
      |SUBSTRING_INDEX(myMiddle(num),'#',1) as l,
      |SUBSTRING_INDEX(SUBSTRING_INDEX(myMiddle(num),'#',2),'#',-1) as m,
      |SUBSTRING_INDEX(myMiddle(num),'#',-1) as h
      |from count_group""".stripMargin

  val maxDate="""select max(time) as nowtime from total_action_info_two"""

  //todo---待测
  val calcuCost=
    """select
      |SUBSTRING_INDEX(myMiddle(totalcost),'#',1) as l,
      |SUBSTRING_INDEX(SUBSTRING_INDEX(myMiddle(totalcost),'#',2),'#',-1) as m,
      |SUBSTRING_INDEX(myMiddle(totalcost),'#',-1) as h
      |from count_group
      |where totalcost!=0.0""".stripMargin

  //todo---待测
  val actionCalcuFinal=
    """insert into table total_action_info_calcu
      |select userid,primaryKind,actionKind,
      |case when (totalcost+0.01)>(select h from C) then '1'
      |when (totalcost-0.01)<(select l from C) then '3'
      |else '2' end as costabilityKind,
      |case when lasttime>(select date_sub(nowtime,7) from B) then '1'
      |when lasttime<(select date_sub(nowtime,30) from B) then '3'
      |else '2' end as actionStatus,
      |case when (num+0.01)>(select h from A) then '1'
      |when (num-0.01)<(select l from A) then '3'
      |else '2' end as frequencyKind
      |from count_group""".stripMargin
  val totalActionInc=
    """insert overwrite table total_action_info_inc
      |select
      |ff.userid,
      |ff.primaryKind,
      |max(ff.actionKind) as actionKind,
      |max(ff.sourceKind) as sourceKind,
      |max(ff.brandid) as brandid,
      |sum(ff.cost) as cost,
      |max(ff.costabilityKind) as costabilityKind,
      |max(ff.actionStatus) as actionStatus,
      |max(ff.frequencyKind) as frequencyKind,
      |max(ff.time) as time
      |from
      |(
      |select x.* from (
      |select total_action_info_one_inc.userid,
      |total_action_info_one_inc.primaryKind,
      |total_action_info_one_inc.actionKind,
      |total_action_info_one_inc.sourceKind,
      |total_action_info_one_inc.brandid,
      |total_action_info_one_inc.cost,
      |total_action_info_calcu.costabilityKind,
      |total_action_info_calcu.actionStatus,
      |total_action_info_calcu.frequencyKind,
      |total_action_info_one_inc.time
      |from total_action_info_one_inc
      |join
      |total_action_info_calcu
      |on
      |total_action_info_one_inc.userid=total_action_info_calcu.userid
      |and total_action_info_one_inc.primaryKind=total_action_info_calcu.primaryKind
      |and total_action_info_one_inc.actionKind=total_action_info_calcu.actionKind
      |) x
      |inner join (select y.userid,max(y.time) as time from
      |(
      |select total_action_info_one_inc.userid,
      |total_action_info_one_inc.primaryKind,
      |total_action_info_one_inc.actionKind,
      |total_action_info_one_inc.sourceKind,
      |total_action_info_one_inc.brandid,
      |total_action_info_one_inc.cost,
      |total_action_info_calcu.costabilityKind,
      |total_action_info_calcu.actionStatus,
      |total_action_info_calcu.frequencyKind,
      |total_action_info_one_inc.time
      |from total_action_info_one_inc
      |join
      |total_action_info_calcu
      |on
      |total_action_info_one_inc.userid=total_action_info_calcu.userid
      |and total_action_info_one_inc.primaryKind=total_action_info_calcu.primaryKind
      |and total_action_info_one_inc.actionKind=total_action_info_calcu.actionKind
      |) y group by y.userid
      |) z
      |on x.userid = z.userid and x.time = z.time
      |) ff group by ff.userid,ff.primaryKind""".stripMargin
  //todo---测试
  val totalActionParti=
    """insert into table total_action_info
      |select
      |ff.userid,
      |ff.primaryKind,
      |max(ff.actionKind) as actionKind,
      |max(ff.sourceKind) as sourceKind,
      |max(ff.brandid) as brandid,
      |sum(ff.cost) as cost,
      |max(ff.costabilityKind) as costabilityKind,
      |max(ff.actionStatus) as actionStatus,
      |max(ff.frequencyKind) as frequencyKind,
      |max(ff.time) as time
      |from
      |(
      |select x.* from (
      |select total_action_info_one_inc.userid,
      |total_action_info_one_inc.primaryKind,
      |total_action_info_one_inc.actionKind,
      |total_action_info_one_inc.sourceKind,
      |total_action_info_one_inc.brandid,
      |total_action_info_one_inc.cost,
      |total_action_info_calcu.costabilityKind,
      |total_action_info_calcu.actionStatus,
      |total_action_info_calcu.frequencyKind,
      |total_action_info_one_inc.time
      |from total_action_info_one_inc
      |join
      |total_action_info_calcu
      |on
      |total_action_info_one_inc.userid=total_action_info_calcu.userid
      |and total_action_info_one_inc.primaryKind=total_action_info_calcu.primaryKind
      |and total_action_info_one_inc.actionKind=total_action_info_calcu.actionKind
      |) x
      |inner join (select y.userid,max(y.time) as time from
      |(
      |select total_action_info_one_inc.userid,
      |total_action_info_one_inc.primaryKind,
      |total_action_info_one_inc.actionKind,
      |total_action_info_one_inc.sourceKind,
      |total_action_info_one_inc.brandid,
      |total_action_info_one_inc.cost,
      |total_action_info_calcu.costabilityKind,
      |total_action_info_calcu.actionStatus,
      |total_action_info_calcu.frequencyKind,
      |total_action_info_one_inc.time
      |from total_action_info_one_inc
      |join
      |total_action_info_calcu
      |on
      |total_action_info_one_inc.userid=total_action_info_calcu.userid
      |and total_action_info_one_inc.primaryKind=total_action_info_calcu.primaryKind
      |and total_action_info_one_inc.actionKind=total_action_info_calcu.actionKind
      |) y group by y.userid
      |) z
      |on x.userid = z.userid and x.time = z.time
      |) ff group by ff.userid,ff.primaryKind""".stripMargin

  //todo---优化数据库存储，待删
  val insightInc=
    """insert overwrite table Insight_table_inc
      |select distinct shoulv_userid as userid,
      |analysis_date as time,
      |case when source='APP' then '1'
      |when source='H5' then '2'
      |else '3' end as source,
      |user_gender as sex,
      |case when user_source='online' then '1'
      |else '2' end as sourceKind,
      |province
      |from original_data""".stripMargin
  //todo---优化数据库存储，待删
  val insight=
    """insert into table Insight_table
      |select distinct shoulv_userid as userid,
      |case when source='APP' then '1'
      |when source='H5' then '2'
      |else '3' end as source,
      |user_gender as sex,
      |case when user_source='online' then '1'
      |else '2' end as sourceKind,
      |province,
      |analysis_date as time
      |from original_data""".stripMargin



}

//todo---选择最新记录中最大actionKind的SQL
//select userid,primaryKind,max(actionKind) as actionKind,sum(cost) as cost,max(time) as time,
//max(brandid) as brandid,max(costabilityKind) as costabilityKind,max(actionStatus) as actionStatus,
//max(frequencyKind) as frequencyKind,max(sourceKind) as sourceKind
//from
//(select total_action_info.* from total_action_info
//inner join (select userid, max(time) as time from total_action_info
//group by userid ) b on total_action_info.userid = b.userid and total_action_info.time = b.time) x
//group by userid,primaryKind;