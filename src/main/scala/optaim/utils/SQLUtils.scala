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
  val consumerInfoInc1=
    """insert overwrite table consumer_base_info_inc_one
      |select distinct shoulv_userid as userid,
      |from_unixtime(user_createtime/1000,'yyyy-MM-dd') as createtime,
      |city,user_mobile as phone,province,sourcetype,country,
      |from_unixtime(user_birthday/1000,'yyyy-MM-dd') as birthday,
      |user_gender as sex,null as channel
      |from original_data_one where shoulv_userid not in
      |(select userid from before_userid_table)""".stripMargin
  val consumerInfoInc=
    """insert overwrite table consumer_base_info_inc
      |select max(distinct userid) as userid ,
      |max(createtime) as createtime ,
      |max(city) as city,
      |max(phone) as phone,
      |max(province) as province,
      |max(sourcetype) as sourcetype,
      |max(country) as country,
      |max(birthday) as birthday,
      |max(sex) as sex,
      |max(channel) as channel
      |from consumer_base_info_inc_one group by userid""".stripMargin
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
  val actionGroupCalcu=
    """select userid,primaryKind,actionKind,
      |count(*) as num,
      |sum(cost) as totalcost,
      |max(time) as lasttime,
      |min(time) as originaltime,
      |count(time) timediff
      |from total_action_info_two
      |group by userid,primaryKind,actionKind""".stripMargin
  //todo-------------------------------------------------------
  val calcuFre=
    """select
      |SUBSTRING_INDEX(myMiddle(num/(timediff+1.0)),'#',1) as l,
      |SUBSTRING_INDEX(SUBSTRING_INDEX(myMiddle(num/(timediff+1.0)),'#',2),'#',-1) as m,
      |SUBSTRING_INDEX(myMiddle(num/(timediff+1.0)),'#',-1) as h
      |from count_group""".stripMargin
  //------------------------------------------------------------
  val maxDate="""select max(time) as nowtime from total_action_info_two"""

  //todo-------------------------------------------------------
  val calcuCost=
    """select
      |SUBSTRING_INDEX(myMiddle(totalcost),'#',1) as l,
      |SUBSTRING_INDEX(SUBSTRING_INDEX(myMiddle(totalcost),'#',2),'#',-1) as m,
      |SUBSTRING_INDEX(myMiddle(totalcost),'#',-1) as h
      |from count_group
      |where totalcost!=0.0""".stripMargin
  //-------------------------------------------------------------

  val actionCalcuFinal=
    """insert into table total_action_info_calcu
      |select userid,primaryKind,actionKind,
      |case when totalcost>(select h from C) then '1'
      |when totalcost<(select l from C) then '3'
      |else '2' end as costabilityKind,
      |case when lasttime>(select date_sub(nowtime,7) from B) then '1'
      |when lasttime<(select date_sub(nowtime,30) from B) then '3'
      |else '2' end as actionStatus,
      |case when (num/(timediff+1.0))>(select h from A) then '1'
      |when (num/(timediff+1.0))<(select l from A) then '3'
      |else '2' end as frequencyKind
      |from count_group""".stripMargin
  val totalActionInc=
    """insert overwrite table total_action_info_inc
      |select total_action_info_one_inc.userid,
      |total_action_info_one_inc.primaryKind,
      |total_action_info_one_inc.actionKind,
      |total_action_info_one_inc.sourceKind,
      |total_action_info_one_inc.brandid,
      |total_action_info_one_inc.cost,
      |total_action_info_calcu.costabilityKind,
      |total_action_info_calcu.actionStatus,
      |total_action_info_calcu.frequencyKind,
      |total_action_info_one_inc.time from total_action_info_one_inc
      |join
      |total_action_info_calcu
      |on
      |total_action_info_one_inc.userid=total_action_info_calcu.userid
      |and total_action_info_one_inc.primaryKind=total_action_info_calcu.primaryKind
      |and total_action_info_one_inc.actionKind=total_action_info_calcu.actionKind""".stripMargin
  val totalActionParti=
    """insert into table total_action_info
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
      |and total_action_info_one_inc.actionKind=total_action_info_calcu.actionKind""".stripMargin
  val insightInc=
    """insert overwrite table Insight_table_inc
      |select distinct shoulv_userid as userid,
      |analysis_date as time,
      |case when source='APP' then '1'
      |when source='H5' then '2'
      |else '3' end as source,
      |case when user_source='online' then '1'
      |else '2' end as sourceKind,
      |user_gender as sex,province
      |from original_data""".stripMargin
  val insight=
    """insert into table Insight_table
      |select distinct shoulv_userid as userid,
      |analysis_date as time,
      |case when source='APP' then '1'
      |when source='H5' then '2'
      |else '3' end as source,
      |case when user_source='online' then '1'
      |else '2' end as sourceKind,
      |user_gender as sex,province
      |from original_data""".stripMargin

  //todo
  //1.修改channel，将其换到可变化的数据库中
  //2.对计算数据那块用中位数代替均值
  //3.求每个业务中actionKind最大的，将重复数据去重，每个actionKind对应的userid只有一个
  //4.修改洞察那块
  //5.有效性那块利用分位数进行代替

}

//todo---选择最新记录中最大actionKind的SQL
//select userid,primaryKind,max(actionKind) as actionKind,sum(cost) as cost,max(time) as time from
//(select total_action_info_copy1.* from total_action_info_copy1
//inner join (select userid, max(time) as time from total_action_info_copy1
//group by userid ) b on total_action_info_copy1.userid = b.userid and total_action_info_copy1.time = b.time) x
//group by userid,primaryKind;