package optaim.action

import java.io.File

import optaim.utils.SQLUtils
import org.apache.spark.sql.SparkSession


object DealWithData {


  def main(args: Array[String]) {
    val warehouseLocation = new File("spark-warehouse")
      .getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName(s"DealWithData")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    spark.read.format("CSV")
      .option("header","true")
      .load(args(0))
      .createOrReplaceTempView("original_data_one")
    spark.read.format("CSV")
      .option("header","true")
      .load(args(0))
      .filter("businesstype!=0")
      .createOrReplaceTempView("original_data")

    //todo
    //注册udf临时函数
    spark.udf.register("myMiddle",MyMiddle)

    import spark.sql

    sql(SQLUtils.useDatabase)

    //设置hive插入分区模式
    sql(SQLUtils.modeSet)

    sql(SQLUtils.originalData)

    //品牌信息增量表
    sql(SQLUtils.brandInc)

    //品牌信息分区数据插入
    sql(SQLUtils.brandParti)

    //每次更新历史brandid全量表
    sql(SQLUtils.braandId)

    //sql(SQLUtils.consumerInfoInc1)

    //基础信息增量表
    sql(SQLUtils.consumerInfoInc)

    //基础信息分区表
    sql(SQLUtils.consumerInfoParti)

    // 每次更新历史userid全量表
    sql(SQLUtils.userId)


    //事件信息增量表
    sql(SQLUtils.actionInfoInc)


    //事件信息分区表
    sql(SQLUtils.actionInfoParti)

    //追加事件数据到事件指标计算的表中
    sql(SQLUtils.actionCalcuTotal)

    //计算事件相关信息
    sql(SQLUtils.actionGroupCalcu).createOrReplaceTempView("count_group")

    sql(SQLUtils.calcuFre).createOrReplaceTempView("A")

    sql(SQLUtils.maxDate).createOrReplaceTempView("B")

    sql(SQLUtils.calcuCost).createOrReplaceTempView("C")

    sql(SQLUtils.actionCalcuFinal)

    //事件信息增量表
    sql(SQLUtils.totalActionInc)

    //事件信息分区表
    sql(SQLUtils.totalActionParti)

    //用户洞察所需增量表
    sql(SQLUtils.insightInc)

    //用户洞察所需分区表
    sql(SQLUtils.insight)
  }

}
