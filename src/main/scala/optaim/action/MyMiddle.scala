package optaim.action

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object MyMiddle extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  override def bufferSchema: StructType = {
    StructType(StructField("str", StringType)  :: Nil)
    //:: StructField("count", LongType)
  }

  override def dataType: DataType = StringType//ArrayType(elementType = LongType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
    //buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      if(buffer.getString(0)==""){
        buffer(0) = buffer.getString(0) + "" + input.getLong(0)
        //buffer(1) = buffer.getString(1) + "" + input.getLong(1)
      }else{
        buffer(0) = buffer.getString(0) + "," + input.getLong(0)
        //buffer(1) = buffer.getString(1) + "" + input.getLong(1)
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
    //buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    val a=buffer.getString(0).split(",").toList.sortWith(java.lang.Double.parseDouble(_)<java.lang.Double.parseDouble(_))
    val aa=buffer.getString(0).split(",").toList.sortWith(java.lang.Double.parseDouble(_)>java.lang.Double.parseDouble(_))
    val b=a.length
    if(b % 2 == 0){
      if(b % 4 == 0){
        println("1")
        ((a(b/4).toDouble+a(b/4-1).toDouble)/2).toString + "#" + ((a(b/2).toDouble+a(b/2-1).toDouble)/2).toString + "#" + ((aa(b/4).toDouble+aa(b/4-1).toDouble)/2).toString
      }else{
        println("2")
        (a(b/4)) + "#" + ((a(b/2).toDouble+a(b/2-1).toDouble)/2).toString + "#" + (aa(b/4))
      }
    }else{
      if((b / 2) % 2 != 0){
        println("3")
        (a(b/4)) + "#" + a(b/2) + "#" + (aa(b/4))
      }else{
        println("4")
        ((a(b/4).toDouble+a(b/4-1).toDouble)/2).toString + "#" + a(b/2) + "#" + ((aa(b/4).toDouble+aa(b/4-1).toDouble)/2).toString
      }
    }

  }
}