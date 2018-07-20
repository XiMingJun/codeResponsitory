package com.zhiyou.bd23
import SparkContextTest.sc
import org.apache.spark.Partitioner
/**
  * Created by ThinkPad on 2018/7/20.
  */
case class Product(id:Int,name:String,price:Double,category:Int)
object PairRDDApiTest {
  //k-v rdd的映射（转换）方法
  def mapTest() = {
    val kvRdd1 = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"d"),(6,"e"),(3,"e")))
    val rdd1 = sc.parallelize(List(1,2,3,4,5))
    val rdd2 = sc.parallelize(List("a,f,k","b,g","c,x,z","d,u","e,i"))
    val kvRdd2 = rdd1.zip(rdd2)

    val keys = kvRdd1.keys
    val values = kvRdd2.values
    //    keys.foreach(println)
    //    values.foreach(println)
    val map1 = kvRdd1.mapValues(x=>x.toUpperCase)
    //    map1.foreach(println)

//    kvRdd2.foreach(println)

    val map2 = kvRdd2.flatMapValues(x=>x.split(","))
        map2.foreach(println)

    val lookup = kvRdd1.lookup(3)
//    lookup.foreach(println)
  }
  //根据key做减集
  def substractTest() = {
    val kvRdd1 = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"d"),(6,"e"),(3,"e")))
    val kvRdd2 = sc.parallelize(List(2->"f",3->"g",4->"d"))
    val subRdd = kvRdd1.subtractByKey(kvRdd2)
    subRdd.foreach(println)
  }
  //重分区
  def partitionByTest() = {
    val kvRdd1 = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"d"),(6,"e"),(3,"e")),3)
    val repartition = kvRdd1.partitionBy(new Partitioner {
      override def getPartition(key: Any): Int = {
        key.asInstanceOf[Int]%2
      }
      override def numPartitions: Int = 2
    })
    repartition.saveAsTextFile("/sparkapitest/rep4")
  }
  //加载ch04_data_products文件，把数据按照产品类型分组
  def groupByKeyTest() = {
    val fileRdd = sc.textFile("/external/transaction/ch04_data_products.txt")
    val productRdd = fileRdd.map(x=>{
      val regx = "(\\d+)#(.*)#(.*)#(\\d+)".r
      x match{
        case regx(c1,c2,c3,c4) => Product(c1.toInt,c2,c3.toDouble,c4.toInt)
        case _ => null
      }
    }).filter(x=>x!=null).keyBy(_.category)

    val grpbkey = productRdd.groupByKey()
    //    grpbkey.foreach(x=>{
    //      println(s"${x._1}-------------------")
    //      for(product <- x._2) println(s"    ${product}")
    //    })
    //使用groupby计算出，每个品类下的商品个数
    val cagegoryCount = grpbkey.mapValues(v=>v.size)
    //    cagegoryCount.foreach(println)
  }
  //先分组再关联：cogroup等同于groupWith
  def cogroupTest() = {
    val kvRdd1 = sc.parallelize(List((1,"a"),(1,"b"),(2,"c"),(2,"d"),(3,"a"),(3,"f"),(4,"e")),1)
    val kvRdd2 = sc.parallelize(List((1,"A"),(1,"C"),(2,"R"),(2,"T"),(3,"I"),(3,"U"),(5,"X")),1)
    val result = kvRdd1.cogroup(kvRdd2)
    //cogroup 关联不上的key会相互的补空
    result.foreach(x=>{
      println(s"${x._1}--------------------")
      println(s"${x._2}")
    })
  }
  def joinTest() = {
    val kvRdd1 = sc.parallelize(List((1,"a"),(1,"b"),(2,"c"),(2,"d"),(3,"a"),(3,"f"),(4,"e")),1)
    val kvRdd2 = sc.parallelize(List((1,"A"),(1,"C"),(2,"R"),(2,"T"),(3,"I"),(3,"U"),(5,"X")),1)
    //innerjoin
    val ijoin = kvRdd1.join(kvRdd2) //等同于kvRdd2.join(kvRdd1)，因为两个数据集的地位是平等的
    //    ijoin.foreach(println)
    val ljoin = kvRdd1.leftOuterJoin(kvRdd2)  //用kvRdd1的key去过滤kvRdd2中的key，2有1没有的不会出现在结果集中，1有2没有的会在结果集中
    val ljoin1 = kvRdd2.leftOuterJoin(kvRdd1) //用kvRdd2的key去过滤kvRdd1中的key，2有1没有的会出现在结果集中，1有2没有的不会在结果集中
    //    ljoin1.foreach(println)
    //fullOutterJoin
    val fjoin = kvRdd1.fullOuterJoin(kvRdd2)
    //    fjoin.foreach(println)

    //求出product表和transaction表的价格对比
    //要求输出：transactionId，quantity，productId，productName，transactionPrice，productPrice
    val productRdd = sc.textFile("/external/transaction/ch04_data_products.txt")
      .map(x=>{
        val regx = "(\\d+)#(.*)#(.*)#(\\d+)".r
        x match{
          case regx(c1,c2,c3,c4) => Product(c1.toInt,c2,c3.toDouble,c4.toInt)
          case _ => null
        }
      }).filter(x=>x!=null)
    val transactionRdd = RDDApiTest.mapPartitionTest()
    val result = productRdd.keyBy(_.id).join(transactionRdd.keyBy(_.productId.toInt))
      .mapValues(x=>{
        (x._2.customerId,x._2.quantity,x._1.id,x._1.name,x._2.price,x._1.price)
      }).map(x=>x._2)
    result.foreach(println)
  }
  def getProductRdd() = {
    val productRdd = sc.textFile("/external/transaction/ch04_data_products.txt")
      .map(x=>{
        val regx = "(\\d+)#(.*)#(.*)#(\\d+)".r
        x match{
          case regx(c1,c2,c3,c4) => Product(c1.toInt,c2,c3.toDouble,c4.toInt)
          case _ => null
        }
      }).filter(x=>x!=null)
    productRdd
  }
  //计算每个品类下商品的个数
  def countByKeyTest() = {
    val productRdd = getProductRdd()
    val categoryCount = productRdd.keyBy(_.category).countByKey()
    categoryCount.foreach(println)
  }
  //统计出每个客户的销售总金额
  def reduceByKeyTest() = {
    val transaction = RDDApiTest.mapPartitionTest()
    val result = transaction.map(x=>(x.customerId,x.quantity*x.price))  //必须把输入的value类型和输出的value类型转换成一致才行
      .reduceByKey((v1,v2)=>v1+v2)
    result.foreach(println)
  }
  def foldByKeyTest() = {
    val transaction = RDDApiTest.mapPartitionTest()
    val result = transaction.map(x=>(x.customerId,x.quantity*x.price))  //必须把输入的value类型和输出的value类型转换成一致才行
      .foldByKey(0.00)((v1,v2)=>v1+v2)
    result.foreach(println)
  }
  //aggregate不要求输入的value类型和输出的value一致才行
  def aggregateByKeyTest() = {
    val transaction = RDDApiTest.mapPartitionTest()
    val result = transaction.keyBy(_.customerId)
      .aggregateByKey(0.00)(
        (c,v)=>c+v.price*v.quantity,       //分区内部的聚合，会使用到初始值
        (c1,c2)=>c1+c2                    //分区之间的聚合，不会使用到初始值
      )
    result.foreach(println)
  }
  /**
    * 计算每个客户的交易总金额和交易次数以及最小交易金额 （amount,times，minamount）
    * */
  def combineByKeyTest() = {
    val transaction = RDDApiTest.mapPartitionTest()
    val result = transaction.keyBy(_.customerId)
      .combineByKey(
        (v:Transaction) =>(v.price*v.quantity,1,v.price*v.quantity),   //产生初始值，输入是每个分区的第一个元素
        (c:(Double,Int,Double),v:Transaction) => (c._1+v.price*v.quantity,c._2+1,if(c._3>v.price*v.quantity) v.price*v.quantity else c._3),
        (c1:(Double,Int,Double),c2:(Double,Int,Double)) => (c1._1+c2._1,c1._2+c2._2,if(c1._3>c2._3) c2._3 else c1._3)
      )
    result.foreach(println)
  }
  def main(args: Array[String]): Unit = {
        mapTest
    //    substractTest()
    //    partitionByTest()
    //    groupByKeyTest()
    //    cogroupTest
    //    joinTest
    //    countByKeyTest()
    //    foldByKeyTest


    //    val rdd = sc.parallelize(List(1,1,1,1,1),2)
    //需要输入初值的两个方法fold和aggregate的在两个聚合的阶段，都共用了同一个初始值
    //    val result = rdd.fold(1)((x1,x2)=>x1+x2)
    //    val result = rdd.aggregate(1)((c,v)=>c+v,(c1,c2)=>c1+c2)
    //    println(result)
    //对于kv的rdd的foldByKey和aggregateByKey在两个聚合阶段，只在分区内部聚合的时候会用到给定初始值，在分区之间聚合的时候不使用给定的初始值
    //而是把第1个分区的聚合值当做初始值
    //    val rdd = sc.parallelize(List(("a",1),("a",1),("a",1),("a",1),("b",1),("b",1)),1)
    //    val result = rdd.foldByKey(1)((x1,x2)=>x1+x2)
    //    val result = rdd.aggregateByKey(1)(
    //      (c,v)=>c+v,
    //      (c1,c2)=>c1+c2
    //    )
    //    result.foreach(println)

    //    aggregateByKeyTest()
//    combineByKeyTest()
  }
}
