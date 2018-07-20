package com.zhiyou.bd23
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import SparkContextTest.sc

import scala.collection.mutable.ListBuffer
/**
  * Created by ThinkPad on 2018/7/18.
  */
case class Transaction(date:String,time:String,customerId:String,productId:String,quantity:Int,price:Double)

object RDDApiTest {
  val fileTransaction = sc.textFile("/spark01/ch04_data_transactions.txt",3)

  def mapTest() = {
    //RDD[String]------->RDD[Transaction]
    println(fileTransaction.count())  //得到fileTransaction的记录数
    //每一条记录都会被执行一遍map的算子
    val transaction = fileTransaction.map(x=>{
      val treg = "(.*)#(.*)#(\\d+)#(\\d+)#(\\d+)#(.*)".r   //rdd中每个元素在转换时都会重新创建一个treg对象
      x match{
        case treg(c1,c2,c3,c4,c5,c6) => Transaction(c1,c2,c3,c4,c5.toInt,c6.toDouble)
        case _ => null
      }
    })
    transaction.foreach(println)
    println(transaction.count())
  }
  //返回的是RDD
  def mapPartitionTest() = {
    //一个rdd的每个partition上只执行一次mapPartition的算子
    val transaction = fileTransaction.mapPartitions(x=>{
      val treg = "(.*)#(.*)#(\\d+)#(\\d+)#(\\d+)#(.*)".r  //一个分区共用同一个treg对象
      for(item <- x) yield {
        item match{
          case treg(c1,c2,c3,c4,c5,c6) => Transaction(c1,c2,c3,c4,c5.toInt,c6.toDouble)
          case _ => null
        }
      }
    })
    transaction
  }

  //跟mappartition一样，只不过多了一个可以在算子中得到partition的编号
  def mapPartitionsWithIndexTest() = {
    val result = fileTransaction.mapPartitionsWithIndex((pi,partitionItems)=>{
      for(item <- partitionItems) yield {
        s"$pi------$item"
      }
    })
    //    result.foreach(println)
    //    result.collect().foreach(println)
    result.take(10).foreach(println)
  }
  //flatmapTest,类似hive 里的explode，列转行
  def flatMapTest() = {
    //姓名，学科，成绩
    val list = List("张三 语文:80,数学:90,英语:70","李四 语文:33,数学:96","王五 语文:55,数学:22,生物:44")
    val rdd = sc.parallelize(list)
    val result = rdd.flatMap(x=>{
      val infos = x.split(" ")
      val classAndScore = infos(1)
      for(cas <- classAndScore.split(",")) yield {
        s"${infos(0)},${cas.replace(":",",")}"
      }
    })
    result.foreach(println)
  }
  //提取出交易数量大于7个以上的记录
  def filterTest() = {
    val transaction = mapPartitionTest()
    val result = transaction.filter(x=>x.quantity>7)
    //    result.foreach(println)
    //使用mapPartition实现filter
    val result1 = transaction.mapPartitions(x=>{
      val list = ListBuffer[Transaction]()
      for(t <- x){
        if(t.quantity>7){
          list += t
        }
      }
      list.toIterator
    })
    result1.foreach(println)
  }
  //取出交易信息表中的客户id，要求去重
  def distinctTest() = {
    val transaction = mapPartitionTest()
    val distinctCustomerIdRdd = transaction.map(x=>x.customerId).distinct()
    distinctCustomerIdRdd.foreach(println)
  }
  //groupby去重,效率低
  def groupByTest() = {
    val transaction = mapPartitionTest()
    val result = transaction.groupBy(x=>x.customerId)
    result.foreach(x=>println(x._1))
  }
  //---------------------------
  /**
    * 排序
    * */
  def sortByTest()={
    val transaction = mapPartitionTest()
    //全排序
//    val resultRdd = transaction.sortBy(x=>x.quantity)
//    resultRdd.foreach(println)

    //二次排序
    val resultRdd2 = transaction.sortBy(x=>(x.quantity,x.price))
    resultRdd2.foreach(println)

  }

  //求价格最高top5
  def topNTest()={

    val transaction = mapPartitionTest()
    //降序
    val topn1 = transaction.map(x=>x.price).top(5)
    //升序
    val topn2 = transaction.map(x=>x.price).takeOrdered(5)
    println(topn1.mkString(","))
    println(topn2.mkString(","))

    val topn11 = transaction.top(5)(new Ordering[Transaction]{

      override def compare(x: Transaction, y: Transaction): Int = x.price.compare(y.price)
    })
    println("--------->"+topn11.mkString(","))
  }

  /**
    * 集合测试
    * */
  def collectTest() = {
    val rdd1 = sc.parallelize(List(1,3,5,6,7,9))
    val rdd2 = sc.parallelize(List(2,3,4,6,8,10))
    val rdd3 = sc.parallelize(List("a","b","c","d"))
    //交集
    val rdd4 = rdd1.intersection(rdd2)
    //    rdd4.foreach(println)
    //合集
    val rdd5 = rdd1.union(rdd2)
    //    rdd5.foreach(println)
    //减集
    val rdd6 = rdd1.subtract(rdd2)
    //    rdd6.foreach(println)
    //笛卡尔乘积
    val rdd7 = rdd1.cartesian(rdd3)
    rdd7.foreach(println)
  }
  //拉链操作
  def zipTest() = {
    val list1 = List(1,2,3,4,5)
    val list2 = List("a","b","c","d","e","f")
    val list3 = List("a","b","c","d","e")
    val result = list2.zip(list1)//两个list元素数不一样可以zip
    //    result.foreach(println)


    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)
    val rdd3 = rdd1.zip(rdd2)//两个rdd的元素数不一样，执行zip会报错
    val rdd31 = rdd1.zipPartitions(rdd2)((p1,p2)=>p1.toList.zip(p2.toList).toIterator)
    rdd31.foreach(println)
    val rdd4 = rdd1.zip(sc.parallelize(list3))
    //    rdd4.foreach(println)

    //元素的位置
    val rdd5 = sc.parallelize(List("a","b","c","d","e","f","g","h","i","j"), 3)
    val rdd6 = rdd5.zipWithIndex()
    //    rdd6.foreach(println)
    val rdd7 = rdd5.zipWithUniqueId()
    //    rdd7.foreach(println)
  }
  //缓存
  def cacheTest() = {
    val transaction = mapPartitionTest()
    //缓存transaction
    transaction.cache()
    //    transaction.persist()  等同于cache，缓存在内存中
    //    transaction.persist(StorageLevel.MEMORY_AND_DISK) 使用MEMORY_AND_DISK级别缓存rdd
    //多次使用transaction
    val startTime = System.currentTimeMillis()
    transaction.foreach(println)
    println(transaction.count())

    println(System.currentTimeMillis() - startTime)
    //取消缓存
    transaction.unpersist()
  }
  //重新分区
  def repartitionTest() = {
    val rdd = sc.parallelize(List("a","b","c","d","e","f","g","h"),4)
    rdd.saveAsTextFile("/sparkapitest/rep0")
    val coalesce = rdd.coalesce(2)
    coalesce.saveAsTextFile("/sparkapitest/rep1")
    val repartition = rdd.repartition(2)
    repartition.saveAsTextFile("/sparkapitest/rep2")
    val repartition1 = rdd.repartition(4)
    repartition1.saveAsTextFile("/sparkapitest/rep3")
  }

  //--------------------------------------------
  //聚合函数
  //1.计算transaction文件中的总交易额
  def calculateTotalAmount()={
    //交易金额 quantity * price

    val transaction = mapPartitionTest()
    val mapAmount = transaction.map(x=>BigDecimal.valueOf(x.quantity*x.price))
    val totalAmount = mapAmount.reduce((x1,x2)=>x1+x2)

    println(s"--->总交易额：${totalAmount}")

  }
  //2.计算transaction最大交易金额
  def maxAmount()={
    val transaction = mapPartitionTest()
    val maxAmount = transaction.max()(new Ordering[Transaction]{
      override def compare(x: Transaction, y: Transaction): Int = (x.quantity *x.price).compare(y.quantity*y.price)
    })
    println(s"最大交易金额：${maxAmount.quantity*maxAmount.price}")
  }
  //3.计算出交易发生的最小的时间
  //TODO: 结果有问题，待解决
  def earliesTransactionTime()={

    val transaction = mapPartitionTest()

    val dataFormater = new SimpleDateFormat("yyyy-MM-dd hh:mm aa", Locale.ENGLISH)

    val dataRdd = transaction.map(x=>{
      println("--------->>"+dataFormater.parse(s"${x.date} ${x.time}"))

      dataFormater.parse(s"${x.date} ${x.time}")
    })

    val resultRdd = dataRdd.min()(new Ordering[Date]{
      override def compare(x: Date, y: Date): Int = x.compareTo(y)
    })

    println(s"交易发生最小时间：${resultRdd}")
  }
  //4.把购买过productId=72的用户id合并成一个字符串：customerId0,customerId1,...
  def findUserBuyProduct()={

    val transaction = mapPartitionTest()
    val resultRdd = transaction.filter(x=>x.productId.equals("72"))

    val resultMapRdd = resultRdd.map(x=>(x.customerId))
    val result = resultMapRdd.reduce((x1,x2)=>s"$x1,$x2")

    println(s"购买productId =72 的用户名单：$result")
  }
  //5.把customerId=58的用户的订单信息合并成(productId0:amount0,productId1:amount1,...)
  def findUserOrders()={

    val transaction = mapPartitionTest()
    val resultFilterRdd = transaction.filter(x=>x.customerId.equals("58"))
    val resultRdd = resultFilterRdd.map(x=> s"${x.productId}:${x.quantity*x.price}")
    val result = resultRdd.reduce((x1,x2)=>s"$x1,$x2")

    println(s"customerId为58 的用户订单：$result")

  }
  /**
    * 2.
    * */
  def main(args: Array[String]): Unit = {
    //    mapPartitionTest()
    //    mapPartitionsWithIndexTest()
    //    flatMapTest()
    //    filterTest()
    //    distinctTest()
//    groupByTest()

//    sortByTest()
//    topNTest()

//    calculateTotalAmount()
//    maxAmount()
    earliesTransactionTime()
//    findUserBuyProduct()
//    findUserOrders()
  }
}
