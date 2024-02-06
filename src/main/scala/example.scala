import org.apache.spark.{SparkConf, SparkContext}

object example {
  def main(args: Array[String]): Unit = {
    // SparkConf를 생성하여 애플리케이션 설정을 정의합니다.
    val conf = new SparkConf()
      .setAppName("example")
      .setMaster("local[*]") // 로컬 모드에서 실행합니다.

    // SparkContext 인스턴스를 생성합니다.
    val sc = new SparkContext(conf)


    /*
      1. coalesce
        - RDD(Resilient Distributed Dataset)의 파티션 수를 재조정
        - RDD는 여러 개의 파티션으로 이루어져 있고, 하나의 파티션은 동일한 타입의 여러 개의 객체들로 이루어져 있는 분산 데이터 집합
        - coalesce 는 줄이는 용도로 주로 사용, 강제로 셔플을 수행하라는 옵션을 지정하지 않는 한 셔플을 사용하지 않음, 비용이 적게 듬
     */
    val rdd_1 = sc.parallelize(1 to 100, 5)
    val repartitionedRDD_1 = rdd_1.coalesce(10)
    println("2 >> " + repartitionedRDD_1.partitions.length) // 출력: 10

    /*
      2. repartition
        - RDD(Resilient Distributed Dataset)의 파티션 수를 재조정
        - coalesce 는 줄이는 용도로 주로 사용, 강제로 셔플을 수행하라는 옵션을 지정하지 않는 한 셔플을 사용하지 않음
        - repartition 은 줄이거나 늘리는 용도로 주로 사용, 셔플을 기반으로 동작, 비용이 많이 듬
     */
    val rdd_2 = sc.parallelize(1 to 100, 5)
    val repartitionedRDD = rdd_2.repartition(10)
    println("2 >> " + repartitionedRDD.partitions.length) // 출력: 10

    /*
      3. flatmap
        - 각 요소에 함수를 적용하고, 결과들을 단일 컬렉션으로 병합
     */
    val list_3 = List("Hello world", "How are you")
    val result_3 = list_3.flatMap(_.split(" "))
    println("3 >> " + result_3) // 출력: List(Hello, world, How, are, you)

    /*
      4. map
        - 컬렉션의 각 요소에 함수를 적용하고, 결과를 새 컬렉션에 저장
     */
    val list_4 = List(1, 2, 3, 4, 5)
    val result_4 = list_4.map(_ * 2)
    println("4 >> " + result_4) // 출력: List(2, 4, 6, 8, 10)

    /*
      5. mapPartitions
        - 각 파티션에 함수를 적용하고 결과를 새 RDD로 반환
     */
    val rdd_5 = sc.parallelize(1 to 100, 5)
    val result = rdd_5.mapPartitions(iter => List(iter.sum).iterator)
    println("5 >> " + result.collect().toList) // 출력: List(210, 610, 1010, 1410, 1810)


    /*
      6. forEach
        - 컬렉션의 각 요소에 함수를 적용
     */
    val list = List(1, 2, 3, 4, 5)
    print("6 >> ")
    list.foreach(println)
    /*
      출력:
        1
        2
        3
        4
        5
     */

    /*
      7. foreachPartition
        - 각 파티션에 함수를 적용
     */
    val rdd_7 = sc.parallelize(1 to 100, 5)
    print("7 >> ")
    rdd_7.foreachPartition(iter => println(iter.sum))
    /*
      출력:
        610
        1010
        210
        1810
        1410
     */
  }
}