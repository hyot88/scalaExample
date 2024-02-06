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
        - RDD는 Apache Spark 에서 사용되는 기본적인 데이터 구조이며 불변성을 가집니다.
        - RDD는 클러스터의 노드에 분할된 데이터의 컬렉션으로, 병렬로 작업을 수행할 수 있습니다.
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

    /*
      8. reduce
        - 컬렉션의 모든 요소를 단일 값으로 축소
     */
    case class Grade(student: String, subject: String, score: Int)

    val grades_8 = List(
      Grade("Alice", "Math", 85),
      Grade("Bob", "Math", 90),
      Grade("Charlie", "Math", 95),
      Grade("Alice", "English", 88),
      Grade("Bob", "English", 92),
      Grade("Charlie", "English", 96)
    )

    val highestGrade = grades_8.reduce((g1, g2) => if (g1.score > g2.score) g1 else g2)

    println(s"8 >> The highest grade is ${highestGrade.score}, achieved by ${highestGrade.student} in ${highestGrade.subject}.")
    // 출력: The highest grade is 96, achieved by Charlie in English.

    /*
      9. groupBy
        - 컬렉션의 요소를 특정 키에 따라 그룹화
     */
    val groupedGrades_9 = grades_8.groupBy(_.subject)

    println("9 >> ")
    groupedGrades_9.foreach { case (subject, grades) =>
      println(s"$subject: ${grades.map(_.score).mkString(", ")}")
    }

    /*
      출력:
      English: 88, 92, 96
      Math: 85, 90, 95
     */

  }
}