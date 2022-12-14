package cn.mkf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author mkf
  * @version
  * @note
  */
object SparkSqlDemo {


    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .master("local")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val cotrDf = spark.read.option("header", true).csv("D:\\workspace\\SparkSqlApi\\src\\main\\resources\\countries.csv")
        // cotrDf.printSchema()
        // cotrDf.show(10, false)

        val deptDf = spark.read.option("header", true).csv("D:\\workspace\\SparkSqlApi\\src\\main\\resources\\departments.csv")
        // deptDf.printSchema()
        // deptDf.show(10, false)
        val empDf = spark.read.option("header", true).csv("D:\\workspace\\SparkSqlApi\\src\\main\\resources\\employees.csv")
        // empDf.printSchema()
        // empDf.show(10, false)
        val jobGraDf = spark.read.option("header", true).csv("D:\\workspace\\SparkSqlApi\\src\\main\\resources\\job_grades.csv")
        // jobGraDf.printSchema()
        // jobGraDf.show(10, false)
        val jobHisDf = spark.read.option("header", true).csv("D:\\workspace\\SparkSqlApi\\src\\main\\resources\\job_history.csv")
        // jobHisDf.printSchema()
        // jobHisDf.show(10, false)

        val jobsDf = spark.read.option("header", true).csv("D:\\workspace\\SparkSqlApi\\src\\main\\resources\\jobs.csv")
        // jobsDf.printSchema()
        // jobsDf.show(10, false)
        val locDf = spark.read.option("header", true).csv("D:\\workspace\\SparkSqlApi\\src\\main\\resources\\locations.csv")
        // locDf.printSchema()
        // locDf.show(10, false)

        val orderDf = spark.read.option("header", true).csv("D:\\workspace\\SparkSqlApi\\src\\main\\resources\\order.csv")
        // orderDf.printSchema()
        // orderDf.show(10, false)
        val regionsDf = spark.read.option("header", true).csv("D:\\workspace\\SparkSqlApi\\src\\main\\resources\\regions.csv")
        // regionsDf.printSchema()
        // regionsDf.show(10, false)

        println("====================================================================================================")
        import spark.implicits._

        // ????????????12???????????????????????????????????????ANNUAL SALARY
        // empDf.select(col("employee_id"), col("last_name"), col("salary") * 12 as "ANNUAL SALARY").show(10)
        // empDf.select($"employee_id", $"last_name", $"salary", $"salary" * 12 as "ANNUAL SALARY").show(10, false)

        // ??????????????????????????????????????????
        // empDf.explain("simple")
        // empDf.explain("extended")
        // empDf.explain("formatted")
        // empDf.explain("cost")
        // empDf.explain("codegen")

        // ????????????df?????????????????????
        // val cn = empDf.cache().count()
        // println(cn)
        // val plan = empDf.queryExecution.logical
        // val estimated: BigInt = spark.sessionState.executePlan(plan).optimizedPlan.stats.sizeInBytes
        // println(estimated)

        //??????noop????????????DataFrame????????????
        // val df = empDf.select($"employee_id", $"last_name", $"salary", $"salary" * 12 as "sa")
        // val start = System.currentTimeMillis()
        // df.write.mode("Overwrite").format("noop").save()
        // val end = System.currentTimeMillis()
        // println(end - start)

        // ??????employees?????????????????????job_id???????????????
        /*empDf.dropDuplicates("job_id").show(10, false)*/

        // ??????????????????12000????????????????????????
        // empDf.where($"salary" > 12000).na.fill("0").show(10, false) //na.fill(?????????????????????null?????????????????????,??????null???????????????String,????????? "0")
        // empDf.filter($"salary" > 12000).show(10, false)


        // ??????????????????176??????????????????????????????
        // empDf.select($"last_name", $"department_id").where($"employee_id" === 176).show(10, false)
        // empDf.select($"last_name", $"department_id").where($"employee_id".equalTo(176)).show(10, false)
        // empDf.select($"last_name", $"department_id").filter($"employee_id" === 176).show(10, false)


        //?????? employee_id?????? employee_id==176??? cnt=0,employee_id==176??? cnt=2,??????cnt=3
        // empDf.select($"employee_id", when($"employee_id" === 176, 0).when($"employee_id" === 181, 2).otherwise(3) as "cnt").show(1000)
        // column?????????api??????
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179)).show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179)).where($"department_id".isNull).show
        // (10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179)).where($"department_id".isNotNull) .show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179) && $"department_id".isNull).show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179) && $"department_id".isNotNull).show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".isin(176, 177, 178, 179) && $"department_id".isNotNull) .show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".isInCollection(List(176, 177, 178, 179)) && $"department_id".isNotNull ).show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".like("10%")).show(10)

        //?????????????????????like
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".ilike("10%")).show(10)

        // val df = spark.sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
        // df.select($"struct_col".withField("c", lit(3))).show(10)
        // empDf.describe("salary").show()

        // ???????????????????????????????????????????????????????????? ?????????????????????
        // empDf.select($"last_name", $"department_id", $"salary" * 12 as "annual_sal").orderBy(desc("annual_sal"), asc("last_name")).show(10)

        // ?????????????????? 8000 ??? 17000 ????????????????????????????????????????????????????????? 21???40???????????????
        // empDf.select($"last_name", $"salary").where(!$"salary".between(8000, 17000)).show(10)

        // ????????????????????? e ???????????????????????????????????????????????????????????????????????????
        // empDf.select($"last_name", $"email", $"department_id", length($"email") as "e_len").where($"email".contains("E")).orderBy(desc("e_len"), asc("department_id")).show(10)
        // empDf.select($"last_name", $"email", $"department_id").where($"email".contains("E")).orderBy(length($"email").desc, $"department_id".asc).show(10)
        // empDf.select($"last_name", $"email", $"department_id").where($"email".contains("E")).orderBy(length($"email").desc, asc("department_id")).show(10)

        // ??????????????????????????????????????????????????????
        //empDf.join(deptDf, Seq("department_id"), "left").select($"last_name", $"department_id", $"department_name").show(10)

        // ??????90??????????????????job_id???90????????????location_id
        empDf.join(deptDf, Seq("department_id"), "left").where($"department_id" === 90).select($"job_id", $"location_id").show(10)

        empDf.createTempView("employees")
        deptDf.createTempView("departments")

        val str_sql =
            """SELECT job_id, location_id
              |FROM employees e, departments d
              |WHERE e.department_id = d.department_id
              |AND e.department_id = 90""".stripMargin


        spark.sql(str_sql).explain("extended") //
        spark.sql(str_sql).explain("codegen") //
    }

}
