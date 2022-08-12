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

        // 查询员工12个月的工资总和，并起别名为ANNUAL SALARY
        // empDf.select(col("employee_id"), col("last_name"), col("salary") * 12 as "ANNUAL SALARY").show(10)
        // empDf.select($"employee_id", $"last_name", $"salary", $"salary" * 12 as "ANNUAL SALARY").show(10, false)

        // 打印不同的执行计划或生成代码
        // empDf.explain("simple")
        // empDf.explain("extended")
        // empDf.explain("formatted")
        // empDf.explain("cost")
        // empDf.explain("codegen")

        // 计算一个df在内存中的大小
        // val cn = empDf.cache().count()
        // println(cn)
        // val plan = empDf.queryExecution.logical
        // val estimated: BigInt = spark.sessionState.executePlan(plan).optimizedPlan.stats.sizeInBytes
        // println(estimated)

        //利用noop精确计算DataFrame运行时间
        // val df = empDf.select($"employee_id", $"last_name", $"salary", $"salary" * 12 as "sa")
        // val start = System.currentTimeMillis()
        // df.write.mode("Overwrite").format("noop").save()
        // val end = System.currentTimeMillis()
        // println(end - start)

        // 查询employees表中去除重复的job_id以后的数据
        /*empDf.dropDuplicates("job_id").show(10, false)*/

        // 查询工资大于12000的员工姓名和工资
        // empDf.where($"salary" > 12000).na.fill("0").show(10, false) //na.fill(填充值必须和为null值得列是同类型,比如null值列类型为String,就填充 "0")
        // empDf.filter($"salary" > 12000).show(10, false)


        // 查询员工号为176的员工的姓名和部门号
        // empDf.select($"last_name", $"department_id").where($"employee_id" === 176).show(10, false)
        // empDf.select($"last_name", $"department_id").where($"employee_id".equalTo(176)).show(10, false)
        // empDf.select($"last_name", $"department_id").filter($"employee_id" === 176).show(10, false)


        //获取 employee_id并且 employee_id==176时 cnt=0,employee_id==176时 cnt=2,其他cnt=3
        // empDf.select($"employee_id", when($"employee_id" === 176, 0).when($"employee_id" === 181, 2).otherwise(3) as "cnt").show(1000)
        // column的常用api使用
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179)).show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179)).where($"department_id".isNull).show
        // (10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179)).where($"department_id".isNotNull) .show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179) && $"department_id".isNull).show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".between(176, 179) && $"department_id".isNotNull).show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".isin(176, 177, 178, 179) && $"department_id".isNotNull) .show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".isInCollection(List(176, 177, 178, 179)) && $"department_id".isNotNull ).show(10)
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".like("10%")).show(10)

        //不区分大小写的like
        // empDf.select($"employee_id", $"last_name", $"department_id").where($"employee_id".ilike("10%")).show(10)

        // val df = spark.sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
        // df.select($"struct_col".withField("c", lit(3))).show(10)
        // empDf.describe("salary").show()

        // 查询员工的姓名和部门号和年薪，按年薪降序 按姓名升序显示
        // empDf.select($"last_name", $"department_id", $"salary" * 12 as "annual_sal").orderBy(desc("annual_sal"), asc("last_name")).show(10)

        // 选择工资不在 8000 到 17000 的员工的姓名和工资，按工资降序，显示第 21到40位置的数据
        // empDf.select($"last_name", $"salary").where(!$"salary".between(8000, 17000)).show(10)

        // 查询邮箱中包含 e 的员工信息，并先按邮箱的字节数降序，再按部门号升序
        // empDf.select($"last_name", $"email", $"department_id", length($"email") as "e_len").where($"email".contains("E")).orderBy(desc("e_len"), asc("department_id")).show(10)
        // empDf.select($"last_name", $"email", $"department_id").where($"email".contains("E")).orderBy(length($"email").desc, $"department_id".asc).show(10)
        // empDf.select($"last_name", $"email", $"department_id").where($"email".contains("E")).orderBy(length($"email").desc, asc("department_id")).show(10)

        // 显示所有员工的姓名，部门号和部门名称
        //empDf.join(deptDf, Seq("department_id"), "left").select($"last_name", $"department_id", $"department_name").show(10)

        // 查询90号部门员工的job_id和90号部门的location_id
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
