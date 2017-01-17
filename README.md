
#### 使用说明：
1. 开发以local模式调试运行，需要添加环境变量：mode=local，目的用于读取src/main/resources/spark.properties配置文件 
2. 部署打包执行： ./package.sh
3. yarn-cluster模式运行任务：
    > ./bin/spark-submit --verbose --deploy-mode cluster --master yarn-cluster ${app_home}/target/spark-odps-driver-1.0.0-SNAPSHOT-shaded.jar  sql语句base64编码

### set 语句
1. 根据计算数据量设置 set spark.sql.shuffle.partitions=x 参数优化sql性能；
2. 缓存表: set spark.cache.table[.projectcode].tablename = true/false
3. 保存create table 表，根据DataFrame schema创建 table，再把df数据保存表中：set spark.insert.table[.projectcode].tablename = true;
4. 触发定制算法：set spark.algorithm.table[.projectcode].tablename=算法名称，例如：set spark.algorithm.table.tdl_spark_event_tb_order_create_tmp=stat_KDE;
5. 算法相关参数设置：set spark.algorithm.config[.projectcode].tablename='key=value;key=value'; 例如：set spark.algorithm.config.tdl_spark_event_tb_order_create_tmp='estimate=-1.0, 2.0, 5.0';

#### 支持odps语句：
1. 支持odps ddl语句。
2. 支持create table ... as select ...语句，创建spark rdd。
3. 支持insert overwrite|into语句，支持动态分区，执行结果数据写入odps或hbase表。
4. 支持分区表和非分区表。
6. 支持spark sql 和 hive sql语法和函数

#### odps语句限制条件：
1. create table和 insert sql的select 部分不能支持*，需要明确指定输出列的名称，例如：select * from user 不支持。
2. insert sql的select输出列名称必须与写入表字段名称一致，可以通过别名保持一致，例如：select substr(ds, 1, 8) as ds from user。
3. 对于分区表，where部分需要指定分区范围，明确在某一时间段范围内，不支持like匹配，例如：where ds<='20161011' 需要加上 ds>='20161010'。
4. 不支持select语句，执行没有意义。

#### 定制算法：

例如定制核概率估计算法：

```java
class KernelDensityAlg extends Algorithm {

    /**
      * 执行算法
      *
      * @param context
      * @param conf
      * @param df
      * @param sparkTableName
      */
    override def executeAgl(context: SparkDriverContext, conf: PropertiesConfiguration,
                         df: DataFrame, sparkTableName: String): Array[Row] = {

        val intervalNum = conf.getInt("intervalNum", 100)
        val resultRows = ArrayBuffer[Row]()
        val labelColName = conf.getString("labelColName", "lable")

        var hasLabelColumn = false
        df.schema.fields.foreach(field => {
            if(StringUtils.equals(field.name, labelColName)) {
                hasLabelColumn = true
            }
        })

        val sdRows = calcStandardDeviation(context, conf, sparkTableName, hasLabelColumn)
        sdRows.indices.foreach(index => {
            //按照label分组处理数据
            val row = sdRows(index)
            val labelColumnValue = row.getAs[String]("lable")
            val labelRdd: RDD[Array[Double]] = df.rdd.map(row => {
                var arrBuf = ArrayBuffer[Double]()
                val labelValue = if (hasLabelColumn) row.getAs[String]("lable") else null

                for (field <- row.schema.fields if field.name != "lable") {
                    val value = row.getAs[Double](field.name)
                    if (labelColumnValue == labelValue && value != null) {
                        arrBuf += row.getAs[Double](field.name)
                    }
                }

                arrBuf.toArray
            })

            val stat = Statistics.colStats(labelRdd.map((values: Array[Double]) => Vectors.dense(values)))

            for (field <- row.schema.fields if field.name != "lable") {
                val colName = field.name
                val bandwidth = row.getAs[Double](colName)
                val minValue = stat.min(index)
                val maxValue = stat.max(index)
                val interval = (maxValue - minValue) / intervalNum

                if (interval != 0) {
                    val evaluationPoints = new Array[scala.Double](intervalNum)
                    (0 until intervalNum).foreach(no => {
                        val value = minValue + (no + 1) * interval

                        if (value > maxValue) {
                            evaluationPoints(no) = maxValue
                        } else {
                            evaluationPoints(no) = value
                        }
                    })

                    val dataRdd = labelRdd.map(_ (index))
                    val densities = new KernelDensity().setSample(dataRdd).setBandwidth(bandwidth)
                        .estimate(evaluationPoints)

                    densities.indices.foreach(i => {
                        resultRows += Row(colName, labelColumnValue, evaluationPoints(i), densities(i))
                    })
                }
            }
        })

        resultRows.toArray
    }

    /**
      * 返回结果表列信息，(列名称-> odps 数据类型)
      *
      * @return
      */
    override def resultTableColumns():Map[String, String] = {
        Map("colname" -> "string", "label" -> "string", "x" -> "double", "pdf" -> "double")
    }

    /**
      * odpsOps.saveToTable 需要的 transfer
      *
      * @param row
      * @param record
      * @param schema
      */
    override def saveTableTransfer(row: Row, record: Record, schema: TableSchema): Unit = {
        record.set("colname", row.get(0))
        record.set("label", row.get(1))
        record.set("x", row.get(2))
        record.set("pdf", row.get(3))
    }

    /**
      * 计算标准方差
      *
      * +-----+-------------------+-------------------+
        |lable|              selid|             selid1|
        +-----+-------------------+-------------------+
        | null|9.176106951050788E8|9.176106951050788E8|
        +-----+-------------------+-------------------+
      *
      * @param context
      * @param conf
      * @param sparkTableName
      */
    private def calcStandardDeviation(context: SparkDriverContext, conf: PropertiesConfiguration,
                                      sparkTableName: String, hasLabelColumn: Boolean): Array[Row] = {

        var featureColNames = conf.getStringArray("featureColNames")
        val labelColName = conf.getString("labelColName", "lable")

        featureColNames = featureColNames.map(colName => s"stddev(${colName}) as ${colName}")
        var sql:String = null
        if(hasLabelColumn) {
            sql = s"SELECT ${labelColName} as lable, ${featureColNames.mkString(",")} FROM $sparkTableName GROUP BY $labelColName"
        } else {
            sql = s"SELECT null as lable, ${featureColNames.mkString(",")} FROM $sparkTableName"
        }

        context.sqlContext.sql(sql).collect()
    }
}
```

运行核概率估计算法：
```sql
-- 由于会触发多次action，指定缓存spark临时表，加快计算速度
set spark.cache.table.tdl_spark_event_tb_order_create_tmp_1 = true;
create table tdl_spark_event_tb_order_create_tmp_1 LIFECYCLE 11 as
select selid from tdl_spark_event_tb_order_create
where ds = '201610101015';

-- 基于tdl_spark_event_tb_order_create_tmp表数据，计算核概率密度估计
set spark.algorithm.table.tdl_spark_event_tb_order_create_tmp=stat_KDE;
-- featureColNames：计算特征变量，intervalNum：点分布数量，默认100，outputTableName：指定数据表, lifecycle: 指定数据表生命周期
set spark.algorithm.config.tdl_spark_event_tb_order_create_tmp='featureColNames=variable;intervalNum=100;labelColName=label;outputTableName=tdl_spark_KDE_result;lifecycle=2';

create table tdl_spark_event_tb_order_create_tmp LIFECYCLE 11 as
select cast(selid as double) as selid from tdl_spark_event_tb_order_create_tmp_1
```

#### 计划功能：
1. 调用readTable方法时，transfer 能够获取分区字段值（需求比较紧急）；
2. kryo 序列化支持（减少缓存内存使用，提高序列化速度）；
