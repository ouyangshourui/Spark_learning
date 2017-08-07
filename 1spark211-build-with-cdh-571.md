$ dev/make-distribution.sh -DskipTests \

-Dhadoop.version=2.6.0-cdh5.7.1 \

-Phadoop-2.6 \

-Pyarn \

-Phive \

-Phive-thriftserver

我自己修改的内容：

$ dev/make-distribution.sh -DskipTests \

-Dhadoop.version=2.6.0-cdh5.7.1 \

-Phadoop-2.6 \

-Pyarn \

-Phive \

-Phive-thriftserver

pom.xml 文件中添加

```
<repositories>
   <repository>
     <id>cloudera</id>      
        <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
   </repository>
</repositories>
```
在CDH的spark中，要想集成hive-thriftserver进行编译，需要修改 pom.xml 文件，添加一行 sql/hive-thriftserver：


注意：
```
[root@lpsllfdrcw1 spark]# bin/spark-sql --master local       
java.lang.ClassNotFoundException: org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver
        at java.net.URLClassLoader.findClass(URLClassLoader.java:381)
        at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
        at java.lang.ClassLoader.loadClass(ClassLoader.java:357)
        at java.lang.Class.forName0(Native Method)
        at java.lang.Class.forName(Class.java:348)
        at org.apache.spark.util.Utils$.classForName(Utils.scala:175)
        at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:689)
        at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:181)
        at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:206)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:121)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Failed to load main class org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.
You need to build Spark with -Phive and -Phive-thriftserver.

```

注意设置：
[root@lpsllfdrcw1 spark]# export SPARK_HOME=/opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/lib/spark
[root@lpsllfdrcw1 spark]# export SPARK_HOME=/root/spark/spark



