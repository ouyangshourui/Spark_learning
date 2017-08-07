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



