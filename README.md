# druid

[![Build Status](https://travis-ci.org/alibaba/druid.svg?branch=master)](https://travis-ci.org/alibaba/druid)
[![Coverage Status](https://img.shields.io/codecov/c/github/alibaba/druid/master.svg)](https://codecov.io/github/alibaba/druid?branch=master&view=all#sort=coverage&dir=asc)  
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.alibaba/druid/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.alibaba/druid/)
[![GitHub release](https://img.shields.io/github/release/alibaba/druid.svg)](https://github.com/alibaba/druid/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

Introduction
---

- git clone https://github.com/alibaba/druid.git
- cd druid && mvn install
- have fun.

Documentation
---

- 中文 https://github.com/alibaba/druid/wiki/%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98
- English https://github.com/alibaba/druid/wiki/FAQ
- Druid Spring Boot Starter https://github.com/alibaba/druid/tree/master/druid-spring-boot-starter


Issues

 - 支持impala jdbc
 
 ### 正常的参数条件
 
 ```
  val IMPALAD_HOST=
  val IMPALAD_JDBC_PORT = "21050"
  val CONNECTION_URL = "jdbc:impala://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";
 ```

### 通过druid 调用impala，实现kudu select查询用于分析

、、、
<!--impala jar-->
            <dependency>
                <groupId>com.cloudera.impala.jdbc</groupId>
                <artifactId>hive_metastore</artifactId>
                <version>${impala.jdbc.version}</version>
            </dependency>
            <dependency>
                <groupId>com.cloudera.impala.jdbc</groupId>
                <artifactId>hive_service</artifactId>
                <version>${impala.jdbc.version}</version>
            </dependency>
            <dependency>
                <groupId>com.cloudera.impala.jdbc</groupId>
                <artifactId>ImpalaJDBC41</artifactId>
                <version>${impala.jdbc.version}</version>
            </dependency>
            <dependency>
                <groupId>com.cloudera.impala.jdbc</groupId>
                <artifactId>ql</artifactId>
                <version>${impala.jdbc.version}</version>
            </dependency>
            <dependency>
                <groupId>com.cloudera.impala.jdbc</groupId>
                <artifactId>TCLIServiceClient</artifactId>
                <version>${impala.jdbc.version}</version>
            </dependency>
            、、、
