# spark 3 
## spark 3 环境配置
### 1. 备份原 /usr/local/service/spark
- ```shell script
  mv /usr/local/service/spark /usr/local/service/spark_backup
  ```
### 2. 下载官方3.x 安装包, 解压到 /usr/local/service/ 目录下, 然后建立软链接
- ```shell script
  tar -zxvf spark-3.0.0-bin-without-hadoop.tgz -C /usr/local/service/
  chown hadoop:hadoop -R /usr/local/service/spark-3.0.0-bin-without-hadoop
  cd /usr/local/service/
  ln -s spark-3.0.0-bin-without-hadoop spark
  ```
### 3. 修改spark conf 和 env
#### 3.1 spark-defaults.conf
- **copy 原 spark-defaults.conf 或** 编辑新的 spark-defaults.conf
#### 3.2 spark-env.sh
- cd /usr/local/service/spark/conf
- cp spark-env.sh.template spark-env.sh
- 编辑 spark-env.sh, 添加如下配置
  - ```text
    
    export SPARK_HOME=/usr/local/service/spark
    export SPARK_CONF_DIR=/usr/local/service/spark/conf
    
    export HADOOP_CONF_DIR=/usr/local/service/hadoop/etc/hadoop
    export SPARK_DAEMON_MEMORY=1g
    
    export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=10000 -Dspark.history.retainedApplications=10 -Dspark.history.fs.logDirectory=hdfs://HDFS43373/spark-history -Dfile.encoding=UT
    F-8"
    export SPARK_LOG_DIR=/data/emr/spark/logs
    export SPARK_PID_DIR=/data/emr/spark/logs
    
    export SPARK_DIST_CLASSPATH=$(hadoop classpath)
    ```
## spark 3 maven 依赖注意事项
- spark 3 中, **spark-core 依赖 hadoop-client 2.7.4**, EMR2.5 使用的 hadoop 2.8.5 **如果使用fat jar, 需要 exclusion hadoop-client**
- spark streaming 项目可以参考 https://github.com/winfred958/spark-practice.git practice-streaming 模块