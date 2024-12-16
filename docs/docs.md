# flink cdc 底层是如何实现的
Flink CDC（Change Data Capture）的底层实现原理主要依赖于具体的CDC连接器，这些连接器能够连接到数据库的变更源，解析捕获到的变更事件，并将其转换为Flink流处理应用程序的输入
# Flink CDC的优势
* Flink的算子和SQL模块更为成熟和易用。
* Flink作业可以通过调整算子并行度的方式轻松扩展处理能力。
* Flink支持高级的状态后端（State Backends），允许存取海量的状态数据。
* Flink Table/SQL模块将数据库表和变动记录流（例如CDC的数据流）看做是同一事物的两面，因此内部提供的Upsert消息结构可以与Debezium等生成的变动记录一一对应。
  #habse
  *HBase 是 Hadoop 的子项目，利用 HDFS 作为其文件存储系统，并使用 Zookeeper 进行协调
