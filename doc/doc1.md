# 区分维度表和事实表
>1.根据字段op的状态
2.根据表名
用maxwell把数据存到kafka中后  可以根据 表结构中的
sink_table字段把数据进行分为维度表和事实表
维度表存入到 hbase中,维表关联
事实表存入到kafka中;
# flume断点续传
>基于offset消费指针进行
每次断掉了记录offset
下次继续消费根据offset;
