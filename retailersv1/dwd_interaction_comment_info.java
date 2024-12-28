package com.retailersv1;

import common.utils.CommonUtils;
import common.utils.ConfigUtils;
import common.utils.EnvironmentSettingUtils;
import common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.xml.crypto.Data;
import java.util.Date;

public class dwd_interaction_comment_info {

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");

    private static final String kafka_topic_db = ConfigUtils.getString("kafka.topic.db");

    private static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = ConfigUtils.getString("dwd.interaction.comment.info");

    @SneakyThrows
    public static void main(String[] args) {
        CommonUtils.printCheckPropEnv(
                false,
                kafka_botstrap_servers,
                kafka_topic_db
        );

        //读取kafka数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

//        DataStreamSource<String> kafkaSource = env.fromSource(KafkaUtils.buildKafkaSource(
//                        kafka_botstrap_servers,
//                        kafka_topic_db,
//                        new Date().toString(),
//                        OffsetsInitializer.earliest()),
//                WatermarkStrategy.noWatermarks(), "kafkaSource"
//        );
//        kafkaSource.print(">>>>");

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        streamTableEnvironment.executeSql("CREATE TABLE topic_db (\n" +
                "  op string," +
                "db string," +
                "before map<String,String>," +
                "after map<String,String>," +
                "source map<String,String>," +
                "ts_ms bigint," +
                "row_time as TO_TIMESTAMP_LTZ(ts_ms,3)," +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + kafka_topic_db + "',\n" +
                "  'properties.bootstrap.servers' = '" + kafka_botstrap_servers + "',\n" +
                "  'properties.group.id' = '1',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        Table table = streamTableEnvironment.sqlQuery("select " +
                "`after` ['id'] as id ,\n" +
                "`after` ['user_id'] as user_id ,\n" +
                "`after` ['sku_id'] as sku_id ,\n" +
                "`after` ['cart_price'] as cart_price ,\n" +
                "if(op='c',cast(after['sku_num'] as bigint),cast(after['sku_num'] as bigint)-cast(before['sku_num'] as bigint)) sku_num ,\n" +
                "`after` ['img_url'] as img_url ,\n" +
                "`after` ['sku_name'] as sku_name,\n" +
                "`after` ['is_checked'] as is_checked ,\n" +
                "`after` ['create_time'] as create_time ,\n" +
                "`after` ['operate_time'] as operate_time ,\n" +
                "`after` ['is_ordered'] as is_ordered ,\n" +
                "`after` ['order_time'] as order_time ," +
                "ts_ms as ts_ms " +
                "from topic_db " +
                "where source['table']='cart_info' and source['db']='gmall'  " +
                "and (op='c' or (op='u' and before['sku_num'] is not null " +
                "and cast (after['sku_num'] as bigint) > cast(before['sku_num'] as bigint)))");
//        table.execute().print();

        DataStream<Row> rowDataStream = streamTableEnvironment.toDataStream(table);

        SingleOutputStreamOperator<String> map = rowDataStream.map(String::valueOf);
        map.sinkTo(
                KafkaUtils.buildKafkaSink(kafka_botstrap_servers,TOPIC_DWD_INTERACTION_COMMENT_INFO)
        );


        env.execute();
    }



}
