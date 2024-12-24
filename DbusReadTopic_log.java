package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import common.utils.ConfigUtils;
import common.utils.DateTimeUtils;
import common.utils.EnvironmentSettingUtils;
import common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;

public class DbusReadTopic_log {
    private static final OutputTag<JSONObject> Err_JSON_MSG = new OutputTag<JSONObject>("err"){};
    private static final OutputTag<JSONObject> Start_JSON_MSG = new OutputTag<JSONObject>("start"){};
    private static final OutputTag<JSONObject> Display_JSON_MSG = new OutputTag<JSONObject>("displays"){};
    private static final OutputTag<JSONObject> Action_JSON_MSG = new OutputTag<JSONObject>("actions"){};
    private static final OutputTag<JSONObject> Page_JSON_MSG = new OutputTag<JSONObject>("page"){};
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        DataStreamSource<String> kafkaSource = env.fromSource(KafkaUtils.buildKafkaSource(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC"),
                        new Date().toString(),
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), "kafkaSource");
//        kafkaSource.print("读取kafka中的数据>>>>");
        SingleOutputStreamOperator<JSONObject> map = kafkaSource.map(JSONObject::parseObject).setParallelism(1);
        //数据清洗
        SingleOutputStreamOperator<JSONObject> jsonObjectSingleOutputStreamOperator = map.flatMap(new RichFlatMapFunction<JSONObject, JSONObject>() {
             @Override
             public void flatMap(JSONObject s, Collector<JSONObject> collector) throws Exception {
                 try {
                     String ts = s.getString("ts");
                     JSONObject common = s.getJSONObject("common");
                     String mid = common.getString("mid");
                     if (!ts.isEmpty() && !mid.isEmpty()) {
                         collector.collect(s);
                     }
                 } catch (Exception e) {
                     throw new RuntimeException(e);
                 }
             }
      }
        );
//        jsonObjectSingleOutputStreamOperator.print("数据清洗>>>>");

        //设置水位线,根据mid进行分组
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjectSingleOutputStreamOperator.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
//        jsonObjectStringKeyedStream.print(">>>>>>>>>>>>>>");


        //新老用户
        SingleOutputStreamOperator<JSONObject> newOldStream = jsonObjectStringKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<String>("is_new", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");
                String curDate = DateTimeUtils.format(new Date(ts), "yyyy-MM-dd");
                String is_New = common.getString("is_new");
                String value = state.value();
                if ("1".equals(is_New)) {
                    if (value != null && !curDate.equals(value)) {
                        common.put("is_new", 0);
                    } else {
                        state.update(curDate);
                    }
                }
                return jsonObject;
            }
        });
//        newOldStream.print("新老用户>>>");
        SingleOutputStreamOperator<JSONObject> process = newOldStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                {
                    JSONObject err = jsonObject.getJSONObject("err");
                    if (err != null) {
                        context.output(Err_JSON_MSG, err);
                        jsonObject.remove("err");
                    }
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (start != null) {
                        context.output(Start_JSON_MSG, start);
                        jsonObject.remove("start");
                    } else if (page != null) {
                        JSONArray displays1 = jsonObject.getJSONArray("displays");
                        if (displays1 != null) {
                            for (int i = 0; i < displays1.size(); i++) {
                                JSONObject display = displays1.getJSONObject(i);
                                display.put("page", page);
                                display.put("common", common);
                                display.put("ts", ts);
                                context.output(Display_JSON_MSG, display);
                            }
                            jsonObject.remove("displays");
                        }
                        JSONArray actions = jsonObject.getJSONArray("actions");
                        if (actions != null) {
                            for (int i = 0; i < actions.size(); i++) {
                                JSONObject action = actions.getJSONObject(i);
                                action.put("page", page);
                                action.put("common", common);
                                action.put("ts", ts);
                                context.output(Action_JSON_MSG, action);
                            }
                            jsonObject.remove("actions");
                        }
                        collector.collect(jsonObject);
                    }
                }
            }
        });

        SideOutputDataStream<JSONObject> errSideOut = process.getSideOutput(Err_JSON_MSG);
//        errSideOut.print("错误>>>>>");

        SideOutputDataStream<JSONObject> DisplaySideOut = process.getSideOutput(Display_JSON_MSG);
//        DisplaySideOut.print("曝光(页面)>>>>>");

        SideOutputDataStream<JSONObject> StartSideOut = process.getSideOutput(Start_JSON_MSG);
//        StartSideOut.print("启动>>>>>");

        SideOutputDataStream<JSONObject> ActionSideOut = process.getSideOutput(Action_JSON_MSG);
//        ActionSideOut.print("动作>>>>>");



        //数据存入kafka中
//        errSideOut.addSink(new SinkFunction<JSONObject>() {
//            @Override
//            public void invoke(JSONObject value, Context context) throws Exception {
//                ArrayList<JSONObject> list = new ArrayList<>();
//                list.add(value);
//                KafkaUtils.sinkJson2KafkaMessage("topic_err",list);
//            }
//        });

        DisplaySideOut.addSink(new SinkFunction<JSONObject>() {
            @Override
            public void invoke(JSONObject value, Context context) throws Exception {
                ArrayList<JSONObject> list = new ArrayList<>();
                list.add(value);
                KafkaUtils.sinkJson2KafkaMessage("topic_display",list);
            }
        });










        env.execute();


    }
}
