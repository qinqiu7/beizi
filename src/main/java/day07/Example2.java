package day07;

import day04.Example7;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Properties;

//写入kafka
public class Example2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties=new Properties();
        properties.put("bootstrap.servers","192.168.174.100:9092");
        properties.setProperty("group.id","first");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        env.addSource(new FlinkKafkaConsumer<String>(
                "user-behavior1",
                new SimpleStringSchema(),properties
        )).map(new MapFunction<String, Example7.UserBehavior>() {

            @Override
            public Example7.UserBehavior map(String s) throws Exception {
                String[] arr=s.split(",");
                return new Example7.UserBehavior(
                        arr[0],arr[1],arr[2],arr[3],
                        Long.parseLong(arr[4]) * 1000L
                );
            }
        })
                .filter(r->r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Example7.UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Example7.UserBehavior>() {
                            @Override
                            public long extractTimestamp(Example7.UserBehavior userBehavior, long recordTimestamp) {
                                return userBehavior.timestamp;
                            }
                        })
                )
                .keyBy(r->true)
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .process(new ProcessWindowFunction<Example7.UserBehavior, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean,Context context, Iterable<Example7.UserBehavior> element, Collector<String> out) throws Exception {
                        HashMap<String, Long>hashMap=new HashMap<>();
                        for (Example7.UserBehavior e:element){
                            if (hashMap.containsKey(e.itemId)){
                                hashMap.put(e.itemId,hashMap.get(e.itemId)+1L);
                            }
                            else {
                                hashMap.put(e.itemId,1L);
                            }
                        }
                        ArrayList<Tuple2<String,Long>>arrayList=new ArrayList<>();
                        for (String key:hashMap.keySet()){
                            arrayList.add(Tuple2.of(key,hashMap.get(key)));
                        }
                        arrayList.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> t2, Tuple2<String, Long> t1) {
                                return t1.f1.intValue()-t2.f1.intValue();
                            }
                        });
                        StringBuilder result = new StringBuilder();
                        result
                                .append("========================================\n")
                                .append("窗口：" + new Timestamp(context.window().getStart()) + "~" + new Timestamp(context.window().getEnd()))
                                .append("\n");
                        for (int i = 0; i < 3; i++) {
                            Tuple2<String, Long> currElement = arrayList.get(i);
                            result
                                    .append("第" + (i+1) + "名的商品ID是：" + currElement.f0 + "; 浏览次数是：" + currElement.f1)
                                    .append("\n");
                        }
                        out.collect(result.toString());
                    }
                }).print();
        env.execute();
    }
}
