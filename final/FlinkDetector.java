import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

public class TemperatureAnomalyDetector {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<ObjectNode> kafkaConsumer = createConsumerForTopic("Temperatura");

        // Odczyt danych i zamiana na TemperatureInfo
        DataStream<TemperatureInfo> temperatures = env
                .addSource(kafkaConsumer)
                .map((MapFunction<ObjectNode, TemperatureInfo>) jsonNode -> new TemperatureInfo(
                        jsonNode.findValue("thermometer_id").asText(),
                        jsonNode.findValue("timestamp").asText(),
                        jsonNode.findValue("temperature").asDouble()
                ))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TemperatureInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.extractEpochMillis())
                );

        // Okna czasowe i filtr anomalii
        SingleOutputStreamOperator<String> alerts = temperatures
                .filter(temp -> temp.temperature < 0)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10))) // np. co 10 sekund
                .apply((window, input, out) -> {
                    for (TemperatureInfo temp : input) {
                        out.collect("ALARM! Temp < 0: " + temp.temperature + " | Sensor: " +
                                temp.thermometerId + " | Time: " + temp.timestamp);
                    }
                });

        alerts.addSink(createProducerForTopic("Alarm"));
        alerts.print();

        env.execute("Temperature Anomaly Detector with Time Windows");
    }

    private static FlinkKafkaConsumer<ObjectNode> createConsumerForTopic(String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        return new FlinkKafkaConsumer<>(topic, new JSONKeyValueDeserializationSchema(true), props);
    }

    private static FlinkKafkaProducer<String> createProducerForTopic(String topic) {
        return new FlinkKafkaProducer<>("localhost:9092", topic, new SimpleStringSchema());
    }

    public static class TemperatureInfo {
        public String thermometerId;
        public String timestamp;
        public Double temperature;

        public TemperatureInfo(String thermometerId, String timestamp, Double temperature) {
            this.thermometerId = thermometerId;
            this.timestamp = timestamp;
            this.temperature = temperature;
        }

        public long extractEpochMillis() {
            // Zakładamy format ISO8601 — np. "2024-05-01T12:34:56Z"
            return java.time.Instant.parse(timestamp).toEpochMilli();
        }

        @Override
        public String toString() {
            return String.format("thermometerId: %s, timestamp: %s, temperature: %.2f",
                    thermometerId, timestamp, temperature);
        }
    }
}
