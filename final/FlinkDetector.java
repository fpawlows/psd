import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Random;
import java.util.Properties;
public class Temperature {

 public static void main(String[] args) throws Exception {
 final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 FlinkKafkaConsumer<ObjectNode> kafkaConsumer = createConsumerForTopic("Temperatura");
 DataStream<TemperatureInfo> temperaturesStream = env.addSource(kafkaConsumer).map(jsonNode -> {
 return new TemperatureInfo(
 jsonNode.findValue("thermometer_id").asText(),
 jsonNode.findValue("timestamp").asText(),
 jsonNode.findValue("temperature").asDouble()
 );
 });
 DataStream<TemperatureInfo> negativeTemperatures = temperaturesStream.filter(new FilterFunction<TemperatureInfo>() {
 @Override
 public boolean filter(TemperatureInfo temp) throws Exception {
 return temp.temperature < 0;
 }
 });
 FlinkKafkaProducer<String> kafkaProducer = createProducerForTopic("Alarm");
 negativeTemperatures.map(temperature -> {
 return "Alarm! Temperature: " + temperature.temperature + ", timestamp: " + temperature.timestamp;
 }).addSink(kafkaProducer);
 negativeTemperatures.print();
 env.execute();
 }
 public static FlinkKafkaConsumer<ObjectNode> createConsumerForTopic(String topic) {
 Properties props = new Properties();
 props.setProperty("bootstrap.servers", "localhost:9092");
 FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<>(topic, new
JSONKeyValueDeserializationSchema(true), props);
 return consumer;
 }
 public static FlinkKafkaProducer<String> createProducerForTopic(String topic) {
 return new FlinkKafkaProducer<String>("localhost:9092", topic, new SimpleStringSchema());
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
 public String toString() {
 return "thermometerId: " + thermometerId + ", timestamp: " + timestamp + ", temperature: " + temperature;
 }
 }
}