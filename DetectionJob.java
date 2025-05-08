package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FreezeDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Sensor> temperatures = env.fromElements(
            new Sensor(1255465, -1),
            new Sensor(1277465, 35),
            new Sensor(1277465, 35),
            new Sensor(1277465, 35),
            new Sensor(1277465, -123214),
            new Sensor(1277465, 0),
            new Sensor(1277465, 0),
            new Sensor(1277465, 5),
            new Sensor(1277465, 35),
            new Sensor(1248865, -12)
        ).name("temperatures");

        DataStream<Alert> alerts = temperatures
            .keyBy(Sensor::getId)
            .process(new FreezeDetector())
            .name("freeze-detector");

        alerts
            .addSink(new AlertSink())
            .name("send-alerts");

        alerts.print();

        env.execute("Freeze Detection");
    }

    public static class Sensor {
        public Integer id;
        public Integer temperature;

        public Sensor() {}

        public Sensor(Integer id, Integer temperature) {
            this.id = id;
            this.temperature = temperature;
        }

        public String toString() {
            return this.id.toString() + ": temperature " + this.temperature.toString();
        }

        public Integer getTemperature() {
            return this.temperature;
        }

        public Integer getId() {
            return this.id;
        }
    }
}
