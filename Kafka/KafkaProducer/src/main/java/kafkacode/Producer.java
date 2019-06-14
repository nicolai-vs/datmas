package kafkacode;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.nio.Buffer;
import java.util.Properties;

public class Producer {
    private static final Logger logger = LogManager.getLogger(Producer.class);

    public static void main(String[] args) {
        String topicName =  "udp-input";
        String host = "localhost";
        int port = 9999;
        BufferedReader in;

        logger.trace("Creating Kafka Producer...");
        KafkaProducer<Integer, String> producer = generateProducer();

        try {
            logger.trace("Starting connection to Generator...");
            Socket s = new Socket(host, port);
            in = new BufferedReader(
                    new InputStreamReader(s.getInputStream())
            );

            logger.trace("Listening for Events...");
            while (true) {
                String jsonString = in.readLine();
                if (jsonString == null) {
                    throw new SocketException();
                }
                producer.send(new ProducerRecord<>(topicName, jsonString));
            }
        } catch (KafkaException e) {
            logger.error("Exception occurred – Check log for more details.\n" + e.getMessage());
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            logger.info("Finished Producer – Closing Kafka Producer.");
            producer.close();
        }
    }
    private static KafkaProducer<Integer, String> generateProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

}
