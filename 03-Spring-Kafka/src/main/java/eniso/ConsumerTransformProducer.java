package eniso;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class ConsumerTransformProducer {

    private static Properties getProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.185:9092");
        props.put("retries", 3); // 重试次数
        props.put("batch.size", 100); // 批量发送大小
        props.put("buffer.memory", 33554432); // 缓存大小，根据本机内存大小配置
        props.put("linger.ms", 3000); // 发送频率，满足任务一个条件发送
        props.put("client.id", "producer-syn-2"); // 发送端 id，便于统计
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("transactional.id", "producer-2"); // 每台机器唯一
        props.put("enable.idempotence", true); // 设置幂等性
        return props;
    }

    private static Properties getConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.185:9092");
        props.put("group.id", "consumer-group-1");
        props.put("session.timeout.ms", 30000); // 如果其超时，将会可能触发 re-balance 并认为已经死去，重新选举 Leader
        props.put("enable.auto.commit", "false"); // 开启自动提交
        props.put("auto.commit.interval.ms", "1000"); // 自动提交时间
        props.put("auto.offset.reset", "earliest"); // 从最早的 offset 开始拉取，从最近的 offset 开始消费
        props.put("client.id", "consumer-syn-1"); // 接收端 id，便于统计
        props.put("max.poll.records", "100"); // 每次批量拉取条数
        props.put("max.poll.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("isolation.level", "read_committed"); // 设置隔离级别
        return props;
    }

    public static void main(String[] args) {
        // 监听 foo.bar.cmd，转发到 foo.bar
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(getProducerProps());
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(getConsumerProps());
        producer.initTransactions();
        consumer.subscribe(Collections.singletonList("foo.bar.cmd"));
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(500));
            try {
                producer.beginTransaction();
                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                for (ConsumerRecord<Integer, String> record : records) {
                    log.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                    // 记录提交的偏移量
                    commits.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                    // 转发消息
                    Future<RecordMetadata> future = producer.send(new ProducerRecord<>("foo.bar", "forward: " + record.value()));
                }
                // 提交偏移量
                producer.sendOffsetsToTransaction(commits, "consumer-group-1");
                producer.commitTransaction();
            } catch (Exception e) {
                e.printStackTrace();
                producer.abortTransaction();
                break;
            }
        }
    }

}
