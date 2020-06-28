package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * FoobarListener
 *
 * @author Eniso
 */
@Slf4j
@Component
public class FoobarListener {

    /**
     * 监听主题名称为 foo.bar
     *
     * @param msg 消费的消息
     */
    @KafkaListener(topics = "foo.bar",
            containerFactory = "listenerContainerFactory",
            errorHandler = "consumerAwareErrorHandler")
    public void listenMessage(String msg) {
        log.info("Received from 'foo.bar': {}", msg);
        if ("error".equals(msg)) {
            throw new RuntimeException("make some noise");
        }
    }

    /**
     * 使用 {@link ConsumerRecord} 消费者记录类，包含 Headers 信息、分区信息、时间戳等额外数据
     *
     * @param record {@link ConsumerRecord} 消费者记录
     */
    @KafkaListener(topics = "foo.bar.cmd",
            containerFactory = "listenerContainerFactory",
            errorHandler = "consumerAwareErrorHandler")
    public void listenConsumerRecord(ConsumerRecord<Integer, String> record) {
        log.info("Received from 'foo.bar.cmd': {}", record);
    }

    /**
     * 监听主题名称为 foo.bar
     *
     * @param msg 消费的消息集合（批量）
     */
    @KafkaListener(topics = "foo.bar",
            containerFactory = "batchListenerContainerFactory",
            errorHandler = "consumerAwareErrorHandler")
    public void listenMessage(List<String> msg) {
        log.info("Received batch from 'foo.bar': {}", msg);
        if (msg != null && msg.contains("error")) {
            throw new RuntimeException("make some noise");
        }
    }

    /**
     * 使用 {@link ConsumerRecord} 消费者记录类，包含 Headers 信息、分区信息、时间戳等额外数据
     *
     * @param records 消费者记录集合
     */
    @KafkaListener(topics = "foo.bar.cmd",
            containerFactory = "batchListenerContainerFactory",
            errorHandler = "consumerAwareErrorHandler")
    public void listenConsumerRecord(List<ConsumerRecord<Integer, String>> records) {
        log.info("Received batch from 'foo.bar.cmd': {}", records);
    }

    /**
     * 指定分区消费（可能很少使用）
     *
     * @param records 消费者记录集合
     */
    @KafkaListener(groupId = "bbb", clientIdPrefix = "bwp",
            containerFactory = "batchListenerContainerFactory",
            topicPartitions = {
                    @TopicPartition(topic = "foo.bar.cmd", partitions = {"1", "2"}),
                    @TopicPartition(topic = "foo.bar.cmd", partitions = {"0"}/*,
                            partitionOffsets = @PartitionOffset(partition = "3", initialOffset = "1")*/
                    )
            }
    )
    public void listenConsumerRecordWithPartition(List<ConsumerRecord<Integer, String>> records) {
        log.info("Received batch from 'foo.bar.cmd': {}", records);
    }

    /**
     * 注解方式获取消息头及消息体
     *
     * @param msg         消息体
     * @param topic       主题
     * @param partitionId 分区编号
     * @param timestamp   时间戳
     */
    @KafkaListener(id = "header-example", topics = "foo.bar.cmd")
    public void listenMessageWithHeaders(
            @Payload String msg,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partitionId,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {
        log.info("Received from 'foo.bar.cmd': msg={}, topic={}, partitionId={}, timestamp={}",
                msg, topic, partitionId, timestamp);
    }

    /**
     * ACK 机制的消息监听
     *
     * @param record 消费的消息
     * @param ack    {@link Acknowledgment} 对象
     */
    @KafkaListener(id = "ack-example",
            groupId = "ack-example",
            topics = "foo.bar.ack.cmd",
            containerFactory = "ackListenerContainerFactory")
    public void ackListenerMessage(ConsumerRecord<Integer, String> record, Acknowledgment ack) {
        log.info("Received with ack from 'foo.bar.ack.cmd': {}", record);
        // 如果不调用 ack.acknowledge()，表示拒绝次消息
        ack.acknowledge();
    }

    /**
     * 异常处理 Bean
     *
     * @return {@link ConsumerAwareListenerErrorHandler}
     */
    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler() {
        return (message, exception, consumer) -> {
            log.info("consumerAwareErrorHandler: {}", message.getPayload().toString());

            MessageHeaders headers = message.getHeaders();
            for (String key : headers.keySet()) {
                log.info("k:{}, v:{}", key, headers.get(key));
                // 注意：批量时，value 的类型是 List<?>
            }

            return null;
        };
    }

}
