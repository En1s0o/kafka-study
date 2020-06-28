package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * FoobarKafkaTemplate
 *
 * @author Eniso
 */
@Slf4j
@Component
public class FoobarKafkaTemplate {

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    public FoobarKafkaTemplate(KafkaTemplate<Integer, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendDefault() throws ExecutionException, InterruptedException {
        // this.kafkaTemplate.setDefaultTopic("foo.bar"); 设置了默认的主题
        // sendDefault 会将消息发送到默认的主题中
        ListenableFuture<SendResult<Integer, String>> future =
                kafkaTemplate.sendDefault("sendDefault " + UUID.randomUUID());
        SendResult<Integer, String> result = future.get();
        ProducerRecord<Integer, String> record = result.getProducerRecord();
        log.info("sendDefault: {}", record);
    }

    public void sendDefault(Integer key) throws ExecutionException, InterruptedException {
        // this.kafkaTemplate.setDefaultTopic("foo.bar"); 设置了默认的主题
        // sendDefault 会将消息发送到默认的主题中
        ListenableFuture<SendResult<Integer, String>> future =
                kafkaTemplate.sendDefault(key, "sendDefault " + UUID.randomUUID());
        SendResult<Integer, String> result = future.get();
        ProducerRecord<Integer, String> record = result.getProducerRecord();
        log.info("sendDefault({}): {}", key, record);
    }

    public void sendDefault(Integer partition, Integer key) throws ExecutionException, InterruptedException {
        // this.kafkaTemplate.setDefaultTopic("foo.bar"); 设置了默认的主题
        // sendDefault 会将消息发送到默认的主题中
        ListenableFuture<SendResult<Integer, String>> future =
                kafkaTemplate.sendDefault(partition, key, "sendDefault " + UUID.randomUUID());
        SendResult<Integer, String> result = future.get();
        ProducerRecord<Integer, String> record = result.getProducerRecord();
        log.info("sendDefault({}, {}): {}", partition, key, record);
    }

    public void sendDefault(Integer partition, Long timestamp, Integer key) throws ExecutionException, InterruptedException {
        // this.kafkaTemplate.setDefaultTopic("foo.bar"); 设置了默认的主题
        // sendDefault 会将消息发送到默认的主题中
        ListenableFuture<SendResult<Integer, String>> future =
                kafkaTemplate.sendDefault(partition, timestamp, key, "sendDefault " + UUID.randomUUID());
        SendResult<Integer, String> result = future.get();
        ProducerRecord<Integer, String> record = result.getProducerRecord();
        log.info("sendDefault({}, {}, {}): {}", partition, timestamp, key, record);
    }

    public void send(ProducerRecord<Integer, String> producerRecord) throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(producerRecord);
        SendResult<Integer, String> result = future.get();
        ProducerRecord<Integer, String> record = result.getProducerRecord();
        log.info("{}", record);
    }

    public void send(Message<String> message) throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(message);
        SendResult<Integer, String> result = future.get();
        ProducerRecord<Integer, String> record = result.getProducerRecord();
        log.info("{}", record);
    }

    public void executeInTransaction(boolean abort) throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.executeInTransaction(operations -> {
            ListenableFuture<SendResult<Integer, String>> f =
                    operations.sendDefault("Send message in executeInTransaction");
            if (abort) {
                throw new RuntimeException("make some noise");
            }
            return f;
        });
        SendResult<Integer, String> result = future.get();
        ProducerRecord<Integer, String> record = result.getProducerRecord();
        log.info("{}", record);
    }

    @Transactional
    public void atTransactional() {
        kafkaTemplate.sendDefault("Send message in @Transactional");
        throw new RuntimeException("make some noise");
    }

}
