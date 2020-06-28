package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

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

    public void send(ProducerRecord<Integer, String> producerRecord) throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(producerRecord);
        SendResult<Integer, String> result = future.get();
        ProducerRecord<Integer, String> record = result.getProducerRecord();
        log.info("{}", record);
    }

}
