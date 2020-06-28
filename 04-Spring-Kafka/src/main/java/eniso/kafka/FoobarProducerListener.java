package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;

/**
 * FoobarProducerListener
 *
 * @author Eniso
 */
@Slf4j
public class FoobarProducerListener implements ProducerListener<Integer, String> {

    @Override
    public void onSuccess(ProducerRecord<Integer, String> producerRecord, RecordMetadata recordMetadata) {
        log.info("{}", producerRecord);
        log.info("{}", recordMetadata);
    }

    @Override
    public void onError(ProducerRecord<Integer, String> producerRecord, Exception exception) {
        log.info("{}", producerRecord);
        log.info(exception.getMessage(), exception);
    }

}
