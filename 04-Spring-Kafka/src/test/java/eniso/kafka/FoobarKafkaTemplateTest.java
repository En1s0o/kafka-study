package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
class FoobarKafkaTemplateTest {

    @Autowired
    private FoobarKafkaTemplate foobarKafkaTemplate;

    @Test
    void sendPartitioner() throws ExecutionException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            ProducerRecord<Integer, String> record = new ProducerRecord<>(
                    "foo.bar.cmd", null, System.currentTimeMillis(), 0xABCD,
                    "Send message via ProducerRecord " + i);
            foobarKafkaTemplate.send(record);
            Thread.sleep(100);
        }
        Thread.sleep(5000);
    }

    @Test
    void send() throws ExecutionException, InterruptedException {
        ProducerRecord<Integer, String> record = new ProducerRecord<>(
                "foo.bar.cmd", 1, System.currentTimeMillis(), 0xABCD,
                "Send message via ProducerRecord");
        foobarKafkaTemplate.send(record);

        // 以 ignore 开头
        ProducerRecord<Integer, String> record2 = new ProducerRecord<>(
                "foo.bar.cmd", 1, System.currentTimeMillis(), 0xABCD,
                "ignore Send message via ProducerRecord");
        foobarKafkaTemplate.send(record2);

        Thread.sleep(5000);
    }

}
