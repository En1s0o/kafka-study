package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
class FoobarKafkaTemplateTest {

    @Autowired
    private FoobarKafkaTemplate foobarKafkaTemplate;

    @Test
    void sendDefault() throws ExecutionException, InterruptedException, ParseException {
        foobarKafkaTemplate.sendDefault();
        foobarKafkaTemplate.sendDefault(0x1234);
        foobarKafkaTemplate.sendDefault(1, 0x5678);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        long time = sdf.parse("2020-01-02T03:04:05").getTime();
        foobarKafkaTemplate.sendDefault(1, time, 0x9999);
    }

    @Test
    void send() throws ExecutionException, InterruptedException, ParseException {
        // 通过 ProducerRecord 发送消息
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        long time = sdf.parse("2020-01-02T03:04:05").getTime();
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("Username", "foobar".getBytes()));
        headers.add(new RecordHeader("Token", UUID.randomUUID().toString().getBytes()));
        ProducerRecord<Integer, String> record = new ProducerRecord<>(
                "foo.bar.code", 1, time, 0xABCD,
                "Send message via ProducerRecord", headers);
        foobarKafkaTemplate.send(record);

        // 通过 Message<?> 发送消息
        Map<String, Object> map = new HashMap<>();
        map.put(KafkaHeaders.TOPIC, "foo.bar.code");
        map.put(KafkaHeaders.PARTITION_ID, 3);
        map.put(KafkaHeaders.TIMESTAMP, time);
        map.put(KafkaHeaders.MESSAGE_KEY, 0xCDEF);
        map.put("Username", "foobar");
        map.put("Token", UUID.randomUUID().toString());
        GenericMessage<String> message = new GenericMessage<>(
                "Send message via Message", new MessageHeaders(map));
        foobarKafkaTemplate.send(message);

        Thread.sleep(5000);
    }

    @Test
    public void executeInTransaction() throws ExecutionException, InterruptedException {
        foobarKafkaTemplate.executeInTransaction(true);
    }

    @Test
    public void atTransactional() {
        foobarKafkaTemplate.atTransactional();
    }

}
