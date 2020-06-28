package eniso.kafka;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class FoobarListenerTest {

    @Autowired
    private KafkaProperties kafkaProperties;

    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Before
    public void beforeTest() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Test
    public void testFoobar() throws InterruptedException, ExecutionException {
        String data = "hello, foobar " + UUID.randomUUID();
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send("foo.bar", data);
        SendResult<Integer, String> sendResult = future.get();
        Assert.assertEquals(sendResult.getProducerRecord().value(), data);
        // 休眠 5 秒，因为监听器收数据需要时间
        Thread.sleep(5000);
    }

}
