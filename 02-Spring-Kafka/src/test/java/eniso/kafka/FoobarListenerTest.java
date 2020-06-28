package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
public class FoobarListenerTest {

    @Autowired
    private AdminClient adminClient;

    @Before
    public void beforeTest() {
    }

    @After
    public void afterTest() {
        adminClient.close();
    }

    @Test
    public void testCreateTopic() throws InterruptedException, ExecutionException {
        final String topicName = "foo.bar.code.client";
        if (adminClient.listTopics().names().get().contains(topicName)) {
            log.info("delete topic first");
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
        }
        // 代码执行至此，请求删除主题成功，主题可能未被真正删除
        Thread.sleep(1000);
        NewTopic topic = new NewTopic(topicName, 5, (short) 1);
        CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(topic));
        result.all().get(); // wait for complete
        Assert.assertTrue(adminClient.listTopics().names().get().contains(topicName));
    }

    @Test
    public void testDescribeTopics() throws ExecutionException, InterruptedException {
        log.info("{}", adminClient.listTopics().namesToListings().get());
        final String topicName = "foo.bar.code.client";
        DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topicName));
        log.info("{}", result.all().get());
    }

    @Test
    public void testAlterTopicPartition() throws ExecutionException, InterruptedException {
        final String topicName = "foo.bar.code.client";
        Map<String, NewPartitions> partitions = new HashMap<>();
        partitions.put(topicName, NewPartitions.increaseTo(7));
        CreatePartitionsResult result = adminClient.createPartitions(partitions);
        result.all().get(); // wait for complete
        Assert.assertTrue(adminClient.listTopics().names().get().contains(topicName));
    }

}
