package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

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
    @KafkaListener(topics = "foo.bar")
    public void listen(String msg) {
        log.info("Received from 'foo.bar': {}", msg);
    }

}
