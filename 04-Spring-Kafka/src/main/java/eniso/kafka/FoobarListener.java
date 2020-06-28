package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.messaging.MessageHeaders;
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
     * 监听主题名称为 foo.bar.cmd
     *
     * @param msg 消费的消息
     */
    @KafkaListener(topics = "foo.bar.cmd",
            containerFactory = "listenerContainerFactory",
            errorHandler = "consumerAwareErrorHandler")
    public void listenMessage(String msg) {
        log.info("Received from 'foo.bar.cmd': {}", msg);
        if ("error".equals(msg)) {
            throw new RuntimeException("make some noise");
        }
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
