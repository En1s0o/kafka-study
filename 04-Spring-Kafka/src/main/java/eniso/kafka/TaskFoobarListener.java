package eniso.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * TaskFoobarListener
 *
 * @author Eniso
 */
@Slf4j
@Component
@EnableScheduling
public class TaskFoobarListener {

    private final KafkaListenerEndpointRegistry registry;

    @Autowired
    public TaskFoobarListener(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @KafkaListener(id = "task-example",
            topics = "foo.bar.cmd",
            autoStartup = "false",
            containerFactory = "listenerContainerFactory")
    public void durableListener(String msg) {
        log.info("Received (task) from 'foo.bar.cmd': {}", msg);
    }

    // 定时器，每天早上 2 点开启监听
    @Scheduled(cron = "0 0 2 * * ?")
    public void startListener() {
        if (!registry.getListenerContainer("task-example").isRunning()) {
            registry.getListenerContainer("task-example").start();
        }
        registry.getListenerContainer("task-example").resume();
    }

    // 定时器，每天早上 8 点关闭监听
    @Scheduled(cron = "0 0 8 * * ?")
    public void shutDownListener() {
        registry.getListenerContainer("task-example").pause();
    }

}
