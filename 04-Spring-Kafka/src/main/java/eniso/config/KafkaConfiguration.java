package eniso.config;

import eniso.kafka.FoobarProducerListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

/**
 * KafkaConfiguration
 *
 * @author Eniso
 */
@Slf4j
@Configuration
public class KafkaConfiguration {

    private final KafkaProperties kafkaProperties;

    @Autowired
    public KafkaConfiguration(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * 消费者工厂
     *
     * @return {@link ConsumerFactory}
     */
    @Bean("consumerFactory")
    public ConsumerFactory<Integer, String> consumerFactory() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 监听容器工厂
     *
     * @param consumerFactory {@link ConsumerFactory} 消费者工厂
     * @return {@link ConcurrentKafkaListenerContainerFactory}
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, String> listenerContainerFactory(
            @Autowired ConsumerFactory<Integer, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setAckDiscarded(true); // 回复丢弃
        factory.setRecordFilterStrategy(record -> {
            String msg = record.value();
            log.info("msg={}", msg);
            // 返回 true 表示不再交给监听器消费，即丢弃
            return msg != null && msg.startsWith("ignore");
        });
        return factory;
    }

    /**
     * 生产者工厂
     *
     * @return {@link ProducerFactory}
     */
    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        // 指定分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KafkaPartitioner.class.getName());
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Kafka 模板
     *
     * @param producerFactory {@link ProducerFactory} 生产者工厂
     * @return {@link KafkaTemplate}
     */
    @Bean
    public KafkaTemplate<Integer, String> kafkaTemplate(
            @Autowired ProducerFactory<Integer, String> producerFactory) {
        KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setDefaultTopic("foo.bar");
        kafkaTemplate.setProducerListener(new FoobarProducerListener());
        return kafkaTemplate;
    }

}
