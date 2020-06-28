package eniso.config;

import eniso.kafka.FoobarProducerListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.transaction.KafkaTransactionManager;

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
     * 消费者工厂（禁止自动提交）
     *
     * @return {@link ConsumerFactory}
     */
    @Bean("consumerFactoryDisableAutoCommit")
    public ConsumerFactory<Integer, String> consumerFactoryDisableAutoCommit() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
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
        // 并发监听器容器工厂
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        // 设置消费者工厂
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    /**
     * 监听容器工厂（批量）
     *
     * @param consumerFactory {@link ConsumerFactory} 消费者工厂
     * @return {@link ConcurrentKafkaListenerContainerFactory}
     */
    @Bean("batchListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Integer, String> batchListenerContainerFactory(
            @Autowired ConsumerFactory<Integer, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // 设置并发量，小于或等于 Topic 的分区数
        factory.setConcurrency(5);
        // 设置为批量监听
        factory.setBatchListener(true);
        return factory;
    }

    /**
     * ACK 机制监听容器工厂
     *
     * @param consumerFactoryDisableAutoCommit {@link ConsumerFactory} 消费者工厂
     * @return {@link ConcurrentKafkaListenerContainerFactory}
     */
    @Bean("ackListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Integer, String> ackListenerContainerFactory(
            @Autowired ConsumerFactory<Integer, String> consumerFactoryDisableAutoCommit) {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryDisableAutoCommit);
        factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    /**
     * 以直接监听的方式消费消息（另外，SpringBoot 还提供 @KafkaListener 注解方式）
     *
     * @param consumerFactory 消费者工厂
     * @return {@link KafkaMessageListenerContainer}
     */
    @Bean
    public KafkaMessageListenerContainer<Integer, String> messageListenerContainer(
            @Autowired ConsumerFactory<Integer, String> consumerFactory) {
        ContainerProperties properties = new ContainerProperties("foo.bar.cmd");
        properties.setGroupId("at_bean");
        properties.setMessageListener((MessageListener<Integer, String>) record ->
                log.info("foo.bar.cmd @Bean received: {}", record));
        return new KafkaMessageListenerContainer<>(consumerFactory, properties);
    }

    /**
     * 生产者工厂
     *
     * @return {@link ProducerFactory}
     */
    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        DefaultKafkaProducerFactory<Integer, String> factory =
                new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties());
        factory.setTransactionIdPrefix("tx"); // 设置事务前缀，表示开启事务支持
        return factory;
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

    /**
     * 事务管理器
     *
     * @param producerFactory {@link ProducerFactory} 生产者工厂
     * @return {@link KafkaTransactionManager}
     */
    @Bean
    public KafkaTransactionManager<Integer, String> transactionManager(
            @Autowired ProducerFactory<Integer, String> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    /**
     * 创建/修改主题名称为 foo.bar.code.annotation 的主题，并设置分区数为 5，副本数为 1
     *
     * @return {@link NewTopic} 对象
     */
    @Bean
    public NewTopic initialTopic() {
        return new NewTopic("foo.bar.code.annotation", 5, (short) 1);
    }

    /**
     * Kafka 管理员
     *
     * @return {@link KafkaAdmin}
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(kafkaProperties.buildAdminProperties());
    }

    /**
     * Kafka 管理员客户端
     *
     * @return {@link AdminClient}
     */
    @Bean
    public AdminClient adminClient() {
        return AdminClient.create(kafkaAdmin().getConfig());
    }

}
