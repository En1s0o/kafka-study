package eniso.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * KafkaConfiguration
 *
 * @author Eniso
 */
@Slf4j
@Configuration
public class KafkaConfiguration {

    private final KafkaProperties kafkaProperties;

    public KafkaConfiguration(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
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

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(kafkaProperties.buildAdminProperties());
    }

    @Bean
    public AdminClient adminClient() {
        return AdminClient.create(kafkaAdmin().getConfig());
    }

}
