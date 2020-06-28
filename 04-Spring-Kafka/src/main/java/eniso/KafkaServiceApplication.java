package eniso;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * KafkaServiceApplication
 *
 * @author Eniso
 */
@Slf4j
@SpringBootApplication
public class KafkaServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaServiceApplication.class, args);
    }

}
