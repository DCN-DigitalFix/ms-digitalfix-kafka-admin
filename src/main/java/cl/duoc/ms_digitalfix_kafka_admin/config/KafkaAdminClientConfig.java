package cl.duoc.ms_digitalfix_kafka_admin.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Admin es la unica puerta de entrada al cluster: no hay productor ni
 * consumidor en este servicio, solo administracion.
 *
 * Es un bean singleton de un solo Admin reutilizado por todas las peticiones
 * (el cliente administra su propio pool de conexiones a los brokers); Spring
 * lo cierra solo al apagar el contexto porque Admin implementa Closeable.
 */
@Configuration
@EnableConfigurationProperties(KafkaAdminProperties.class)
public class KafkaAdminClientConfig {

    // No se llama "kafkaAdmin": Spring Kafka ya autoconfigura un bean con ese
    // nombre (su propia clase KafkaAdmin, para aprovisionar @Bean NewTopic),
    // y el choque de nombres tumba el contexto con
    // BeanDefinitionOverrideException aunque los tipos sean distintos.
    @Bean
    public Admin adminClient(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        return Admin.create(config);
    }
}
