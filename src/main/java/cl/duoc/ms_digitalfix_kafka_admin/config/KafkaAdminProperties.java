package cl.duoc.ms_digitalfix_kafka_admin.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Ajustes propios de esta API, leidos desde application.yaml bajo
 * "digitalfix.kafka". No tiene relacion con el DigitalfixKafkaProperties de
 * audit/report/workorders (ese configura un productor o consumidor; este
 * configura el AdminClient y la lista de topicos protegidos).
 *
 * @param timeoutMs         limite de cada llamada al AdminClient, en milisegundos
 * @param topicosProtegidos topicos que esta API nunca borra, aunque se lo pidan
 */
@ConfigurationProperties(prefix = "digitalfix.kafka")
public record KafkaAdminProperties(long timeoutMs, List<String> topicosProtegidos) {
}
