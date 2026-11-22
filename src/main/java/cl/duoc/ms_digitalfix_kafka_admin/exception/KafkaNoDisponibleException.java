package cl.duoc.ms_digitalfix_kafka_admin.exception;

/** El cluster no respondio a tiempo o la conexion fallo. Mapea a 503. */
public class KafkaNoDisponibleException extends RuntimeException {
    public KafkaNoDisponibleException(String message, Throwable cause) {
        super(message, cause);
    }
}
