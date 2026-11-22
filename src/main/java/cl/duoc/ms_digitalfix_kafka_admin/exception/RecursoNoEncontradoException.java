package cl.duoc.ms_digitalfix_kafka_admin.exception;

/** El topico o consumer group referenciado no existe en el cluster. Mapea a 404. */
public class RecursoNoEncontradoException extends RuntimeException {
    public RecursoNoEncontradoException(String message) {
        super(message);
    }
}
