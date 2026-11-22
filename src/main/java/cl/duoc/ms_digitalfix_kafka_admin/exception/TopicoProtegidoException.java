package cl.duoc.ms_digitalfix_kafka_admin.exception;

/** El topico pedido es uno de los que sostienen el flujo de eventos del sistema. Mapea a 409. */
public class TopicoProtegidoException extends RuntimeException {
    public TopicoProtegidoException(String message) {
        super(message);
    }
}
