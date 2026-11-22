package cl.duoc.ms_digitalfix_kafka_admin.exception;

/**
 * La peticion era sintacticamente valida pero la operacion no se puede
 * aplicar: reducir particiones, factor de replicacion mayor al numero de
 * brokers, configuracion invalida. Mapea a 400.
 */
public class OperacionInvalidaException extends RuntimeException {

    public OperacionInvalidaException(String message) {
        super(message);
    }

    public OperacionInvalidaException(String message, Throwable cause) {
        super(message, cause);
    }
}
