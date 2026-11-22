package cl.duoc.ms_digitalfix_kafka_admin.dto;

/** Resultado de una operacion de administracion. */
public record OperacionResponse(String recurso, String accion, String detalle) {

    public static OperacionResponse creado(String recurso, String detalle) {
        return new OperacionResponse(recurso, "CREADO", detalle);
    }

    public static OperacionResponse actualizado(String recurso, String detalle) {
        return new OperacionResponse(recurso, "ACTUALIZADO", detalle);
    }

    public static OperacionResponse eliminado(String recurso, String detalle) {
        return new OperacionResponse(recurso, "ELIMINADO", detalle);
    }
}
