package cl.duoc.ms_digitalfix_kafka_admin.dto;

/** Fila de GET /api/kafka/consumer-groups: una por grupo, sin el detalle de offsets. */
public record ConsumerGroupResumenResponse(String groupId, String estado, int miembros) {
}
