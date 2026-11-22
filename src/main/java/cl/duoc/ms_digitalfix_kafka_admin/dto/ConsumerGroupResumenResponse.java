package cl.duoc.ms_digitalfix_kafka_admin.dto;

/** Fila de GET /api/v1/kafka/consumer-groups: una por grupo, sin el detalle de offsets. */
public record ConsumerGroupResumenResponse(String groupId, String estado, int miembros) {
}
