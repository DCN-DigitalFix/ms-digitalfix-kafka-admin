package cl.duoc.ms_digitalfix_kafka_admin.dto;

import java.util.List;

/** Detalle de GET /api/v1/kafka/consumer-groups/{id}: miembros y offset por particion asignada. */
public record ConsumerGroupResponse(
        String groupId,
        String estado,
        List<MiembroResponse> miembros,
        List<OffsetParticionResponse> offsets) {

    public record MiembroResponse(
            String memberId,
            String clientId,
            String host,
            List<String> particionesAsignadas) {
    }

    /**
     * @param offsetActual ultimo offset confirmado por el grupo; null si nunca consumio esa particion
     * @param offsetFinal  log end offset actual de la particion
     * @param lag          offsetFinal - offsetActual; null si offsetActual es null (no hay como calcularlo)
     */
    public record OffsetParticionResponse(
            String topico,
            int particion,
            Long offsetActual,
            long offsetFinal,
            Long lag) {
    }
}
