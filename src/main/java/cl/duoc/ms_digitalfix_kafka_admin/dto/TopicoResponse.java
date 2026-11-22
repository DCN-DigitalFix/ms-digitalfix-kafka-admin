package cl.duoc.ms_digitalfix_kafka_admin.dto;

import java.util.Map;

/** Estado de un topico: particiones, replicas y su configuracion efectiva. */
public record TopicoResponse(
        String nombre,
        int particiones,
        int factorReplicacion,
        Map<String, String> configuracion) {
}
