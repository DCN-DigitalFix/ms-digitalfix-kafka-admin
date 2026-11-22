package cl.duoc.ms_digitalfix_kafka_admin.dto;

import java.util.Map;

import jakarta.validation.constraints.NotEmpty;

/** P. ej. {"retention.ms": "604800000", "min.insync.replicas": "2"}. */
public record ActualizarConfigRequest(
        @NotEmpty(message = "debe indicar al menos una configuracion")
        Map<String, String> configuraciones) {
}
