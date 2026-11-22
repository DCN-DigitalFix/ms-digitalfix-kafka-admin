package cl.duoc.ms_digitalfix_kafka_admin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/** El nuevo total de particiones del topico, no cuantas agregar. Debe ser mayor al actual. */
public record AmpliarParticionesRequest(
        @NotNull(message = "las particiones son obligatorias")
        @Min(value = 1, message = "debe tener al menos 1 particion")
        Integer particiones) {
}
