package cl.duoc.ms_digitalfix_kafka_admin.dto;

import java.util.Map;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record CrearTopicoRequest(

        @NotBlank(message = "el nombre es obligatorio")
        @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "el nombre solo admite letras, numeros, punto, guion y guion bajo")
        String nombre,

        @NotNull(message = "las particiones son obligatorias")
        @Min(value = 1, message = "debe tener al menos 1 particion")
        Integer particiones,

        @NotNull(message = "el factor de replicacion es obligatorio")
        @Min(value = 1, message = "debe tener al menos 1 replica")
        Integer factorReplicacion,

        /** Configuraciones iniciales, p. ej. retention.ms o cleanup.policy. Puede ser null. */
        Map<String, String> configuraciones) {
}
