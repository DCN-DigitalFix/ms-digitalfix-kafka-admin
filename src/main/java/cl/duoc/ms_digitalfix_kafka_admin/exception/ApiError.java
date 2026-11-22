package cl.duoc.ms_digitalfix_kafka_admin.exception;

import java.time.LocalDateTime;
import java.util.List;

public record ApiError(
        LocalDateTime timestamp,
        int status,
        String error,
        String message,
        String path,
        List<String> detalles) {

    public static ApiError of(int status, String error, String message, String path) {
        return new ApiError(LocalDateTime.now(), status, error, message, path, List.of());
    }

    public static ApiError conDetalles(
            int status, String error, String message, String path, List<String> detalles) {
        return new ApiError(LocalDateTime.now(), status, error, message, path, detalles);
    }

    /** Serializacion manual para los handlers de seguridad. */
    public static String json(int status, String error, String message, String path) {
        return """
                {"timestamp":"%s","status":%d,"error":"%s","message":"%s","path":"%s","detalles":[]}"""
                .formatted(LocalDateTime.now(), status, error, message, path);
    }
}
