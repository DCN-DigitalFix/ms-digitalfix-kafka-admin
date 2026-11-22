package cl.duoc.ms_digitalfix_kafka_admin.exception;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidacion(
            MethodArgumentNotValidException ex, WebRequest request) {

        List<String> detalles = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .toList();

        return ResponseEntity.badRequest().body(ApiError.conDetalles(
                HttpStatus.BAD_REQUEST.value(), "Bad Request",
                "La peticion tiene campos invalidos", path(request), detalles));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleJsonInvalido(
            HttpMessageNotReadableException ex, WebRequest request) {
        return ResponseEntity.badRequest().body(ApiError.of(
                HttpStatus.BAD_REQUEST.value(), "Bad Request",
                "El cuerpo de la peticion no se pudo interpretar", path(request)));
    }

    @ExceptionHandler(RecursoNoEncontradoException.class)
    public ResponseEntity<ApiError> handleNoEncontrado(
            RecursoNoEncontradoException ex, WebRequest request) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ApiError.of(
                HttpStatus.NOT_FOUND.value(), "Not Found", ex.getMessage(), path(request)));
    }

    @ExceptionHandler(OperacionInvalidaException.class)
    public ResponseEntity<ApiError> handleOperacionInvalida(
            OperacionInvalidaException ex, WebRequest request) {
        return ResponseEntity.badRequest().body(ApiError.of(
                HttpStatus.BAD_REQUEST.value(), "Bad Request", ex.getMessage(), path(request)));
    }

    @ExceptionHandler(TopicoProtegidoException.class)
    public ResponseEntity<ApiError> handleTopicoProtegido(
            TopicoProtegidoException ex, WebRequest request) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ApiError.of(
                HttpStatus.CONFLICT.value(), "Conflict", ex.getMessage(), path(request)));
    }

    @ExceptionHandler(KafkaNoDisponibleException.class)
    public ResponseEntity<ApiError> handleKafkaNoDisponible(
            KafkaNoDisponibleException ex, WebRequest request) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ApiError.of(
                HttpStatus.SERVICE_UNAVAILABLE.value(), "Service Unavailable", ex.getMessage(), path(request)));
    }

    private String path(WebRequest request) {
        return request.getDescription(false).replace("uri=", "");
    }
}
