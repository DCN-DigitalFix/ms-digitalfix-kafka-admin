package cl.duoc.ms_digitalfix_kafka_admin.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.ms_digitalfix_kafka_admin.dto.ActualizarConfigRequest;
import cl.duoc.ms_digitalfix_kafka_admin.dto.AmpliarParticionesRequest;
import cl.duoc.ms_digitalfix_kafka_admin.dto.CrearTopicoRequest;
import cl.duoc.ms_digitalfix_kafka_admin.dto.OperacionResponse;
import cl.duoc.ms_digitalfix_kafka_admin.dto.TopicoResponse;
import cl.duoc.ms_digitalfix_kafka_admin.service.KafkaAdminService;

import jakarta.validation.Valid;

/** Administracion de topicos: crear, listar, modificar configuracion, ampliar particiones, borrar. */
@RestController
@RequestMapping("/api/kafka/topics")
public class TopicController {

    private final KafkaAdminService service;

    @Autowired
    public TopicController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping
    @PreAuthorize("hasRole('Admin') and hasAuthority('SCOPE_OT.Read')")
    public ResponseEntity<List<TopicoResponse>> listar() {
        return ResponseEntity.ok(service.listarTopicos());
    }

    @GetMapping("/{nombre}")
    @PreAuthorize("hasRole('Admin') and hasAuthority('SCOPE_OT.Read')")
    public ResponseEntity<TopicoResponse> obtener(@PathVariable String nombre) {
        return ResponseEntity.ok(service.obtenerTopico(nombre));
    }

    @PostMapping
    @PreAuthorize("hasRole('Admin') and hasAuthority('SCOPE_OT.Create')")
    public ResponseEntity<TopicoResponse> crear(@Valid @RequestBody CrearTopicoRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(service.crearTopico(request));
    }

    @PutMapping("/{nombre}/config")
    @PreAuthorize("hasRole('Admin') and hasAuthority('SCOPE_OT.Create')")
    public ResponseEntity<OperacionResponse> actualizarConfiguracion(
            @PathVariable String nombre, @Valid @RequestBody ActualizarConfigRequest request) {
        service.actualizarConfiguracion(nombre, request.configuraciones());
        return ResponseEntity.ok(OperacionResponse.actualizado(
                nombre, "Configuracion actualizada: " + request.configuraciones().keySet()));
    }

    @PutMapping("/{nombre}/partitions")
    @PreAuthorize("hasRole('Admin') and hasAuthority('SCOPE_OT.Create')")
    public ResponseEntity<OperacionResponse> ampliarParticiones(
            @PathVariable String nombre, @Valid @RequestBody AmpliarParticionesRequest request) {
        service.ampliarParticiones(nombre, request.particiones());
        return ResponseEntity.ok(OperacionResponse.actualizado(
                nombre, "Particiones ampliadas a " + request.particiones()));
    }

    @DeleteMapping("/{nombre}")
    @PreAuthorize("hasRole('Admin') and hasAuthority('SCOPE_OT.Create')")
    public ResponseEntity<OperacionResponse> eliminar(@PathVariable String nombre) {
        service.eliminarTopico(nombre);
        return ResponseEntity.ok(OperacionResponse.eliminado(nombre, "Topico eliminado"));
    }
}
