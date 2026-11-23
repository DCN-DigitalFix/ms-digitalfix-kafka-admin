package cl.duoc.ms_digitalfix_kafka_admin.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.ms_digitalfix_kafka_admin.dto.ConsumerGroupResponse;
import cl.duoc.ms_digitalfix_kafka_admin.dto.ConsumerGroupResumenResponse;
import cl.duoc.ms_digitalfix_kafka_admin.service.KafkaAdminService;

/** Monitoreo de consumer groups: miembros, offsets y lag por particion. Solo lectura. */
@RestController
@RequestMapping("/api/v1/kafka/consumer-groups")
public class ConsumerGroupController {

    @Autowired
    private KafkaAdminService service;

    @GetMapping
    @PreAuthorize("hasRole('Admin') and hasAuthority('SCOPE_OT.Read')")
    public ResponseEntity<List<ConsumerGroupResumenResponse>> listar() {
        return ResponseEntity.ok(service.listarConsumerGroups());
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasRole('Admin') and hasAuthority('SCOPE_OT.Read')")
    public ResponseEntity<ConsumerGroupResponse> obtener(@PathVariable String id) {
        return ResponseEntity.ok(service.obtenerConsumerGroup(id));
    }
}
