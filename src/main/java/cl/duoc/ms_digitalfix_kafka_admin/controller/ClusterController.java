package cl.duoc.ms_digitalfix_kafka_admin.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.ms_digitalfix_kafka_admin.dto.ClusterResponse;
import cl.duoc.ms_digitalfix_kafka_admin.service.KafkaAdminService;

@RestController
@RequestMapping("/api/kafka/cluster")
public class ClusterController {

    private final KafkaAdminService service;

    @Autowired
    public ClusterController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping
    @PreAuthorize("hasRole('Admin') and hasAuthority('SCOPE_OT.Read')")
    public ResponseEntity<ClusterResponse> obtener() {
        return ResponseEntity.ok(service.obtenerCluster());
    }
}
