package cl.duoc.ms_digitalfix_kafka_admin.dto;

import java.util.List;

/** GET /api/kafka/cluster: brokers y cual de ellos es el controller. */
public record ClusterResponse(
        String clusterId,
        BrokerResponse controlador,
        List<BrokerResponse> brokers) {

    public record BrokerResponse(int id, String host, int puerto) {}
}
