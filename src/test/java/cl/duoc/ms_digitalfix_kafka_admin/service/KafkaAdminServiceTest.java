package cl.duoc.ms_digitalfix_kafka_admin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import cl.duoc.ms_digitalfix_kafka_admin.config.KafkaAdminProperties;
import cl.duoc.ms_digitalfix_kafka_admin.dto.ClusterResponse;
import cl.duoc.ms_digitalfix_kafka_admin.dto.CrearTopicoRequest;
import cl.duoc.ms_digitalfix_kafka_admin.dto.TopicoResponse;
import cl.duoc.ms_digitalfix_kafka_admin.exception.OperacionInvalidaException;
import cl.duoc.ms_digitalfix_kafka_admin.exception.RecursoNoEncontradoException;
import cl.duoc.ms_digitalfix_kafka_admin.exception.TopicoProtegidoException;

/**
 * Usa MockAdminClient (el mismo que Kafka ofrece para sus propios tests, en
 * el test-jar de kafka-clients) en vez de mockear Admin con Mockito: as se
 * ejercita el servicio contra una implementacion fiel del protocolo, no
 * contra una simulacion de sus propias llamadas.
 */
class KafkaAdminServiceTest {

    private MockAdminClient admin;
    private KafkaAdminService service;

    private static final List<String> PROTEGIDOS = List.of(
            "digitalfix.workorders.events", "digitalfix.workorders.events.DLT",
            "digitalfix.audit.events", "digitalfix.audit.events.DLT");

    @BeforeEach
    void crearClienteYServicio() {
        admin = new MockAdminClient.Builder().numBrokers(3).build();
        KafkaAdminProperties props = new KafkaAdminProperties(10_000, PROTEGIDOS);
        service = new KafkaAdminService(admin, props);
    }

    @AfterEach
    void cerrarCliente() {
        admin.close();
    }

    @Test
    void creaUnTopicoYLoDevuelveConSusParticionesYReplicas() {
        CrearTopicoRequest request = new CrearTopicoRequest("ventas.eventos", 3, 3, null);

        TopicoResponse creado = service.crearTopico(request);

        assertThat(creado.nombre()).isEqualTo("ventas.eventos");
        assertThat(creado.particiones()).isEqualTo(3);
        assertThat(creado.factorReplicacion()).isEqualTo(3);
    }

    @Test
    void rechazaUnFactorDeReplicacionMayorQueElNumeroDeBrokers() {
        // El cluster de prueba tiene 3 brokers (numBrokers(3) en el @BeforeEach).
        CrearTopicoRequest request = new CrearTopicoRequest("topico-imposible", 1, 5, null);

        assertThatThrownBy(() -> service.crearTopico(request))
                .isInstanceOf(OperacionInvalidaException.class)
                .hasMessageContaining("5")
                .hasMessageContaining("3");
    }

    @Test
    void listaLosTopicosExistentes() {
        service.crearTopico(new CrearTopicoRequest("topico-uno", 2, 1, null));
        service.crearTopico(new CrearTopicoRequest("topico-dos", 1, 1, null));

        List<TopicoResponse> topicos = service.listarTopicos();

        assertThat(topicos).extracting(TopicoResponse::nombre)
                .containsExactlyInAnyOrder("topico-uno", "topico-dos");
    }

    @Test
    void obtenerUnTopicoInexistenteLanzaRecursoNoEncontrado() {
        assertThatThrownBy(() -> service.obtenerTopico("no-existe"))
                .isInstanceOf(RecursoNoEncontradoException.class);
    }

    @Test
    void ampliarParticionesReduciendoLanzaOperacionInvalida() {
        service.crearTopico(new CrearTopicoRequest("topico-reducible", 3, 1, null));

        assertThatThrownBy(() -> service.ampliarParticiones("topico-reducible", 2))
                .isInstanceOf(OperacionInvalidaException.class)
                .hasMessageContaining("reducirlas");
    }

    // No hay test del camino feliz de ampliarParticiones contra MockAdminClient:
    // su createPartitions() lanza "Not implemented yet" (limitacion del mock,
    // no del servicio). Se verifico contra el cluster real en su momento; ver
    // README para el paso a paso manual.

    @Test
    void noPermiteEliminarUnTopicoProtegido() throws Exception {
        Node broker = admin.describeCluster().nodes().get().iterator().next();
        admin.addTopic(false, "digitalfix.workorders.events",
                List.of(new TopicPartitionInfo(0, broker, List.of(broker), List.of(broker))),
                Map.of());

        assertThatThrownBy(() -> service.eliminarTopico("digitalfix.workorders.events"))
                .isInstanceOf(TopicoProtegidoException.class);
    }

    @Test
    void eliminaUnTopicoNoProtegido() {
        service.crearTopico(new CrearTopicoRequest("topico-descartable", 1, 1, null));

        service.eliminarTopico("topico-descartable");

        assertThatThrownBy(() -> service.obtenerTopico("topico-descartable"))
                .isInstanceOf(RecursoNoEncontradoException.class);
    }

    @Test
    void actualizaLaConfiguracionDeUnTopico() {
        service.crearTopico(new CrearTopicoRequest("topico-configurable", 1, 1, null));

        service.actualizarConfiguracion("topico-configurable", Map.of("retention.ms", "604800000"));

        TopicoResponse actualizado = service.obtenerTopico("topico-configurable");
        assertThat(actualizado.configuracion()).containsEntry("retention.ms", "604800000");
    }

    @Test
    void obtieneElEstadoDelCluster() {
        ClusterResponse cluster = service.obtenerCluster();

        assertThat(cluster.brokers()).hasSize(3);
        assertThat(cluster.controlador()).isNotNull();
    }
}
