package cl.duoc.ms_digitalfix_kafka_admin.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;

import cl.duoc.ms_digitalfix_kafka_admin.config.KafkaAdminProperties;
import cl.duoc.ms_digitalfix_kafka_admin.dto.ClusterResponse;
import cl.duoc.ms_digitalfix_kafka_admin.dto.ConsumerGroupResponse;
import cl.duoc.ms_digitalfix_kafka_admin.dto.ConsumerGroupResumenResponse;
import cl.duoc.ms_digitalfix_kafka_admin.dto.CrearTopicoRequest;
import cl.duoc.ms_digitalfix_kafka_admin.dto.TopicoResponse;
import cl.duoc.ms_digitalfix_kafka_admin.exception.KafkaNoDisponibleException;
import cl.duoc.ms_digitalfix_kafka_admin.exception.OperacionInvalidaException;
import cl.duoc.ms_digitalfix_kafka_admin.exception.RecursoNoEncontradoException;
import cl.duoc.ms_digitalfix_kafka_admin.exception.TopicoProtegidoException;

/**
 * Unico punto del servicio que habla con el AdminClient de Kafka. Los
 * controladores llaman a metodos de alto nivel y no conocen NewTopic,
 * ConfigResource ni KafkaFuture; si algun dia cambia la libreria de acceso al
 * cluster, solo esta clase se reescribe.
 *
 * Cada operacion queda registrada con el subject del JWT de quien la pidio:
 * administrar topicos y particiones es irreversible, asi que conviene poder
 * auditar quien hizo que.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    @Autowired
    private Admin admin;

    @Autowired
    private KafkaAdminProperties props;

    // -----------------------------------------------------------------------
    // Topicos
    // -----------------------------------------------------------------------

    public List<TopicoResponse> listarTopicos() {
        try {
            Collection<String> nombres = admin.listTopics().names().get(timeout(), TimeUnit.MILLISECONDS);
            if (nombres.isEmpty()) {
                return List.of();
            }
            return describirTopicos(nombres);
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudieron listar los topicos");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudieron listar los topicos", e);
        }
    }

    public TopicoResponse obtenerTopico(String nombre) {
        List<TopicoResponse> resultado = describirTopicos(List.of(nombre));
        return resultado.get(0);
    }

    public TopicoResponse crearTopico(CrearTopicoRequest request) {
        validarFactorReplicacion(request.factorReplicacion());

        NewTopic nuevo = new NewTopic(
                request.nombre(), request.particiones(), request.factorReplicacion().shortValue());
        if (request.configuraciones() != null && !request.configuraciones().isEmpty()) {
            nuevo.configs(request.configuraciones());
        }

        try {
            admin.createTopics(List.of(nuevo)).all().get(timeout(), TimeUnit.MILLISECONDS);
            log.info("Topico '{}' creado por {} ({} particiones, factor {})",
                    request.nombre(), actorActual(), request.particiones(), request.factorReplicacion());
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudo crear el topico '" + request.nombre() + "'");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudo crear el topico '" + request.nombre() + "'", e);
        }

        return obtenerTopico(request.nombre());
    }

    public void actualizarConfiguracion(String nombre, Map<String, String> configuraciones) {
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);

        List<AlterConfigOp> operaciones = configuraciones.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();

        try {
            admin.incrementalAlterConfigs(Map.of(recurso, operaciones))
                    .all().get(timeout(), TimeUnit.MILLISECONDS);
            log.info("Configuracion de '{}' actualizada por {}: {}", nombre, actorActual(), configuraciones);
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudo actualizar la configuracion de '" + nombre + "'");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudo actualizar la configuracion de '" + nombre + "'", e);
        }
    }

    /** Kafka no permite reducir particiones: se valida antes de llamar al broker para un 400 claro. */
    public void ampliarParticiones(String nombre, int nuevoTotal) {
        TopicoResponse actual = obtenerTopico(nombre);

        if (nuevoTotal <= actual.particiones()) {
            throw new OperacionInvalidaException(
                    "El topico '" + nombre + "' ya tiene " + actual.particiones()
                            + " particiones. Kafka no permite reducirlas, solo aumentarlas"
                            + " (pediste " + nuevoTotal + ")");
        }

        try {
            admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(nuevoTotal)))
                    .all().get(timeout(), TimeUnit.MILLISECONDS);
            log.info("Particiones de '{}' ampliadas de {} a {} por {}",
                    nombre, actual.particiones(), nuevoTotal, actorActual());
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudieron ampliar las particiones de '" + nombre + "'");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudieron ampliar las particiones de '" + nombre + "'", e);
        }
    }

    public void eliminarTopico(String nombre) {
        if (props.topicosProtegidos() != null && props.topicosProtegidos().contains(nombre)) {
            throw new TopicoProtegidoException(
                    "'" + nombre + "' es un topico protegido: sostiene el flujo de eventos del sistema"
                            + " y no se puede borrar desde esta API");
        }

        try {
            admin.deleteTopics(List.of(nombre)).all().get(timeout(), TimeUnit.MILLISECONDS);
            log.info("Topico '{}' eliminado por {}", nombre, actorActual());
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudo eliminar el topico '" + nombre + "'");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudo eliminar el topico '" + nombre + "'", e);
        }
    }

    // -----------------------------------------------------------------------
    // Consumer groups
    // -----------------------------------------------------------------------

    public List<ConsumerGroupResumenResponse> listarConsumerGroups() {
        try {
            Collection<ConsumerGroupListing> listados =
                    admin.listConsumerGroups().all().get(timeout(), TimeUnit.MILLISECONDS);

            if (listados.isEmpty()) {
                return List.of();
            }

            List<String> ids = listados.stream().map(ConsumerGroupListing::groupId).toList();
            Map<String, ConsumerGroupDescription> descripciones =
                    admin.describeConsumerGroups(ids).all().get(timeout(), TimeUnit.MILLISECONDS);

            return listados.stream()
                    .map(l -> {
                        ConsumerGroupDescription d = descripciones.get(l.groupId());
                        int miembros = d != null ? d.members().size() : 0;
                        String estado = d != null ? d.state().toString() : l.state().map(Object::toString).orElse("DESCONOCIDO");
                        return new ConsumerGroupResumenResponse(l.groupId(), estado, miembros);
                    })
                    .toList();
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudieron listar los consumer groups");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudieron listar los consumer groups", e);
        }
    }

    public ConsumerGroupResponse obtenerConsumerGroup(String groupId) {
        ConsumerGroupDescription descripcion;
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsetsConfirmados;

        try {
            Map<String, ConsumerGroupDescription> descripciones =
                    admin.describeConsumerGroups(List.of(groupId)).all().get(timeout(), TimeUnit.MILLISECONDS);
            descripcion = descripciones.get(groupId);

            if (descripcion == null || descripcion.members().isEmpty() && esGrupoInexistente(groupId)) {
                throw new RecursoNoEncontradoException("No existe el consumer group '" + groupId + "'");
            }

            offsetsConfirmados = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(timeout(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudo describir el consumer group '" + groupId + "'");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudo describir el consumer group '" + groupId + "'", e);
        }

        List<ConsumerGroupResponse.MiembroResponse> miembros = descripcion.members().stream()
                .map(this::aMiembro)
                .toList();

        List<ConsumerGroupResponse.OffsetParticionResponse> offsets =
                offsetsConUltimoOffset(offsetsConfirmados);

        return new ConsumerGroupResponse(groupId, descripcion.state().toString(), miembros, offsets);
    }

    /**
     * listConsumerGroupOffsets no falla si el grupo no existe: devuelve un
     * mapa vacio. Se distingue consultando listConsumerGroups.
     */
    private boolean esGrupoInexistente(String groupId) throws ExecutionException, InterruptedException, TimeoutException {
        Collection<ConsumerGroupListing> listados =
                admin.listConsumerGroups().all().get(timeout(), TimeUnit.MILLISECONDS);
        return listados.stream().noneMatch(l -> l.groupId().equals(groupId));
    }

    private ConsumerGroupResponse.MiembroResponse aMiembro(MemberDescription m) {
        List<String> particiones = m.assignment().topicPartitions().stream()
                .map(tp -> tp.topic() + "-" + tp.partition())
                .sorted()
                .toList();
        return new ConsumerGroupResponse.MiembroResponse(
                m.consumerId(), m.clientId(), m.host(), particiones);
    }

    private List<ConsumerGroupResponse.OffsetParticionResponse> offsetsConUltimoOffset(
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> confirmados) {

        if (confirmados.isEmpty()) {
            return List.of();
        }

        Map<TopicPartition, OffsetSpec> consulta = new HashMap<>();
        confirmados.keySet().forEach(tp -> consulta.put(tp, OffsetSpec.latest()));

        Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> finales;
        try {
            finales = admin.listOffsets(consulta).all().get(timeout(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudieron leer los offsets finales");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudieron leer los offsets finales", e);
        }

        List<ConsumerGroupResponse.OffsetParticionResponse> resultado = new ArrayList<>();
        confirmados.forEach((tp, metadata) -> {
            Long actual = metadata != null ? metadata.offset() : null;
            long fin = finales.containsKey(tp) ? finales.get(tp).offset() : 0L;
            Long lag = actual != null ? fin - actual : null;
            resultado.add(new ConsumerGroupResponse.OffsetParticionResponse(
                    tp.topic(), tp.partition(), actual, fin, lag));
        });

        return resultado.stream()
                .sorted((a, b) -> a.topico().equals(b.topico())
                        ? Integer.compare(a.particion(), b.particion())
                        : a.topico().compareTo(b.topico()))
                .toList();
    }

    // -----------------------------------------------------------------------
    // Cluster
    // -----------------------------------------------------------------------

    public ClusterResponse obtenerCluster() {
        try {
            var resultado = admin.describeCluster();
            String clusterId = resultado.clusterId().get(timeout(), TimeUnit.MILLISECONDS);
            Node controller = resultado.controller().get(timeout(), TimeUnit.MILLISECONDS);
            Collection<Node> nodos = resultado.nodes().get(timeout(), TimeUnit.MILLISECONDS);

            List<ClusterResponse.BrokerResponse> brokers = nodos.stream()
                    .map(n -> new ClusterResponse.BrokerResponse(n.id(), n.host(), n.port()))
                    .sorted((a, b) -> Integer.compare(a.id(), b.id()))
                    .toList();

            ClusterResponse.BrokerResponse controlador =
                    new ClusterResponse.BrokerResponse(controller.id(), controller.host(), controller.port());

            return new ClusterResponse(clusterId, controlador, brokers);
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudo describir el cluster");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudo describir el cluster", e);
        }
    }

    // -----------------------------------------------------------------------

    private List<TopicoResponse> describirTopicos(Collection<String> nombres) {
        try {
            Map<String, TopicDescription> descripciones =
                    admin.describeTopics(nombres).allTopicNames().get(timeout(), TimeUnit.MILLISECONDS);

            List<ConfigResource> recursos = nombres.stream()
                    .map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n))
                    .toList();
            Map<ConfigResource, Config> configuraciones =
                    admin.describeConfigs(recursos).all().get(timeout(), TimeUnit.MILLISECONDS);

            List<TopicoResponse> respuesta = new ArrayList<>();
            for (String nombre : nombres) {
                TopicDescription descripcion = descripciones.get(nombre);
                int particiones = descripcion.partitions().size();
                int factorReplicacion = descripcion.partitions().isEmpty()
                        ? 0 : descripcion.partitions().get(0).replicas().size();

                Config config = configuraciones.get(new ConfigResource(ConfigResource.Type.TOPIC, nombre));
                Map<String, String> configMap = new HashMap<>();
                if (config != null) {
                    for (ConfigEntry entry : config.entries()) {
                        configMap.put(entry.name(), entry.value());
                    }
                }

                respuesta.add(new TopicoResponse(nombre, particiones, factorReplicacion, configMap));
            }
            return respuesta;
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudo describir el topico");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudo describir el topico", e);
        }
    }

    private void validarFactorReplicacion(int factorReplicacion) {
        int brokers;
        try {
            brokers = admin.describeCluster().nodes().get(timeout(), TimeUnit.MILLISECONDS).size();
        } catch (ExecutionException e) {
            throw traducir(e, "No se pudo consultar el tamano del cluster");
        } catch (InterruptedException | TimeoutException e) {
            throw noDisponible("No se pudo consultar el tamano del cluster", e);
        }

        if (factorReplicacion > brokers) {
            throw new OperacionInvalidaException(
                    "El factor de replicacion (" + factorReplicacion + ") no puede ser mayor "
                            + "al numero de brokers del cluster (" + brokers + ")");
        }
    }

    private long timeout() {
        return props.timeoutMs();
    }

    private String actorActual() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof JwtAuthenticationToken jwtAuth) {
            return jwtAuth.getToken().getSubject();
        }
        return "sistema";
    }

    private RuntimeException traducir(ExecutionException e, String contexto) {
        Throwable causa = e.getCause() != null ? e.getCause() : e;

        if (causa instanceof UnknownTopicOrPartitionException || causa instanceof GroupIdNotFoundException) {
            return new RecursoNoEncontradoException(contexto + ": " + causa.getMessage());
        }
        if (causa instanceof InvalidPartitionsException
                || causa instanceof InvalidReplicationFactorException
                || causa instanceof InvalidConfigurationException
                || causa instanceof TopicExistsException) {
            return new OperacionInvalidaException(contexto + ": " + causa.getMessage());
        }
        return new KafkaNoDisponibleException(contexto + ": " + causa.getMessage(), causa);
    }

    private KafkaNoDisponibleException noDisponible(String contexto, Exception causa) {
        if (causa instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new KafkaNoDisponibleException(contexto + ": el cluster no respondio a tiempo", causa);
    }
}
