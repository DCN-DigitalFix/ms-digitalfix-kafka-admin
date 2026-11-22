# ms-digitalfix-kafka-admin

Administrador del clúster de Kafka del sistema DigitalFix: API REST para
crear, consultar, reconfigurar, ampliar particiones y borrar tópicos; listar
consumer groups y ver sus offsets y lag por partición; y consultar el
estado del clúster (brokers, controlador). Es el componente que la pauta de
EP5 pide como "administrador de Kafka" y el que alimenta el monitoreo de
EP6 (offsets, lag, réplicas).

No consume ni produce eventos de negocio: habla con el clúster directamente
a través de la API de administración de Kafka (`org.apache.kafka.clients.admin.Admin`),
no a través de un tópico.

---

## Por qué un microservicio aparte

Todas las operaciones administrativas (crear/borrar tópicos, ver consumer
groups) quedan en un solo lugar con su propia seguridad, en vez de
repartirse entre `audit`, `report` y `workorders`. Ninguno de esos tres
necesita el cliente `Admin` de Kafka para su trabajo normal (consumir o
producir), así que no tiene sentido cargarlos con esa dependencia y esa
superficie de permisos.

---

## Seguridad

Todo requiere rol `Admin`. Las operaciones de lectura piden además el scope
`SCOPE_OT.Read`; las de escritura (crear, reconfigurar, ampliar particiones,
borrar), `SCOPE_OT.Create`. No hay rol Supervisor/Cliente/Auditor aquí: la
administración del clúster es exclusiva de Admin.

---

## Tópicos protegidos

```yaml
digitalfix:
  kafka:
    topicos-protegidos:
      - digitalfix.workorders.events
      - digitalfix.workorders.events.DLT
      - digitalfix.audit.events
      - digitalfix.audit.events.DLT
```

`DELETE /api/v1/kafka/topics/{nombre}` responde 409 si el nombre está en esta
lista, antes de llamar al broker. Evita borrar por error los tópicos que
sostienen el resto del sistema; para los demás (tópicos de prueba creados
desde esta misma API) sí se puede borrar.

---

## API

| Método | Ruta | Scope | Descripción |
|---|---|---|---|
| GET | `/api/v1/kafka/topics` | `OT.Read` | Lista todos los tópicos con particiones, factor de replicación y configuración |
| GET | `/api/v1/kafka/topics/{nombre}` | `OT.Read` | Detalle de un tópico; 404 si no existe |
| POST | `/api/v1/kafka/topics` | `OT.Create` | Crea un tópico (`nombre`, `particiones`, `factorReplicacion`, `configuracion` opcional) |
| PUT | `/api/v1/kafka/topics/{nombre}/config` | `OT.Create` | Modifica configuración (`configuraciones`: mapa clave-valor, p. ej. `retention.ms`) |
| PUT | `/api/v1/kafka/topics/{nombre}/partitions` | `OT.Create` | Amplía particiones a un **total** (no un delta); 400 si el total no es mayor al actual |
| DELETE | `/api/v1/kafka/topics/{nombre}` | `OT.Create` | Borra el tópico; 409 si está protegido |
| GET | `/api/v1/kafka/consumer-groups` | `OT.Read` | Lista los consumer groups con su estado y cantidad de miembros |
| GET | `/api/v1/kafka/consumer-groups/{groupId}` | `OT.Read` | Detalle: miembros (con sus particiones asignadas) y offsets/lag por partición |
| GET | `/api/v1/kafka/cluster` | `OT.Read` | Brokers del clúster y cuál es el controlador |
| GET | `/api/health` | público | — |

Todas las rutas (salvo `/api/health`) también requieren rol `Admin`.

### Reglas de validación antes de tocar el broker

- `crearTopico`: si `factorReplicacion` pide más réplicas que brokers
  disponibles (`describeCluster()`), se rechaza con 400 sin llamar a Kafka.
- `ampliarParticiones`: Kafka no puede reducir particiones; si el total
  pedido es menor o igual al actual, se rechaza con 400 antes de llamar al
  broker, con un mensaje explícito sobre por qué.
- `eliminarTopico`: 409 si el nombre está en `topicos-protegidos`.

### Traducción de errores del cliente de Kafka

| Excepción del cliente Kafka | HTTP |
|---|---|
| `UnknownTopicOrPartitionException`, `GroupIdNotFoundException` | 404 |
| `InvalidPartitionsException`, `InvalidReplicationFactorException`, `InvalidConfigurationException`, `TopicExistsException` | 400 |
| Cualquier otra (timeout, broker caído) | 503 |

---

## Estructura

```
config/
  KafkaAdminProperties.java        timeoutMs y topicosProtegidos, desde application.yaml
  KafkaAdminClientConfig.java      bean Admin (named adminClient, no kafkaAdmin)
  SecurityConfig.java              JWT, audiencia, CORS
dto/
  TopicoResponse, CrearTopicoRequest, ActualizarConfigRequest, AmpliarParticionesRequest
  OperacionResponse
  ConsumerGroupResumenResponse, ConsumerGroupResponse (con MiembroResponse, OffsetParticionResponse)
  ClusterResponse (con BrokerResponse)
service/KafkaAdminService.java     toda la lógica contra el Admin client
controller/
  TopicController.java             /api/v1/kafka/topics
  ConsumerGroupController.java     /api/v1/kafka/consumer-groups
  ClusterController.java           /api/v1/kafka/cluster
  HealthController.java            /api/health (publico)
exception/
  RecursoNoEncontradoException (404), OperacionInvalidaException (400)
  TopicoProtegidoException (409), KafkaNoDisponibleException (503)
```

### Por qué el bean se llama `adminClient` y no `kafkaAdmin`

Spring Kafka autoconfigura su propio bean llamado `kafkaAdmin` (de tipo
`org.springframework.kafka.core.KafkaAdmin`, para aprovisionar `@Bean
NewTopic`). Un `@Bean` propio con el mismo nombre de método, aunque sea de
tipo `org.apache.kafka.clients.admin.Admin`, choca contra ese nombre y
tumba el contexto con `BeanDefinitionOverrideException`. Por eso el método
en `KafkaAdminClientConfig` se llama `adminClient`.

---

## Variables de entorno

| Variable | Por defecto | Descripción |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093,localhost:9094` | Brokers del clúster a administrar |

---

## Cómo correrlo

```
./mvnw spring-boot:run
```

Queda en el puerto **8087**. Requiere el clúster de Kafka levantado (ver
`digitalfix-infra`); no necesita Kafka UI, pero es útil tenerlo abierto en
`http://localhost:8089` para comparar.

---

## Cómo probarlo

Con un token con rol `Admin` y los scopes correspondientes:

1. `GET /api/v1/kafka/cluster` → devuelve los 3 brokers y cuál es el controlador.
2. `GET /api/v1/kafka/topics` → incluye `digitalfix.workorders.events`,
   `digitalfix.audit.events` y sus `.DLT`, cada uno con 3 particiones y
   factor de replicación 3.
3. `POST /api/v1/kafka/topics` con un nombre nuevo (p. ej. `prueba.kafka-admin`)
   y `particiones: 2, factorReplicacion: 3` → 201 con el tópico creado.
4. `PUT /api/v1/kafka/topics/prueba.kafka-admin/partitions` con
   `particiones: 4` → 200; una segunda llamada con `particiones: 3` (menor)
   → 400.
5. `PUT /api/v1/kafka/topics/prueba.kafka-admin/config` con
   `configuraciones: {"retention.ms": "7200000"}` → 200; se refleja en el
   siguiente `GET` del tópico.
6. `GET /api/v1/kafka/consumer-groups` → lista `audit-timeline`,
   `report-metrics` y `report-audit-trail` (si `audit` y `report` están
   corriendo y procesaron al menos un mensaje), todos `Stable`.
7. `GET /api/v1/kafka/consumer-groups/audit-timeline` → miembros con sus
   particiones asignadas y, por partición, `offsetActual`, `offsetFinal` y
   `lag` (0 si el consumidor está al día).
8. `DELETE /api/v1/kafka/topics/prueba.kafka-admin` → 200.
9. `DELETE /api/v1/kafka/topics/digitalfix.workorders.events` → 409 (protegido).

---

## Tests

`KafkaAdminServiceTest` usa `MockAdminClient` (de `kafka-clients`, clasificador
`test`, no Mockito) en vez de mockear el cliente a mano: es la forma que da
Kafka de simular un `Admin` real sin un broker, así que los contratos de
las operaciones (crear, listar, configurar, borrar, consumer groups,
clúster) se ejercitan contra un comportamiento fiel al real. Una excepción:
`MockAdminClient.createPartitions()` lanza
`UnsupportedOperationException: Not implemented yet` (limitación del mock,
no del servicio), así que el camino feliz de `ampliarParticiones` se
verificó manualmente contra el clúster real en Docker en vez de con un test
unitario.

```
./mvnw clean test
```

---

## Pendiente

- Despliegue en AWS: variables de entorno apuntando al broker por IP
  privada, igual que el resto de los microservicios con Kafka.
- Repositorio Git propio (hoy el módulo no tiene `.git` inicializado).
