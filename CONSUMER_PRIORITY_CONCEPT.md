# Consumer Priority 개념 및 구현 가이드

## 목차
1. [Consumer Priority란 무엇인가?](#consumer-priority란-무엇인가)
2. [왜 필요한가?](#왜-필요한가)
3. [메시징 시스템별 지원 현황](#메시징-시스템별-지원-현황)
4. [RabbitMQ Consumer Priority 상세](#rabbitmq-consumer-priority-상세)
5. [Pulsar Priority Level 상세](#pulsar-priority-level-상세)
6. [Kafka의 대안](#kafka의-대안)
7. [실전 예제 및 테스트](#실전-예제-및-테스트)

---

## Consumer Priority란 무엇인가?

### 기본 개념

**Consumer Priority**는 메시지 브로커에서 여러 컨슈머가 동일한 큐/토픽을 구독할 때, 어떤 컨슈머가 먼저 메시지를 받을지 결정하는 메커니즘입니다.

```
                    ┌─────────────┐
                    │   Message   │
                    │    Queue    │
                    └──────┬──────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
          ▼                ▼                ▼
    ┌─────────┐      ┌─────────┐      ┌─────────┐
    │Consumer │      │Consumer │      │Consumer │
    │Priority │      │Priority │      │Priority │
    │   10    │      │    5    │      │    1    │
    └─────────┘      └─────────┘      └─────────┘
         ↑                ↑                ↑
         │                │                │
    가장 먼저       두 번째로         마지막으로
    메시지 수신     메시지 수신      메시지 수신
```

### 동작 원리

1. **우선순위 설정**: 각 컨슈머에게 우선순위 값 할당 (숫자가 클수록 높은 우선순위)
2. **메시지 분배**: 브로커는 메시지를 분배할 때 우선순위가 높은 컨슈머에게 먼저 전달
3. **공정성 보장**: 높은 우선순위 컨슈머가 바쁠 때는 낮은 우선순위 컨슈머도 메시지 수신

---

## 왜 필요한가?

### 문제 시나리오

여러 서버에서 각각 여러 컨슈머를 실행할 때 발생하는 문제:

```yaml
# 설정
Server 1: concurrency = 2  (2개의 컨슈머 스레드)
Server 2: concurrency = 2  (2개의 컨슈머 스레드)
```

#### ❌ 문제: 불균등한 분배

```
Message Queue: [M1, M2, M3, M4, M5, M6, M7, M8]

Server 1          Server 2
┌─────────┐      ┌─────────┐
│Consumer1│      │Consumer3│
│  (M1)   │      │  (idle) │
└─────────┘      └─────────┘
┌─────────┐      ┌─────────┐
│Consumer2│      │Consumer4│
│  (M2)   │      │  (idle) │
└─────────┘      └─────────┘

→ 라운드로빈으로 분배되지만,
  Server 1의 두 컨슈머가 모두 바쁘면
  M3, M4도 Server 1로 가게 됨
```

**결과**:
- Server 1: CPU 200% 사용 (과부하)
- Server 2: CPU 0% 사용 (유휴)

#### ✅ 해결: Consumer Priority 사용

```yaml
# Server 1 설정
spring.cloud.stream.rabbit.bindings.input.consumer.consumer-priority: 10

# Server 2 설정
spring.cloud.stream.rabbit.bindings.input.consumer.consumer-priority: 5
```

```
Message Queue: [M1, M2, M3, M4, M5, M6, M7, M8]

Server 1 (Priority 10)     Server 2 (Priority 5)
┌─────────┐                ┌─────────┐
│Consumer1│ ← M1          │Consumer3│ ← M3
│         │                │         │  (Server1이 바쁠 때)
└─────────┘                └─────────┘
┌─────────┐                ┌─────────┐
│Consumer2│ ← M2          │Consumer4│ ← M4
│         │                │         │  (Server1이 바쁠 때)
└─────────┘                └─────────┘

→ Server 1의 컨슈머들이 먼저 받지만,
  바쁘면 Server 2가 처리
```

**결과**:
- 더 균등한 부하 분산
- 리소스 효율 향상

---

## 메시징 시스템별 지원 현황

| 메시징 시스템 | 네이티브 지원 | 범위 | 설정 방법 |
|-------------|-------------|------|----------|
| **RabbitMQ** | ✅ 지원 | 0-255 | Queue에 `x-max-priority` 설정 + Consumer에 `x-priority` 설정 |
| **Pulsar** | ✅ 지원 | 0-10 | Consumer 생성 시 `priorityLevel` 설정 |
| **Kafka** | ❌ 미지원 | - | Partition Assignment Strategy 또는 Instance Indexing 사용 |

---

## RabbitMQ Consumer Priority 상세

### 1. 기본 개념

RabbitMQ의 Consumer Priority는 **Queue 레벨**과 **Consumer 레벨** 두 단계로 설정됩니다.

#### Queue 설정: x-max-priority

```java
// Queue 선언 시 최대 우선순위 설정
Map<String, Object> args = new HashMap<>();
args.put("x-max-priority", 10);  // 최대 우선순위 10

Queue queue = new Queue("my-queue", true, false, false, args);
```

이 설정은 **Queue가 처리할 수 있는 최대 우선순위**를 정의합니다.

#### Consumer 설정: x-priority

```java
// Consumer 연결 시 우선순위 설정
Map<String, Object> consumerArgs = new HashMap<>();
consumerArgs.put("x-priority", 5);  // 이 컨슈머의 우선순위는 5

listenerContainer.setConsumerArguments(consumerArgs);
```

### 2. 동작 방식

```
┌─────────────────────────────────────────────────┐
│ Queue (x-max-priority: 10)                      │
│                                                 │
│ Messages: [M1, M2, M3, M4, M5, M6]             │
└─────────────┬───────────────────────────────────┘
              │
              │ 브로커가 메시지 분배 시 우선순위 고려
              │
    ┌─────────┴─────────┬─────────────────┐
    │                   │                 │
    ▼                   ▼                 ▼
┌─────────┐       ┌─────────┐       ┌─────────┐
│Consumer │       │Consumer │       │Consumer │
│x-priority│       │x-priority│       │x-priority│
│   10    │       │    5     │       │    1     │
└─────────┘       └─────────┘       └─────────┘
    ↑                   ↑                 ↑
    │                   │                 │
 M1, M2 먼저        M3, M4            M5, M6 마지막
```

### 3. Spring Cloud Stream 구현

#### RabbitConsumerProperties.java

```java
public class RabbitConsumerProperties extends RabbitCommonProperties {

    /**
     * Consumer priority for this consumer. Higher values indicate higher priority.
     * Requires the queue to be declared with x-max-priority argument.
     * Valid range: 0-255. Default: -1 (no priority set).
     */
    private int consumerPriority = -1;

    /**
     * Maximum priority for the queue. When set, the queue will be declared with
     * x-max-priority argument. Valid range: 1-255. Default: -1 (not set).
     */
    private int queueMaxPriority = -1;

    public int getConsumerPriority() {
        return this.consumerPriority;
    }

    public void setConsumerPriority(int consumerPriority) {
        this.consumerPriority = consumerPriority;
    }

    public int getQueueMaxPriority() {
        return this.queueMaxPriority;
    }

    public void setQueueMaxPriority(int queueMaxPriority) {
        this.queueMaxPriority = queueMaxPriority;
    }
}
```

#### RabbitExchangeQueueProvisioner.java - Queue 설정

```java
private void additionalArgs(Map<String, Object> args,
                            RabbitCommonProperties properties,
                            boolean isDlq) {
    // ... 기존 코드 ...

    Integer maxPriority = isDlq ? properties.getDlqMaxPriority()
            : properties.getMaxPriority();

    // Consumer Priority를 위한 Queue Max Priority 설정
    if (!isDlq && properties instanceof RabbitConsumerProperties) {
        RabbitConsumerProperties consumerProps = (RabbitConsumerProperties) properties;
        if (consumerProps.getQueueMaxPriority() > 0) {
            maxPriority = consumerProps.getQueueMaxPriority();
        }
    }

    // x-max-priority 인자 설정
    if (maxPriority != null) {
        args.put("x-max-priority", maxPriority);
    }

    // ... 기존 코드 ...
}
```

**설명**:
1. `RabbitConsumerProperties`에서 `queueMaxPriority` 값 가져오기
2. 값이 0보다 크면 `maxPriority`로 설정
3. Queue 인자에 `x-max-priority` 추가

#### RabbitMessageChannelBinder.java - Consumer 설정

```java
private ObservableListenerContainer createAndConfigureContainer(
        ConsumerDestination consumerDestination,
        String group,
        ExtendedConsumerProperties<RabbitConsumerProperties> properties,
        String destination,
        RabbitConsumerProperties extension) {

    // ... ListenerContainer 생성 코드 ...

    // Consumer Priority 설정
    if (extension.getConsumerPriority() >= 0) {
        Map<String, Object> consumerArgs = new HashMap<>();
        consumerArgs.put("x-priority", extension.getConsumerPriority());
        listenerContainer.setConsumerArguments(consumerArgs);
    }

    // ... 나머지 코드 ...

    return listenerContainer;
}
```

**설명**:
1. `consumerPriority`가 0 이상인지 확인
2. Consumer 인자 Map 생성
3. `x-priority` 키로 우선순위 값 설정
4. ListenerContainer에 인자 적용

### 4. 사용 예제

#### application.yml

```yaml
spring:
  cloud:
    stream:
      rabbit:
        bindings:
          processOrder-in-0:
            consumer:
              # Queue에 최대 우선순위 10 설정
              queue-max-priority: 10
              # 이 컨슈머의 우선순위는 8
              consumer-priority: 8
      bindings:
        processOrder-in-0:
          destination: orders
          group: order-processors
          consumer:
            concurrency: 2
```

#### Java 코드

```java
@Configuration
public class OrderProcessorConfig {

    @Bean
    public Consumer<Order> processOrder() {
        return order -> {
            log.info("Processing order: {}", order.getId());
            // 주문 처리 로직
            processOrderLogic(order);
        };
    }
}
```

#### 서버별 설정 예제

**Server 1 (고성능 서버) - application-server1.yml**
```yaml
spring:
  profiles: server1
  cloud:
    stream:
      rabbit:
        bindings:
          processOrder-in-0:
            consumer:
              consumer-priority: 10  # 최고 우선순위
              queue-max-priority: 10
      bindings:
        processOrder-in-0:
          consumer:
            concurrency: 4  # 4개 스레드
```

**Server 2 (백업 서버) - application-server2.yml**
```yaml
spring:
  profiles: server2
  cloud:
    stream:
      rabbit:
        bindings:
          processOrder-in-0:
            consumer:
              consumer-priority: 5   # 중간 우선순위
              queue-max-priority: 10
      bindings:
        processOrder-in-0:
          consumer:
            concurrency: 2  # 2개 스레드
```

### 5. 테스트 코드

```java
@Test
void testConsumerPriority() throws Exception {
    RabbitTestBinder binder = getBinder();

    // Consumer Properties 생성
    ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties
        = createConsumerProperties();

    // Priority 설정
    consumerProperties.getExtension().setConsumerPriority(10);
    consumerProperties.getExtension().setQueueMaxPriority(10);

    DirectChannel moduleInputChannel = createBindableChannel("input",
        new BindingProperties());

    // Consumer 바인딩
    Binding<MessageChannel> consumerBinding = binder.bindConsumer(
        "priority.test",
        "priorityGroup",
        moduleInputChannel,
        consumerProperties
    );

    // ListenerContainer 가져오기
    AbstractMessageListenerContainer container =
        TestUtils.getPropertyValue(consumerBinding,
            "lifecycle.messageListenerContainer",
            AbstractMessageListenerContainer.class);

    // Consumer Arguments 검증
    Map<String, Object> consumerArgs = container.getConsumerArguments();

    assertThat(consumerArgs).isNotNull();
    assertThat(consumerArgs.get("x-priority")).isEqualTo(10);
    assertThat(consumerProperties.getExtension().getConsumerPriority())
        .isEqualTo(10);
    assertThat(consumerProperties.getExtension().getQueueMaxPriority())
        .isEqualTo(10);

    consumerBinding.unbind();
}
```

### 6. RabbitMQ Management UI에서 확인

```bash
# RabbitMQ Management UI 접속
http://localhost:15672

# Queue 정보 확인
→ Queues 탭
→ 해당 Queue 클릭
→ Arguments 섹션에서 x-max-priority: 10 확인

# Consumer 정보 확인
→ Queue 상세 페이지
→ Consumers 섹션
→ Consumer arguments에서 x-priority 값 확인
```

---

## Pulsar Priority Level 상세

### 1. 기본 개념

Pulsar는 Consumer를 생성할 때 `priorityLevel`을 설정하여 우선순위를 지정합니다.

```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("my-topic")
    .subscriptionName("my-subscription")
    .priorityLevel(5)  // 우선순위 레벨 설정
    .subscribe();
```

### 2. 동작 방식

```
┌─────────────────────────────────────────────────┐
│ Pulsar Topic: persistent://tenant/ns/my-topic  │
│                                                 │
│ Messages: [M1, M2, M3, M4, M5, M6]             │
└─────────────┬───────────────────────────────────┘
              │
              │ Pulsar가 우선순위에 따라 분배
              │
    ┌─────────┴─────────┬─────────────────┐
    │                   │                 │
    ▼                   ▼                 ▼
┌─────────┐       ┌─────────┐       ┌─────────┐
│Consumer │       │Consumer │       │Consumer │
│Priority │       │Priority │       │Priority │
│  Level  │       │  Level  │       │  Level  │
│   10    │       │    5    │       │    1    │
└─────────┘       └─────────┘       └─────────┘
```

**특징**:
- 범위: 0-10 (RabbitMQ보다 좁음)
- 설정이 간단함 (Queue 레벨 설정 불필요)
- Consumer 생성 시점에만 설정

### 3. Spring Cloud Stream 구현

#### ConsumerConfigProperties.java

Pulsar의 경우, Spring Boot의 `PulsarProperties.Consumer`를 상속받으므로 이미 `priorityLevel` 메서드가 존재합니다:

```java
public class ConsumerConfigProperties extends PulsarProperties.Consumer {

    // priorityLevel은 parent class에 이미 정의됨
    // public int getPriorityLevel()
    // public void setPriorityLevel(int priorityLevel)

    /**
     * Gets a map representation of the base consumer properties
     */
    public Map<String, Object> toBaseConsumerPropertiesMap() {
        var consumerProps = new ConsumerConfigProperties.Properties();
        var map = PropertyMapper.get();

        // ... 다른 속성들 ...

        // priorityLevel 자동 매핑
        map.from(this::getPriorityLevel)
           .to(consumerProps.in("priorityLevel"));

        // ... 다른 속성들 ...

        return consumerProps.toMap();
    }
}
```

**설명**:
1. `PulsarProperties.Consumer`에서 `getPriorityLevel()` 상속
2. `toBaseConsumerPropertiesMap()`에서 자동으로 Pulsar Consumer 설정에 매핑
3. 추가 구현 불필요

### 4. 사용 예제

#### application.yml

```yaml
spring:
  cloud:
    stream:
      pulsar:
        bindings:
          processOrder-in-0:
            consumer:
              # Priority Level 설정 (0-10)
              priority-level: 8
              # 기타 Pulsar 설정
              subscription-type: shared
      bindings:
        processOrder-in-0:
          destination: persistent://public/default/orders
          group: order-processors
```

#### Java 코드

```java
@Configuration
public class PulsarOrderProcessorConfig {

    @Bean
    public Consumer<Order> processOrder() {
        return order -> {
            log.info("Processing order with Pulsar: {}", order.getId());
            processOrderLogic(order);
        };
    }
}
```

#### 서버별 설정

**Server 1 - application-server1.yml**
```yaml
spring:
  profiles: server1
  cloud:
    stream:
      pulsar:
        bindings:
          processOrder-in-0:
            consumer:
              priority-level: 10  # 최고 우선순위
              subscription-type: shared
```

**Server 2 - application-server2.yml**
```yaml
spring:
  profiles: server2
  cloud:
    stream:
      pulsar:
        bindings:
          processOrder-in-0:
            consumer:
              priority-level: 5   # 중간 우선순위
              subscription-type: shared
```

### 5. Pulsar CLI로 확인

```bash
# Consumer 정보 확인
bin/pulsar-admin topics stats persistent://public/default/orders

# 출력 예시:
{
  "subscriptions": {
    "order-processors": {
      "consumers": [
        {
          "consumerName": "server1-consumer",
          "priorityLevel": 10,
          "availablePermits": 1000,
          "msgRateOut": 100.5
        },
        {
          "consumerName": "server2-consumer",
          "priorityLevel": 5,
          "availablePermits": 1000,
          "msgRateOut": 50.2
        }
      ]
    }
  }
}
```

---

## Kafka의 대안

Kafka는 네이티브로 Consumer Priority를 지원하지 않습니다. 하지만 다음 대안들을 사용할 수 있습니다.

### 1. Instance Index 사용

```yaml
# Server 1
spring:
  cloud:
    stream:
      instance-index: 0
      instance-count: 2
      bindings:
        processOrder-in-0:
          destination: orders
          group: order-processors
          consumer:
            concurrency: 1

# Server 2
spring:
  cloud:
    stream:
      instance-index: 1
      instance-count: 2
      bindings:
        processOrder-in-0:
          destination: orders
          group: order-processors
          consumer:
            concurrency: 1
```

**동작 방식**:
- Kafka는 파티션을 instance-index에 따라 할당
- Server 1: Partition 0 처리
- Server 2: Partition 1 처리
- 명확한 파티션 분리로 균등 분배

### 2. Multiple Bindings with Concurrency=1

```yaml
spring:
  cloud:
    stream:
      bindings:
        # 첫 번째 바인딩
        processOrder1-in-0:
          destination: orders
          group: order-processors-1
          consumer:
            concurrency: 1
        # 두 번째 바인딩
        processOrder2-in-0:
          destination: orders
          group: order-processors-2
          consumer:
            concurrency: 1
```

```java
@Configuration
public class KafkaMultipleBindingsConfig {

    @Bean
    public Consumer<Order> processOrder1() {
        return order -> {
            log.info("Binding 1 processing: {}", order.getId());
            processOrderLogic(order);
        };
    }

    @Bean
    public Consumer<Order> processOrder2() {
        return order -> {
            log.info("Binding 2 processing: {}", order.getId());
            processOrderLogic(order);
        };
    }
}
```

### 3. Custom Partition Assignment Strategy

```java
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processors");

        // Custom Partition Assignment Strategy
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                  RoundRobinAssignor.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }
}
```

---

## 실전 예제 및 테스트

### 시나리오: 주문 처리 시스템

**요구사항**:
- 2개 서버에서 주문 처리
- Server 1: 고성능 (우선 처리)
- Server 2: 백업 (Server 1이 바쁠 때만)

### RabbitMQ 구현

#### 1. 공통 설정 (application.yml)

```yaml
spring:
  cloud:
    stream:
      function:
        definition: processOrder
      bindings:
        processOrder-in-0:
          destination: orders
          group: order-processors
          consumer:
            max-attempts: 3
      rabbit:
        bindings:
          processOrder-in-0:
            consumer:
              # Queue에 max priority 설정
              queue-max-priority: 10
              # DLQ 설정
              auto-bind-dlq: true
              republish-to-dlq: true
```

#### 2. Server 1 설정 (application-server1.yml)

```yaml
spring:
  config:
    activate:
      on-profile: server1
  cloud:
    stream:
      rabbit:
        bindings:
          processOrder-in-0:
            consumer:
              consumer-priority: 10  # 최고 우선순위
      bindings:
        processOrder-in-0:
          consumer:
            concurrency: 4  # 4개 스레드
```

#### 3. Server 2 설정 (application-server2.yml)

```yaml
spring:
  config:
    activate:
      on-profile: server2
  cloud:
    stream:
      rabbit:
        bindings:
          processOrder-in-0:
            consumer:
              consumer-priority: 5   # 낮은 우선순위
      bindings:
        processOrder-in-0:
          consumer:
            concurrency: 2  # 2개 스레드
```

#### 4. 비즈니스 로직

```java
@Slf4j
@Component
public class OrderProcessor {

    @Autowired
    private OrderService orderService;

    @Autowired
    private MetricsService metricsService;

    @Bean
    public Consumer<Message<Order>> processOrder() {
        return message -> {
            Order order = message.getPayload();
            String server = System.getenv("SERVER_ID");

            log.info("Server {} processing order: {}", server, order.getId());

            try {
                // 메트릭 기록
                metricsService.recordProcessingStart(server, order.getId());

                // 주문 처리
                orderService.process(order);

                // 성공 메트릭
                metricsService.recordProcessingSuccess(server, order.getId());

            } catch (Exception e) {
                log.error("Failed to process order: {}", order.getId(), e);
                metricsService.recordProcessingFailure(server, order.getId());
                throw new RuntimeException("Order processing failed", e);
            }
        };
    }
}
```

#### 5. 메트릭 모니터링

```java
@Service
public class MetricsService {

    private final MeterRegistry meterRegistry;

    public MetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void recordProcessingStart(String server, String orderId) {
        Counter.builder("order.processing.started")
                .tag("server", server)
                .register(meterRegistry)
                .increment();
    }

    public void recordProcessingSuccess(String server, String orderId) {
        Counter.builder("order.processing.success")
                .tag("server", server)
                .register(meterRegistry)
                .increment();
    }

    public void recordProcessingFailure(String server, String orderId) {
        Counter.builder("order.processing.failure")
                .tag("server", server)
                .register(meterRegistry)
                .increment();
    }
}
```

#### 6. 통합 테스트

```java
@SpringBootTest
@TestPropertySource(properties = {
    "spring.cloud.stream.rabbit.bindings.processOrder-in-0.consumer.consumer-priority=10",
    "spring.cloud.stream.rabbit.bindings.processOrder-in-0.consumer.queue-max-priority=10"
})
class OrderProcessorIntegrationTest {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MetricsService metricsService;

    @Test
    void testOrderProcessingWithPriority() throws InterruptedException {
        // 테스트 주문 생성
        List<Order> orders = IntStream.range(0, 100)
            .mapToObj(i -> new Order("ORDER-" + i, "Product-" + i, 1))
            .collect(Collectors.toList());

        // 주문 발송
        orders.forEach(order ->
            rabbitTemplate.convertAndSend("orders", order)
        );

        // 처리 대기
        Thread.sleep(10000);

        // 메트릭 검증
        double server1Count = metricsService.getProcessingCount("server1");
        double server2Count = metricsService.getProcessingCount("server2");

        // Server 1이 더 많이 처리했는지 확인
        assertThat(server1Count).isGreaterThan(server2Count);

        // 모든 주문이 처리되었는지 확인
        assertThat(server1Count + server2Count).isEqualTo(100);
    }
}
```

### 부하 테스트

```java
@Component
public class LoadTestRunner {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void runLoadTest(int messageCount, int delayMillis) {
        ExecutorService executor = Executors.newFixedThreadPool(10);

        IntStream.range(0, messageCount).forEach(i -> {
            executor.submit(() -> {
                try {
                    Order order = generateRandomOrder();
                    rabbitTemplate.convertAndSend("orders", order);

                    if (delayMillis > 0) {
                        Thread.sleep(delayMillis);
                    }
                } catch (Exception e) {
                    log.error("Failed to send message", e);
                }
            });
        });

        executor.shutdown();
    }

    private Order generateRandomOrder() {
        return new Order(
            "ORDER-" + UUID.randomUUID(),
            "Product-" + ThreadLocalRandom.current().nextInt(1000),
            ThreadLocalRandom.current().nextInt(1, 10)
        );
    }
}
```

### 실행 및 모니터링

```bash
# Server 1 실행
java -jar order-processor.jar --spring.profiles.active=server1 --SERVER_ID=server1

# Server 2 실행
java -jar order-processor.jar --spring.profiles.active=server2 --SERVER_ID=server2

# 부하 테스트 실행
curl -X POST http://localhost:8080/load-test?count=1000&delay=10

# Prometheus 메트릭 확인
curl http://localhost:8080/actuator/prometheus | grep order_processing

# 출력 예시:
# order_processing_started{server="server1"} 680.0
# order_processing_started{server="server2"} 320.0
# order_processing_success{server="server1"} 678.0
# order_processing_success{server="server2"} 318.0
```

---

## 결론

### Consumer Priority 사용 시점

✅ **사용하면 좋은 경우**:
- 여러 서버에서 동일한 큐/토픽 구독
- 서버 간 성능 차이가 있음
- 특정 서버를 우선 사용하고 싶음
- 백업/페일오버 구조 필요

❌ **사용이 불필요한 경우**:
- 단일 서버 운영
- 모든 서버 성능이 동일
- 파티셔닝으로 충분히 분산 가능
- 메시지 순서 보장이 중요

### 메시징 시스템 선택 가이드

| 요구사항 | 추천 시스템 | 이유 |
|---------|-----------|-----|
| Consumer Priority 필수 | RabbitMQ, Pulsar | 네이티브 지원 |
| 높은 처리량 | Kafka, Pulsar | 파티셔닝 최적화 |
| 복잡한 라우팅 | RabbitMQ | Exchange 타입 다양 |
| 멀티 테넌시 | Pulsar | 네임스페이스 지원 |
| 간단한 운영 | RabbitMQ | 관리 UI 우수 |

### 참고 자료

- [RabbitMQ Consumer Priority](https://www.rabbitmq.com/consumer-priority.html)
- [Apache Pulsar Consumer API](https://pulsar.apache.org/docs/en/client-libraries-java/)
- [Spring Cloud Stream Reference](https://docs.spring.io/spring-cloud-stream/docs/current/reference/html/)
- [Kafka Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)
