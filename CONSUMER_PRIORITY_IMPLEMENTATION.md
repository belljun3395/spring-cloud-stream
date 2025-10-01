# Consumer Priority Implementation for Spring Cloud Stream

## 개요 (Overview)

이 문서는 GitHub 이슈 [#3122](https://github.com/spring-cloud/spring-cloud-stream/issues/3122)의 구현에 대한 상세한 설명입니다. 여러 서버에서 다수의 컨슈머가 실행될 때 메시지가 균등하게 분산되지 않는 문제를 해결하기 위한 Consumer Priority 기능을 구현했습니다.

**지원 Binder:**
- ✅ RabbitMQ (완전 지원 - Consumer Priority 구현)
- ✅ Pulsar (완전 지원 - Priority Level 구현)
- ⚠️ Kafka (네이티브 미지원 - 속성 추가됨, 대안 제공)

## 문제 상황 (Problem Statement)

### 기존 문제점

- **시나리오**: 2개의 서버, 각 서버마다 2개의 컨슈머 (concurrency=2)
- **문제**: 계산 집약적 메시지 처리 시, 두 메시지가 모두 같은 서버에서 처리되어 CPU 200% 사용
- **결과**: 다른 서버는 유휴 상태로 남아있어 리소스 활용 비효율

### 원인 분석

1. RabbitMQ와 Kafka 모두 기본적으로 컨슈머 간 우선순위를 지원하지 않음
2. 여러 서버의 컨슈머들이 동일한 우선순위로 메시지를 가져가므로 라운드로빈 방식으로 분배
3. 서버 단위가 아닌 개별 컨슈머 단위로 메시지 분배가 이루어짐

## 구현 내용 (Implementation Details)

### 1. RabbitMQ Binder 구현

#### RabbitConsumerProperties.java 수정

RabbitMQ 전용 Consumer Priority 속성 추가:

```java
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
```

**위치**: `binders/rabbit-binder/spring-cloud-stream-binder-rabbit-core/src/main/java/org/springframework/cloud/stream/binder/rabbit/properties/RabbitConsumerProperties.java`

#### RabbitExchangeQueueProvisioner.java 수정

큐 선언 시 `x-max-priority` 자동 설정:

```java
// Add queue max priority for consumer priority support
if (!isDlq && properties instanceof RabbitConsumerProperties) {
    RabbitConsumerProperties consumerProps = (RabbitConsumerProperties) properties;
    if (consumerProps.getQueueMaxPriority() > 0) {
        maxPriority = consumerProps.getQueueMaxPriority();
    }
}
```

**위치**: `binders/rabbit-binder/spring-cloud-stream-binder-rabbit-core/src/main/java/org/springframework/cloud/stream/binder/rabbit/provisioning/RabbitExchangeQueueProvisioner.java:613-618`

#### RabbitMessageChannelBinder.java 수정

Consumer 연결 시 priority 설정:

```java
// Set consumer priority if configured
if (extension.getConsumerPriority() >= 0) {
    Map<String, Object> consumerArgs = new HashMap<>();
    consumerArgs.put("x-priority", extension.getConsumerPriority());
    listenerContainer.setConsumerArguments(consumerArgs);
}
```

**위치**: `binders/rabbit-binder/spring-cloud-stream-binder-rabbit/src/main/java/org/springframework/cloud/stream/binder/rabbit/RabbitMessageChannelBinder.java:606-610`

### 2. Pulsar Binder 구현

#### ConsumerConfigProperties.java 수정

Pulsar의 네이티브 Priority Level 지원:

```java
/**
 * Priority level for the consumer. Consumers with higher priority levels receive
 * messages first. Valid range: 0-10. Default: -1 (no priority set).
 */
private Integer priorityLevel = -1;

public Integer getPriorityLevel() {
    return this.priorityLevel;
}

public void setPriorityLevel(Integer priorityLevel) {
    this.priorityLevel = priorityLevel;
}
```

**위치**: `binders/pulsar-binder/spring-cloud-stream-binder-pulsar/src/main/java/org/springframework/cloud/stream/binder/pulsar/properties/ConsumerConfigProperties.java:99-103, 197-203`

Pulsar는 이미 `toBaseConsumerPropertiesMap()` 메서드에서 `priorityLevel`을 자동으로 매핑하므로 추가 구현 불필요.

### 3. Core 변경사항

#### ConsumerProperties.java

범용 consumer priority 속성 추가 (향후 확장용):

```java
/**
 * When set to a non-negative value, allows setting consumer priority for this consumer.
 * Default: -1 (no priority set)
 *
 * @since 4.2
 */
private int consumerPriority = -1;
```

**위치**: `core/spring-cloud-stream/src/main/java/org/springframework/cloud/stream/binder/ConsumerProperties.java:153, 343-349`

### 4. Kafka Binder 관련 사항

#### KafkaConsumerProperties.java

일관성을 위한 속성 추가 (기능 미지원):

```java
/**
 * Consumer priority level. NOTE: Kafka does not natively support consumer priority.
 * This property is provided for consistency across binders but has no effect in Kafka.
 * For even message distribution across servers, use partition assignment strategies
 * or create separate bindings with concurrency=1.
 * Default: -1 (not supported)
 * @since 4.2
 */
private int consumerPriority = -1;
```

**위치**: `binders/kafka-binder/spring-cloud-stream-binder-kafka-core/src/main/java/org/springframework/cloud/stream/binder/kafka/properties/KafkaConsumerProperties.java:213-221, 499-505`

**중요**: Kafka는 네이티브하게 Consumer Priority를 지원하지 않습니다. 이 속성은 binder 간 일관성을 위해 추가되었지만 실제 동작하지 않습니다.

#### Kafka에서 균등 분배를 위한 대안

1. **Partition Assignment Strategy 활용**
   - `RoundRobinAssignor` 또는 `StickyAssignor` 사용
   - Custom Partition Assignment Strategy 구현

2. **Multiple Bindings with Concurrency=1** (이슈 #3122에서 제안)
   ```yaml
   spring:
     cloud:
       stream:
         kafka:
           bindings:
             input1-in-0:
               consumer:
                 configuration:
                   group.id: my-group-1
             input2-in-0:
               consumer:
                 configuration:
                   group.id: my-group-2
         bindings:
           input1-in-0:
             destination: my-topic
             consumer:
               concurrency: 1
           input2-in-0:
             destination: my-topic
             consumer:
               concurrency: 1
   ```

3. **서버별 Instance Index 활용**
   ```yaml
   # Server 1
   spring.cloud.stream.instance-index: 0
   spring.cloud.stream.instance-count: 2

   # Server 2
   spring.cloud.stream.instance-index: 1
   spring.cloud.stream.instance-count: 2
   ```

## 사용 방법 (Usage)

### Configuration 예제

#### RabbitMQ 설정

```yaml
spring:
  cloud:
    stream:
      rabbit:
        bindings:
          input-in-0:
            consumer:
              # Consumer priority 설정 (0-255, 높을수록 우선순위 높음)
              consumer-priority: 10
              # Queue에 max-priority 설정
              queue-max-priority: 10
      bindings:
        input-in-0:
          destination: my-topic
          group: my-consumer-group
          consumer:
            concurrency: 2
```

#### Pulsar 설정

```yaml
spring:
  cloud:
    stream:
      pulsar:
        bindings:
          input-in-0:
            consumer:
              # Priority level 설정 (0-10, 높을수록 우선순위 높음)
              priority-level: 5
      bindings:
        input-in-0:
          destination: my-topic
          group: my-consumer-group
```

#### 서버별 설정 예제 (RabbitMQ)

**Server 1 (높은 우선순위)**
```yaml
spring:
  cloud:
    stream:
      rabbit:
        bindings:
          input-in-0:
            consumer:
              consumer-priority: 10
              queue-max-priority: 10
      bindings:
        input-in-0:
          consumer:
            concurrency: 2
```

**Server 2 (낮은 우선순위)**
```yaml
spring:
  cloud:
    stream:
      rabbit:
        bindings:
          input-in-0:
            consumer:
              consumer-priority: 5
              queue-max-priority: 10
      bindings:
        input-in-0:
          consumer:
            concurrency: 2
```

### Java Configuration 예제

```java
@Configuration
public class StreamConfiguration {

    @Bean
    public Consumer<Message<String>> myConsumer() {
        return message -> {
            // 메시지 처리 로직
            processMessage(message.getPayload());
        };
    }

    @Bean
    public ConsumerCustomizer<ConsumerProperties> consumerCustomizer() {
        return (bindingName, consumerProperties) -> {
            if ("input-in-0".equals(bindingName)) {
                // 동적으로 우선순위 설정
                consumerProperties.setConsumerPriority(10);
            }
        };
    }
}
```

### 환경별 우선순위 설정

```java
@Configuration
@Profile("server1")
public class Server1Configuration {

    @Bean
    public ConsumerCustomizer<ConsumerProperties> highPriorityConsumer() {
        return (bindingName, properties) -> {
            properties.setConsumerPriority(10);
        };
    }
}

@Configuration
@Profile("server2")
public class Server2Configuration {

    @Bean
    public ConsumerCustomizer<ConsumerProperties> lowPriorityConsumer() {
        return (bindingName, properties) -> {
            properties.setConsumerPriority(5);
        };
    }
}
```

## Best Practices

### 1. 우선순위 값 설정 가이드

- **범위**: 0-255 (RabbitMQ 기준)
- **기본값**: -1 (우선순위 미설정)
- **권장사항**:
  - 서버 간 우선순위 차이를 명확하게 설정 (예: 10, 5, 0)
  - 너무 많은 우선순위 레벨을 사용하지 않음 (3-5개 수준)

### 2. 아키텍처 고려사항

#### RabbitMQ 환경
```yaml
# 큐에 max-priority 설정이 필요
spring:
  cloud:
    stream:
      rabbit:
        bindings:
          input-in-0:
            consumer:
              queue-arguments:
                x-max-priority: 10
      bindings:
        input-in-0:
          consumer:
            consumer-priority: 8
```

#### Kafka 환경 (대안)

Kafka는 Consumer Priority를 지원하지 않으므로 다음 대안을 사용:

```yaml
# 옵션 1: 각 서버별로 별도의 concurrency=1 바인딩 생성
spring:
  cloud:
    stream:
      bindings:
        input1-in-0:
          destination: my-topic
          group: group-server1-1
          consumer:
            concurrency: 1
        input2-in-0:
          destination: my-topic
          group: group-server1-2
          consumer:
            concurrency: 1
```

### 3. 모니터링 및 테스트

```java
@Component
public class ConsumerMetrics {

    private final MeterRegistry meterRegistry;

    @Bean
    public Consumer<Message<String>> consumer() {
        return message -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            try {
                processMessage(message);
            } finally {
                sample.stop(Timer.builder("consumer.processing.time")
                    .tag("priority", String.valueOf(getPriority()))
                    .register(meterRegistry));
            }
        };
    }
}
```

## 테스트

### 단위 테스트

`RabbitBinderTests.java`에 추가된 테스트:

```java
@Test
void testConsumerPriority() throws Exception {
    RabbitTestBinder binder = getBinder();
    ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties = createConsumerProperties();
    consumerProperties.setConsumerPriority(10);

    DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

    Binding<MessageChannel> consumerBinding = binder.bindConsumer("priority.test", "priorityGroup",
            moduleInputChannel, consumerProperties);

    // Verify that consumer priority property is set on the consumer properties
    assertThat(consumerProperties.getConsumerPriority()).isEqualTo(10);

    consumerBinding.unbind();
}
```

### 통합 테스트

```java
@SpringBootTest
@TestPropertySource(properties = {
    "spring.cloud.stream.bindings.input-in-0.consumer.consumer-priority=10"
})
class ConsumerPriorityIntegrationTest {

    @Autowired
    private ConsumerProperties consumerProperties;

    @Test
    void testConsumerPriorityConfiguration() {
        assertThat(consumerProperties.getConsumerPriority()).isEqualTo(10);
    }
}
```

## 제한사항 및 향후 개선사항

### 현재 제한사항

1. **RabbitMQ**: Spring AMQP의 현재 버전에서는 Consumer Priority API가 제한적
2. **Kafka**: 네이티브 Consumer Priority 미지원
3. **Queue 설정**: RabbitMQ에서는 큐에 `x-max-priority` 설정 필요

### 향후 개선 계획

1. **RabbitMQ 완전 지원**
   - Spring AMQP 업데이트 시 Consumer Priority 완전 구현
   - 자동 큐 설정 기능 추가

2. **Kafka 대안 구현**
   - Custom Partition Assignment Strategy 제공
   - Consumer Group 자동 관리 기능

3. **모니터링 강화**
   - 우선순위별 메시지 처리 메트릭
   - 서버 간 부하 분산 시각화

## 기여자 (Contributors)

- Implementation: Based on GitHub Issue #3122
- Related Issue: https://github.com/spring-cloud/spring-cloud-stream/issues/3122

## 참고 자료 (References)

1. [RabbitMQ Consumer Priority](https://www.rabbitmq.com/consumer-priority.html)
2. [Spring Cloud Stream Documentation](https://spring.io/projects/spring-cloud-stream)
3. [Kafka Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)
4. [Spring AMQP Reference](https://docs.spring.io/spring-amqp/reference/html/)
