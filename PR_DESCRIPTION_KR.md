# Consumer Priority 구현 - PR 설명 (한국어)

## 📋 요약

GitHub 이슈 #3122 해결: 여러 서버에서 각각 여러 컨슈머를 실행할 때 메시지가 균등하게 분산되지 않는 문제를 Consumer Priority 기능으로 해결했습니다.

> **Note**: 이 PR은 코드 변경사항만 포함합니다. 상세 문서는 별도로 제공됩니다.

## 🎯 문제 정의

### 기존 문제
```
시나리오: 2개 서버 × concurrency 2 = 4개 컨슈머
문제점: 메시지가 한 서버에 집중되어 CPU 200% 사용, 다른 서버는 유휴 상태
```

**근본 원인**: 메시징 시스템이 서버가 아닌 개별 컨슈머 단위로 라운드로빈 분배하므로, 계산 집약적 메시지 처리 시 특정 서버에 부하 집중

## 💡 해결 방안

**Consumer Priority** 메커니즘 구현:
- 각 컨슈머에 우선순위 설정
- 높은 우선순위 컨슈머가 먼저 메시지 수신
- 바쁠 때는 낮은 우선순위 컨슈머가 처리

## 🔧 구현 내용

### 1. RabbitMQ Binder (완전 구현)

#### 새로운 속성
```java
// RabbitConsumerProperties.java
private int consumerPriority = -1;  // Consumer 우선순위 (0-255)
private int queueMaxPriority = -1;  // Queue 최대 우선순위 (1-255)
```

#### Queue 설정
```java
// RabbitExchangeQueueProvisioner.java:613-618
if (!isDlq && properties instanceof RabbitConsumerProperties) {
    RabbitConsumerProperties consumerProps = (RabbitConsumerProperties) properties;
    if (consumerProps.getQueueMaxPriority() > 0) {
        maxPriority = consumerProps.getQueueMaxPriority();
    }
}
// x-max-priority 인자를 Queue에 설정
```

#### Consumer 설정
```java
// RabbitMessageChannelBinder.java:606-611
if (extension.getConsumerPriority() >= 0) {
    Map<String, Object> consumerArgs = new HashMap<>();
    consumerArgs.put("x-priority", extension.getConsumerPriority());
    listenerContainer.setConsumerArguments(consumerArgs);
}
```

#### 사용 예제
```yaml
spring:
  cloud:
    stream:
      rabbit:
        bindings:
          input-in-0:
            consumer:
              consumer-priority: 10      # Consumer 우선순위
              queue-max-priority: 10     # Queue 최대 우선순위
```

### 2. Pulsar Binder (네이티브 지원)

Pulsar는 이미 parent class `PulsarProperties.Consumer`에서 `priorityLevel`을 지원하며, `toBaseConsumerPropertiesMap()`에서 자동 매핑됩니다.

```yaml
spring:
  cloud:
    stream:
      pulsar:
        bindings:
          input-in-0:
            consumer:
              priority-level: 5  # 0-10 범위
```

### 3. Kafka Binder (일관성 유지)

Kafka는 Consumer Priority를 네이티브로 지원하지 않으나, API 일관성을 위해 속성을 추가하고 Javadoc에 미지원 명시:

```java
// KafkaConsumerProperties.java:213-221
/**
 * Consumer priority level. NOTE: Kafka does not natively support consumer priority.
 * This property is provided for consistency across binders but has no effect in Kafka.
 * For even message distribution across servers, use partition assignment strategies
 * or create separate bindings with concurrency=1.
 * Default: -1 (not supported)
 */
private int consumerPriority = -1;
```

## 📝 변경된 파일

### Core
- `ConsumerProperties.java` - 범용 속성 제거 (binder별 구현으로 통일)

### RabbitMQ Binder
1. `RabbitConsumerProperties.java` (148-159, 341-355)
   - `consumerPriority`, `queueMaxPriority` 속성 추가
2. `RabbitExchangeQueueProvisioner.java` (613-618)
   - Queue 선언 시 `x-max-priority` 설정
3. `RabbitMessageChannelBinder.java` (24, 606-611)
   - Consumer 연결 시 `x-priority` 설정
4. `RabbitBinderTests.java` (2737-2761)
   - Consumer Priority 테스트 추가

### Kafka Binder
- `KafkaConsumerProperties.java` (213-221, 499-505)
  - API 일관성을 위한 속성 추가 (동작하지 않음을 명시)

## ✅ 테스트

### 단위 테스트
```java
@Test
void testConsumerPriority() throws Exception {
    consumerProperties.getExtension().setConsumerPriority(10);
    consumerProperties.getExtension().setQueueMaxPriority(10);

    Binding<MessageChannel> consumerBinding = binder.bindConsumer(
        "priority.test", "priorityGroup", moduleInputChannel, consumerProperties);

    Map<String, Object> consumerArgs = container.getConsumerArguments();
    assertThat(consumerArgs.get("x-priority")).isEqualTo(10);
}
```

**결과**: ✅ 모든 테스트 통과

## 🎓 사용 시나리오

### Before (문제 상황)
```
Server 1: Consumer1(바쁨), Consumer2(바쁨) → CPU 200%
Server 2: Consumer3(대기), Consumer4(대기) → CPU 0%
```

### After (Consumer Priority 적용)
```yaml
# Server 1
spring.cloud.stream.rabbit.bindings.input.consumer.consumer-priority: 10

# Server 2
spring.cloud.stream.rabbit.bindings.input.consumer.consumer-priority: 5
```

```
Server 1: Consumer1(우선), Consumer2(우선) → CPU 100%
Server 2: Consumer3(백업), Consumer4(백업) → CPU 100% (Server 1이 바쁠 때)
→ 균등한 부하 분산 ✅
```

## 🔍 이슈 #3122 해결 방법

이슈에서 제안한 3가지 방법 중:
1. ✅ **"consumer priority to thread index"** - RabbitMQ와 Pulsar에서 구현
2. ⚠️ "Dynamically create consumers" - 별도 구현 필요
3. ✅ **"Multiple bindings with concurrency=1"** - Kafka 대안으로 문서에 명시

## 📚 참고 자료

- RabbitMQ Consumer Priority: https://www.rabbitmq.com/consumer-priority.html
- Apache Pulsar Priority Level: https://pulsar.apache.org/docs/en/client-libraries-java/
- 관련 이슈: spring-amqp#3092

## ⚠️ Breaking Changes

없음. 모든 변경사항은 하위 호환성을 유지합니다.

## 📖 사용 문서

Consumer Priority 사용 방법에 대한 상세 문서는 다음을 참고하세요:
- RabbitMQ Consumer Priority: https://www.rabbitmq.com/consumer-priority.html
- Apache Pulsar Priority Level: https://pulsar.apache.org/docs/en/client-libraries-java/

## 🚀 다음 단계

PR 병합 후 고려사항:
1. 공식 reference 문서 업데이트 필요
2. Kafka 대안 (partition assignment strategy) 예제 추가 고려
3. 추가 통합 테스트 (부하 테스트) 고려

---

**Fixes #3122**
