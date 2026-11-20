package dev.demo.payflow.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import dev.demo.payflow.events.PaymentInitiated;
import dev.demo.payflow.events.Topics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Import(GatewayIT.Containers.class)
@TestPropertySource(properties = { "payflow.topic.replicas=1", "payflow.topic.min-insync-replicas=1" })
class GatewayIT {

	@TestConfiguration(proxyBeanMethods = false)
	static class Containers {

		@Bean
		@ServiceConnection
		KafkaContainer kafka() {
			return new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.2"));
		}
	}

	@Autowired
	MockMvc mvc;

	@Autowired
	JsonMapper json;

	@Autowired
	KafkaContainer kafka;

	static final String BODY = """
			{"fromAccount":"ACC-000003","toAccount":"ACC-000006","amount":12.50,"currency":"GBP"}""";

	String postPayment(String key, String body, int expectedStatus) throws Exception {
		var req = post("/payments").contentType(MediaType.APPLICATION_JSON).content(body);
		if (key != null) {
			req.header("Idempotency-Key", key);
		}
		return mvc.perform(req).andExpect(status().is(expectedStatus)).andReturn().getResponse().getContentAsString();
	}

	List<ConsumerRecord<String, String>> readAll() {
		var props = Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
				ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				"earliest");
		try (var c = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
			c.subscribe(List.of(Topics.PAYMENTS_INITIATED));
			var out = new ArrayList<ConsumerRecord<String, String>>();
			long deadline = System.currentTimeMillis() + 10_000;
			while (System.currentTimeMillis() < deadline) {
				c.poll(Duration.ofMillis(500)).forEach(out::add);
				if (!out.isEmpty() && c.assignment().stream().allMatch(tp -> c.position(tp) >= c.endOffsets(List.of(tp)).get(tp))) {
					break;
				}
			}
			return out;
		}
	}

	@Test
	void acceptedPaymentIsOnTheTopicKeyedByFromAccount() throws Exception {
		var key = "gw-" + UUID.randomUUID();
		var resp = json.readTree(postPayment(key, BODY, 202));
		var paymentId = UUID.fromString(resp.get("paymentId").asString());
		assertThat(paymentId).isEqualTo(PaymentInitiated.eventIdFor(key));

		var rec = readAll().stream().filter(r -> r.value().contains(paymentId.toString())).findFirst().orElseThrow();
		assertThat(rec.key()).isEqualTo("ACC-000003");
		var event = json.readValue(rec.value(), PaymentInitiated.class);
		assertThat(event.idempotencyKey()).isEqualTo(key);
		assertThat(event.amount()).isEqualByComparingTo("12.50");
		assertThat(event.schemaVersion()).isEqualTo(PaymentInitiated.SCHEMA_VERSION);
	}

	@Test
	void retryWithSameKeyReturnsSamePaymentId() throws Exception {
		var key = "gw-retry-" + UUID.randomUUID();
		var a = json.readTree(postPayment(key, BODY, 202)).get("paymentId").asString();
		var b = json.readTree(postPayment(key, BODY, 202)).get("paymentId").asString();
		assertThat(a).isEqualTo(b);
	}

	@Test
	void missingIdempotencyKeyIsRejected() throws Exception {
		postPayment(null, BODY, 400);
	}

	@Test
	void malformedIdempotencyKeyIsRejected() throws Exception {
		postPayment("short", BODY, 400);
	}

	@Test
	void invalidBodyIsRejectedBeforeKafka() throws Exception {
		postPayment("gw-bad-" + UUID.randomUUID(), """
				{"fromAccount":"12345","toAccount":"ACC-000006","amount":-1,"currency":"gbp"}""", 400);
	}
}
