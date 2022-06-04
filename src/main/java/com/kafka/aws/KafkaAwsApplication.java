package com.kafka.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.kafka.aws.models.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@EnableScheduling
@Slf4j
@SpringBootApplication
public class KafkaAwsApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	@Autowired
	private ObjectMapper mapper;
	@Autowired
	private RestHighLevelClient restHighLevelClient;

	public static void main(String[] args) {
		SpringApplication.run(KafkaAwsApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

	}

	@KafkaListener(//id = "devs4j-id", autoStartup = "true",
			topics = "kevin-transactions",
			containerFactory = "listenerContainerFactory",
			groupId = "kevin-group"
//			properties = {"max.poll.interval.ms:4000", "max.poll.records:50"}
	)
	public void listen(List<ConsumerRecord<String, String>> messages) throws JsonProcessingException {
		log.info("Messages received: {}", messages.size());
//		log.info("Start reading messages");
		for (ConsumerRecord<String, String> message : messages) {
//			Transaction transaction = mapper.readValue(message.value(), Transaction.class);
//			log.info("Offset={}, Partition={}, Key={}, Value={}", message.offset(),
//					message.partition(), message.key(), message.value());
			IndexRequest indexRequest = buildIndexRequest(String.format("%s-%s-%s", message.partition(), message.key(),
					message.offset()), message.value());
			restHighLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
				@Override
				public void onResponse(IndexResponse indexResponse) {
					log.debug("Successful request");
				}

				@Override
				public void onFailure(Exception e) {
					log.error("Error storing the message: ", e);
				}
			});
		}
//		log.info("Batch completed");
	}

	private IndexRequest buildIndexRequest(String key, String value) {
		IndexRequest indexRequest = new IndexRequest("kevin-transactions");
		indexRequest.id(key);
		indexRequest.source(value, XContentType.JSON);

		return indexRequest;
	}

	@Scheduled(fixedRate = 10_000)
	public void sendKafkaMessages() throws JsonProcessingException {
		Faker faker = new Faker();
		for (int i = 0; i < 10_000; i++) {
			Transaction transaction = new Transaction();
			transaction.setName(faker.name().firstName());
			transaction.setUsername(faker.name().username());
			transaction.setLastname(faker.name().lastName());
			transaction.setAmount(faker.number().randomDouble(4, 2, 50_000));
			kafkaTemplate.send("kevin-transactions", transaction.getUsername(),
					mapper.writeValueAsString(transaction));
		}
	}
}
