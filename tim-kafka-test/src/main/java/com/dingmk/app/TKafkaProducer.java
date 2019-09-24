package com.dingmk.app;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TKafkaProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.101.213:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		String kafkaKey = "key";
//		Producer<String, String> producer = new KafkaProducer<>(props);
//		for(int i = 0; i < 2; i++) {
//			producer.send(new ProducerRecord<String, String>("test-topic", kafkaKey + i, i+""));
//		}

		String topic = "test-topic";
		String msg = "message content";
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, kafkaKey, msg);
		Producer<String, String> producer = new KafkaProducer<>(props);
		//同步发送：
		Future<RecordMetadata> response = producer.send(record);
//		try {
//			log.info("response message: {}", response.get().toString());
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (ExecutionException e) {
//			e.printStackTrace();
//		}

		//异步发送
//		Future<RecordMetadata> response = producer.send(record, new Callback() {
//			@Override
//			public void onCompletion(final RecordMetadata metadata, final Exception e) {
//				
//				if (e != null) {
//					log.error("Send to Kafka topic faild! topic:[{}], message {}", topic, e.getMessage(), e);
//				}
//				System.out.println("The offset of the record we just sent is: " + metadata.offset());
//			}
//		});
//		
//		try {
//			log.info("response message: {}", response.get().toString());
//		} catch (InterruptedException | ExecutionException e1) {
//			log.error("Send to Kafka topic faild! topic:[{}], message {}", topic, e1.getMessage(), e1);
//			e1.printStackTrace();
//		} finally {
//			
//		}
		
//		producer.close();
	}
}
