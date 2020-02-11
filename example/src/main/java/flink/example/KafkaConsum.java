package flink.example;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsum {
	public static void main(String[] args) {
		List<String> topics = Collections.singletonList("topic");
	    //topics.add("test_topic");
	    Properties consumerConfigurations = new Properties();
	    consumerConfigurations.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.202.99:9092");
	    consumerConfigurations.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    consumerConfigurations.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    consumerConfigurations.put(ConsumerConfig.GROUP_ID_CONFIG, "TestId");

	    Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfigurations);
	    consumer.subscribe(topics);

	    Runtime.getRuntime().addShutdownHook(new Thread()
	    {
	      public void run() {
	        consumer.wakeup();
	      }
	    });

	    try {
	      while (true) {
	        ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
	        Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
	        int i = 0;
	        while (iterator.hasNext()) {
	          i++;
	          ConsumerRecord<String, String> consumerRecord = iterator.next();
	          String key = consumerRecord.key();
	          String value = consumerRecord.value();
	          if (key == "exit" || value == "exit")
	            break;
	          System.out.println("Key=" + key + "\tValue=" + value);
	        }
	        System.out.println("Messages processed = " + Integer.toString(i));
	      }
	    } catch (Exception e) {
	      // Do Nothing
	    	System.out.println(e.getMessage());
	    } finally {
	      consumer.close();
	    }
	  }
		
	}

