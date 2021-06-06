## Kafka E-commerce Data Streaming

This is a small simulation of a e-commerce message streaming service. It was made with Java and the Kafka API.
\
&nbsp;

![image](https://user-images.githubusercontent.com/51497214/120552394-94f92500-c3cd-11eb-8275-60855783d2ef.png "The flowchart with the producers-broker-consumers relations.")
The flowchart with the producers-broker-consumers relations.
\
&nbsp;



### Kafka Dispatcher

The Dispatcher class receive the producer message of a given topic and send the record to the broker. 

```java
class KafkaDispatcher<T> implements Closeable{

	private final KafkaProducer<String, Message<T>> producer;

	public KafkaDispatcher() {
		this.producer = new KafkaProducer<String, Message<T>>(properties());
	}

	public void send(String topic, String key, CorrelationId id, T payload) throws InterruptedException, ExecutionException {
		var value = new Message<T>(id, payload);
		var record = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if(ex != null) {
                ex.printStackTrace();
                return;
            }
        }
		producer.send(record, callback).get();
	}
	
	private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "server");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
   }
    
    @Override
	public void close() {
		producer.close();
	}
 }
 ```


### Kafka Service

Working on the other side of the broker, the Kafka service class is responsible for the listening and consumption of records.

```java
class KafkaService<T> implements Closeable {
	
	private final KafkaConsumer<String, Message<T>> consumer;
	private final ConsumerFunction parse;
	private Map<String, String> props;

	KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> props) {
		this.parse = parse;
		this.props = props;
		this.consumer = new KafkaConsumer<String, Message<T>>(properties(groupId, props));
		consumer.subscribe(Collections.singletonList(topic));
	}

	KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Class<T> type, Map<String, String> props) {
		this.parse = parse;
		this.props = props;
		this.consumer = new KafkaConsumer<String, Message<T>>(properties(groupId, props));
		consumer.subscribe(topic);
	}

	public void run() {
		while(true) {
			var records = consumer.poll(Duration.ofMillis(100));
			if(!records.isEmpty()){
				for(var record : records) {
					try {
						parse.consume(record);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	private Properties properties(String groupId, Map<String, String> props) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.putAll(props);
		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}
}
```


### Message Serialization

The serialization is mainly made by the MessageAdapter class

```java
public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message>{

	@Override
	public JsonElement serialize(Message message, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject obj = new JsonObject();
		obj.addProperty("type", message.getPayload().getClass().getName());
		obj.add("payload", context.serialize(message.getPayload()));
		obj.add("correlationId", context.serialize(message.getId()));
		return obj;
	}

	@Override
	public Message deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		var obj = json.getAsJsonObject();
		var payloadType = obj.get("type").getAsString();
		var correlationId = (CorrelationId) context.deserialize(obj.get("correlationId"), CorrelationId.class);
		try {
			var payload = context.deserialize(obj.get("payload"), Class.forName(payloadType));
			return new Message(correlationId, payload);
		} catch (JsonParseException | ClassNotFoundException e) {
			throw new JsonParseException(e); 
		} 
	}

}
```
