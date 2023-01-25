import java.util.Objects;
import org.opensky.atco.queueTranscriptQueueItem.
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

/**
 *
 **/
public class CustomPartitioner extends DefaultPartitioner {

  @Override
  public int partition(String topic, Object key, byte[] keyBytes,
  Object value, byte[] valueBytes,
      Cluster cluster) {

    String partitionKey = null;
    if (Objects.nonNull(key)) {
      TranscriptQueueItem item = (TranscriptQueueItem) valueBytes;
      partitionKey = item.getDeviceId();
      keyBytes = partitionKey.getBytes();
    }
    return super.partition(topic, partitionKey, keyBytes, value,
    valueBytes, cluster);
  }
}

public class SampleProducer {

  public static void main(String[] args) {
    String topicName = "recording_raw";

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");

 // we need to define the key serializer here
    props.put("key.serializer",
"org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer",
"org.apache.kafka.common.serialization.StringSerializer");

    // here we are using the partitioner we created
    props.put("partitioner.class",
"com.opensky.CustomPartitioner");

    Producer<TranscriptQueueItem, String> producer = new KafkaProducer<>(props);

    for (int i = 0; i <10; i++) {
      producer.send(
          new ProducerRecord<>(topicName,
              new TranscriptQueueItem(),
              "value" + i));
    }
    producer.close();
  }
}
