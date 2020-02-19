import java.lang
import java.nio.charset.StandardCharsets

import Analytics.ResultMessage
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

class SerializationSchema(topic: String) extends KafkaSerializationSchema[ResultMessage] {

  override def serialize(element: ResultMessage, timestamp: lang.Long): ProducerRecord[Array[Byte],Array[Byte]] = {
    val producerRecord = new ProducerRecord[Array[Byte],Array[Byte]](topic, element.key.getBytes(StandardCharsets.UTF_8), element.value.getBytes(StandardCharsets.UTF_8))
    producerRecord
  }
}
