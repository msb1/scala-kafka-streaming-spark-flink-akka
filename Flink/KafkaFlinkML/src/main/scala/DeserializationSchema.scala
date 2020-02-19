import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

case class KafkaMessage(key: String, value: String, partition: Int, offset: Long)

class DeserializationSchema extends KafkaDeserializationSchema[KafkaMessage] {

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): KafkaMessage = {
    val key = new String(record.key(), StandardCharsets.UTF_8)
    val value = new String(record.value(), StandardCharsets.UTF_8)
    val kafkaMessage = new KafkaMessage(key, value, record.partition(), record.offset())
    kafkaMessage
  }

  override def getProducedType: TypeInformation[KafkaMessage] = {
    TypeInformation.of(classOf[KafkaMessage])
  }

  override def isEndOfStream(nextElement: KafkaMessage): Boolean = {
    false
  }

}
