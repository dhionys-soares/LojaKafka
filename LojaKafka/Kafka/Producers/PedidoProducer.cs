using Confluent.Kafka;
using LojaKafka.Models;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace LojaKafka.Kafka.Producers
{
    public class PedidoProducer
    {
        private readonly IProducer<Null, string> _producer;
        private readonly KafkaSettings _kafkaSettings;

        public PedidoProducer(IProducer<Null, string> producer, IOptions<KafkaSettings> settings)
        {
            _producer = producer;
            _kafkaSettings = settings.Value;
        }

        public async Task PublishAsync(Pedido pedido)
        {
            var message = JsonSerializer.Serialize(pedido);
            await _producer.ProduceAsync(_kafkaSettings.TopicName, new Message<Null, string>
            {
                Value = message
            });
        }

    }
}
