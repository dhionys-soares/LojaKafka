using Confluent.Kafka;
using LojaKafka.Models;
using LojaKafka.Services;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace LojaKafka.Kafka.Consumers
{
    public class EstoqueConsumer : BackgroundService
    {
        private readonly KafkaSettings _kafkaSettings;
        private readonly IEstoqueService _estoqueService;

        public EstoqueConsumer(IOptions<KafkaSettings> kafkaSettings, IEstoqueService estoqueService)
        {            
            _kafkaSettings = kafkaSettings.Value;
            _estoqueService = estoqueService;
        }

        public IConsumer<Null, string> CreateConsumer()
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _kafkaSettings.BootstrapServers,
                GroupId = "estoque-consumer",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();

            return consumer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var consumer = CreateConsumer();

            consumer.Subscribe(_kafkaSettings.TopicName);

            try 
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var result = consumer.Consume(stoppingToken);
                    var message = result.Message.Value;

                    try
                    {
                        var pedido = JsonSerializer.Deserialize<Pedido>(message);
                        
                        if (pedido is not null)
                        {
                            var disponivel = _estoqueService.VerificarDisponibilidade(pedido);

                            if (disponivel)
                            {
                                Console.WriteLine($"Estoque disponível para o pedido {pedido.Id}.");
                                consumer.Commit(result);
                            }
                        }
                        
                    }

                    catch (JsonException)
                    {

                    }
                    

                    
                }
            }
            catch(OperationCanceledException)
            {
                
            }
            finally
            {
                consumer.Close();
            }

        }
    }
}
