using Confluent.Kafka;
using LojaKafka.Kafka;

namespace LojaKafka.Extensions
{
    public static class KafkaSettingsExtension
    {
        public static void AddKafkaSettings(this IServiceCollection services, IConfiguration configuration)
        {
            // obtem as configurações do Kafka do arquivo appsettings.json
            services.Configure<KafkaSettings>(
                configuration.GetSection("KafkaSettings"));

            // Adiciona o produtor do Kafka como serviços singleton
            services.AddSingleton<IProducer<Null, string>>(options => 
            {
                var configuration = options.GetRequiredService<IConfiguration>();
                
                var bootstrapServers = configuration["KafkaSettings:BootstrapServers"];
                var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

                return new ProducerBuilder<Null, string>(producerConfig).Build();
            });

            // Adiciona o consumidor do Kafka como serviços singleton
            services.AddSingleton<IConsumer<Null, string>> (options =>
            {
                var configuration = options.GetRequiredService<IConfiguration>();
                var bootstrapServers = configuration["KafkaSettings:BootstrapServers"];
                var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers };

                return new ConsumerBuilder<Null, string>(consumerConfig).Build();
            });
        }
    }
}
