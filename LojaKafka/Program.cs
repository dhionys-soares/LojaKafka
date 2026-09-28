using LojaKafka.Extensions;
using LojaKafka.Caches;
using LojaKafka.Kafka.Producers;
using LojaKafka.Services;
using LojaKafka.Kafka.Consumers;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

// Adiciona as configurações do Kafka por meio da extensionMethod
builder.Services.AddKafkaSettings(builder.Configuration);
builder.Services.AddMemoryCache();
builder.Services.AddSingleton<IPedidoCache, PedidoCache>();
builder.Services.AddSingleton<PedidoProducer>();
builder.Services.AddHostedService<EstoqueConsumer>();
builder.Services.AddScoped<IPedidoService, PedidoService>();
builder.Services.AddSingleton<IEstoqueService, EstoqueService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapGet("/pedidos", (IPedidoService pedidoService) =>
    Results.Ok(pedidoService.Listar()));

app.MapGet("/pedidos/{id:guid}", (Guid id, IPedidoService pedidoService) =>
{
    var pedido = pedidoService.ObterPorId(id);
    return pedido is null ? Results.NotFound() : Results.Ok(pedido);
});

app.MapPost("/pedidos", async (PedidoRequest request, IPedidoService pedidoService) =>
{
    if (string.IsNullOrWhiteSpace(request.Produto) ||
        request.Quantidade <= 0 ||
        request.PrecoUnitario < 0)
    {
        return Results.BadRequest("Produto, quantidade e preço unitário devem ser válidos.");
    }

    var pedido = await pedidoService.CriarAsync(
        request.Produto,
        request.Quantidade,
        request.PrecoUnitario);

    return Results.Created($"/pedidos/{pedido.Id}", pedido);
});

app.MapPut("/pedidos/{id:guid}",
    (Guid id, PedidoRequest request, IPedidoService pedidoService) =>
    {
        if (string.IsNullOrWhiteSpace(request.Produto) ||
            request.Quantidade <= 0 ||
            request.PrecoUnitario < 0)
        {
            return Results.BadRequest("Produto, quantidade e preço unitário devem ser válidos.");
        }

        var atualizado = pedidoService.Atualizar(
            id,
            request.Produto,
            request.Quantidade,
            request.PrecoUnitario);

        return atualizado
            ? Results.NoContent()
            : Results.NotFound();
    });

app.MapDelete("/pedidos/{id:guid}", (Guid id, IPedidoService pedidoService) =>
    pedidoService.Remover(id) ? Results.NoContent() : Results.NotFound());

app.Run();

internal sealed record PedidoRequest(
    string Produto,
    int Quantidade,
    decimal PrecoUnitario);
