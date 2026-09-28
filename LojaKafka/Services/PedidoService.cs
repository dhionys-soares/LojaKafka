using LojaKafka.Caches;
using LojaKafka.Kafka.Producers;
using LojaKafka.Models;

namespace LojaKafka.Services;

public sealed class PedidoService : IPedidoService
{
    private readonly IPedidoCache _pedidoCache;
    private readonly PedidoProducer _pedidoProducer;

    public PedidoService(IPedidoCache pedidoCache, PedidoProducer pedidoProducer)
    {
        _pedidoCache = pedidoCache;
        _pedidoProducer = pedidoProducer;
    }

    public IReadOnlyCollection<Pedido> Listar()
    {
        return _pedidoCache.ObterTodos();
    }

    public Pedido? ObterPorId(Guid id)
    {
        return _pedidoCache.ObterPorId(id);
    }

    public async Task<Pedido> CriarAsync(
        string produto,
        int quantidade,
        decimal precoUnitario)
    {
        var pedido = new Pedido(
            Guid.NewGuid(),
            produto,
            quantidade,
            precoUnitario);

        _pedidoCache.Adicionar(pedido);

        await _pedidoProducer.PublishAsync(pedido);

        return pedido;
    }

    public bool Atualizar(
        Guid id,
        string produto,
        int quantidade,
        decimal precoUnitario)
    {
        var pedido = new Pedido(id, produto, quantidade, precoUnitario);
        return _pedidoCache.Atualizar(pedido);
    }

    public bool Remover(Guid id)
    {
        return _pedidoCache.Remover(id);
    }
}
