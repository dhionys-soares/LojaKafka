using LojaKafka.Models;
using Microsoft.Extensions.Caching.Memory;

namespace LojaKafka.Caches;

public sealed class PedidoCache : IPedidoCache
{
    private const string CacheKey = "pedidos";
    private readonly IMemoryCache _cache;
    private readonly object _syncRoot = new();

    public PedidoCache(IMemoryCache cache)
    {
        _cache = cache;
    }

    public IReadOnlyCollection<Pedido> ObterTodos()
    {
        lock (_syncRoot)
        {
            return ObterPedidos().ToArray();
        }
    }

    public Pedido? ObterPorId(Guid id)
    {
        lock (_syncRoot)
        {
            return ObterPedidos().FirstOrDefault(pedido => pedido.Id == id);
        }
    }

    public void Adicionar(Pedido pedido)
    {
        lock (_syncRoot)
        {
            var pedidos = ObterPedidos();
            pedidos.Add(pedido);
            SalvarPedidos(pedidos);
        }
    }

    public bool Atualizar(Pedido pedido)
    {
        lock (_syncRoot)
        {
            var pedidos = ObterPedidos();
            var indice = pedidos.FindIndex(item => item.Id == pedido.Id);

            if (indice < 0)
            {
                return false;
            }

            pedidos[indice] = pedido;
            SalvarPedidos(pedidos);
            return true;
        }
    }

    public bool Remover(Guid id)
    {
        lock (_syncRoot)
        {
            var pedidos = ObterPedidos();
            var removidos = pedidos.RemoveAll(pedido => pedido.Id == id);

            if (removidos == 0)
            {
                return false;
            }

            SalvarPedidos(pedidos);
            return true;
        }
    }

    private List<Pedido> ObterPedidos()
    {
        return _cache.TryGetValue(CacheKey, out List<Pedido>? pedidos)
            ? pedidos!
            : [];
    }

    private void SalvarPedidos(List<Pedido> pedidos)
    {
        _cache.Set(CacheKey, pedidos);
    }
}
