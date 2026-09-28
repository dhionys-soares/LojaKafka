using LojaKafka.Models;

namespace LojaKafka.Caches;

public interface IPedidoCache
{
    IReadOnlyCollection<Pedido> ObterTodos();

    Pedido? ObterPorId(Guid id);

    void Adicionar(Pedido pedido);

    bool Atualizar(Pedido pedido);

    bool Remover(Guid id);
}
