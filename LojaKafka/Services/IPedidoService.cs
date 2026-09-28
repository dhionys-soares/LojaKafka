using LojaKafka.Models;

namespace LojaKafka.Services;

public interface IPedidoService
{
    IReadOnlyCollection<Pedido> Listar();

    Pedido? ObterPorId(Guid id);

    Task<Pedido> CriarAsync(string produto, int quantidade, decimal precoUnitario);

    bool Atualizar(Guid id, string produto, int quantidade, decimal precoUnitario);

    bool Remover(Guid id);
}
