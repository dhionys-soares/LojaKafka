namespace LojaKafka.Models
{
    public record Pedido(
        Guid Id,
        string Produto,
        int Quantidade,
        decimal PrecoUnitario
    );
}
