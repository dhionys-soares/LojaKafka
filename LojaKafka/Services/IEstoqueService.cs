namespace LojaKafka.Services;
using LojaKafka.Models;

    public interface IEstoqueService
    {
        bool VerificarDisponibilidade(Pedido pedido);
    }

