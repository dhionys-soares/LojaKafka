using LojaKafka.Models;

namespace LojaKafka.Services
{
    public class EstoqueService : IEstoqueService
    {
        public bool VerificarDisponibilidade(Pedido pedido)
        {
            return true;
        }
    }
}
