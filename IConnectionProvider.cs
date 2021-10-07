using System.Data;

namespace VDataAccessLayer
{
    public interface IConnectionProvider
    {
        IDbConnection CreateConnection();
    }
}
