using System.Data.SqlClient;
using System.Threading.Tasks;
using Npgsql;

namespace Rebus.PostgreSql
{
    /// <summary>
    /// PostgreSql Server database connection provider that allows for easily changing how the current <see cref="PostgresConnection"/> is obtained,
    /// possibly also changing how transactions are handled
    /// </summary>
    public interface IPostgresConnectionProvider
    {
        /// <summary>
        /// Gets a wrapper with the current <see cref="PostgresConnection"/> inside
        /// </summary>
        Task<PostgresConnection> GetConnection();
    }
}