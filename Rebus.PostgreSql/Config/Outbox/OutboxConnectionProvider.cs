using System;
using Npgsql;
using Rebus.PostgreSql.Outbox;

namespace Rebus.Config.Outbox;

class OutboxConnectionProvider : IOutboxConnectionProvider
{
    readonly string _connectionString;

    public OutboxConnectionProvider(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public OutboxConnection GetDbConnection()
    {
        var connection = new NpgsqlConnection(_connectionString);

        try
        {
            connection.Open();

            var transaction = connection.BeginTransaction();

            return new OutboxConnection(connection, transaction);
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    public OutboxConnection GetDbConnectionWithoutTransaction()
    {
        var connection = new NpgsqlConnection(_connectionString);

        try
        {
            connection.Open();

            return new OutboxConnection(connection, null);
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }
}