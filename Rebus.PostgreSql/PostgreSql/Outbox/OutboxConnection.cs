using System;
using Npgsql;

// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.PostgreSql.Outbox;

/// <summary>
/// Holds an open <see cref="SqlConnection"/>
/// </summary>
public class OutboxConnection
{
    /// <summary>
    /// Gets the connection
    /// </summary>
    public NpgsqlConnection Connection { get; }

    /// <summary>
    /// Gets the current transaction
    /// </summary>
    public NpgsqlTransaction Transaction { get; }

    internal OutboxConnection(NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
    }
}
