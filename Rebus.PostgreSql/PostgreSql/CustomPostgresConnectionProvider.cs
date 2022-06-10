using System;
using System.Data;
using System.Threading.Tasks;
using Npgsql;
// ReSharper disable UnusedMember.Global

namespace Rebus.PostgreSql;

/// <summary>
/// Implementation of <see cref="IPostgresConnectionProvider"/> that provides the connection using a user-provided function.
/// Will optionally start transactions whenever the connection is provided.
/// 
/// </summary>
public class CustomPostgresConnectionProvider : IPostgresConnectionProvider
{
    readonly Func<Task<NpgsqlConnection>> _provideConnection;
    readonly bool _autoStartTransactions;

    /// <summary>
    /// Constructor that allows specifying transaction behavior
    /// Defaults to not starting transactions
    /// </summary>
    /// <param name="provideConnection">Function that will provide an asynchronous <see cref="NpgsqlConnection"/> when invoked</param>
    /// <param name="autoStartTransactions">Whether to automatically start transaction every time a new connection is provided</param>
    public CustomPostgresConnectionProvider(Func<Task<NpgsqlConnection>> provideConnection, bool autoStartTransactions = false)
    {
        _provideConnection = provideConnection;
        _autoStartTransactions = autoStartTransactions;
    }


        
    /// <summary>
    /// Constructor that allows specifying transaction behavior
    /// Defaults to not starting transactions
    ///  Allows providing the connection through a non-async Func
    /// </summary>
    /// <param name="provideConnection">Function that will provide an <see cref="NpgsqlConnection"/> when invoked</param>
    /// <param name="autoStartTransactions">Whether to automatically start transaction every time a new connection is provided</param>
    public CustomPostgresConnectionProvider(Func<NpgsqlConnection> provideConnection, bool autoStartTransactions = false) : this(()=>Task.FromResult(provideConnection()), autoStartTransactions)
    {
    }



    /// <summary>
    /// Getst the connection by using user-provided Func. Will optionally start a transaction on the connection if configured to do so. 
    /// 
    /// </summary>
    /// <returns>The <see cref="PostgresConnection"/> object wrapping the connection and transaction</returns>
    public async Task<PostgresConnection> GetConnection()
    {
        var connection = await _provideConnection();
        var transaction = _autoStartTransactions ? connection.BeginTransaction(IsolationLevel.ReadCommitted) : null;
        return new PostgresConnection(connection, transaction);
    }
}