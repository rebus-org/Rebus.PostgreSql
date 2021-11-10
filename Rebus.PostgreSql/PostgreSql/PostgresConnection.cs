using System;
using System.Threading.Tasks;
using Npgsql;
// ReSharper disable EmptyGeneralCatchClause
#pragma warning disable 1998

namespace Rebus.PostgreSql
{
    /// <summary>
    /// Wraps an opened <see cref="NpgsqlConnection"/> and makes it easier to work with it
    /// </summary>
    public class PostgresConnection : IDisposable
    {
        readonly NpgsqlConnection _currentConnection;
        NpgsqlTransaction _currentTransaction;

        readonly bool _managedExternally;
        bool _disposed;

        /// <summary>
        /// Constructs the wrapper with the given connection and transaction
        /// </summary>
        public PostgresConnection(NpgsqlConnection currentConnection, NpgsqlTransaction currentTransaction, bool managedExternally = false)
        {
            _currentConnection = currentConnection ?? throw new ArgumentNullException(nameof(currentConnection));
            _currentTransaction = currentTransaction ?? throw new ArgumentNullException(nameof(currentTransaction));
            _managedExternally = managedExternally;
        }

        /// <summary>
        /// Constructs the wrapper with the given connection which should already be enlisted in a transaction.
        /// </summary>
        public PostgresConnection(NpgsqlConnection currentConnection)
        {
            _currentConnection = currentConnection ?? throw new ArgumentNullException(nameof(currentConnection));
        }

        /// <summary>
        /// Creates a new command, enlisting it in the current transaction
        /// </summary>
        public NpgsqlCommand CreateCommand()
        {
            var command = _currentConnection.CreateCommand();
            command.Transaction = _currentTransaction;
            return command;
        }

        /// <summary>
        /// Completes the transaction
        /// </summary>

        public async Task Complete()
        {
            if (_managedExternally) return;
            if (_currentTransaction == null) return;
            using (_currentTransaction)
            {
                _currentTransaction.Commit();
                _currentTransaction = null;
            }
        }

        /// <summary>
        /// Rolls back the transaction if it hasn't been completed
        /// </summary>
        public void Dispose()
        {
            if (_managedExternally) return;
            if (_disposed) return;

            try
            {
                try
                {
                    if (_currentTransaction == null) return;
                    using (_currentTransaction)
                    {
                        try
                        {
                            _currentTransaction.Rollback();
                        }
                        catch { }
                        _currentTransaction = null;
                    }
                }
                finally
                {
                    _currentConnection.Dispose();
                }
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}