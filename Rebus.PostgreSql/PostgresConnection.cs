using System;
using Npgsql;

namespace Rebus.PostgreSql
{
    /// <summary>
    /// Wraps an opened <see cref="NpgsqlConnection"/> and makes it easier to work with it
    /// </summary>
    public class PostgresConnection : IDisposable
    {
        readonly NpgsqlConnection _currentConnection;
        readonly NpgsqlTransaction _currentTransaction;

        bool _completed;
        bool _disposed;

        /// <summary>
        /// Constructs the wrapper with the given connection and transaction
        /// </summary>
        public PostgresConnection(NpgsqlConnection currentConnection, NpgsqlTransaction currentTransaction)
        {
            if (currentConnection == null) throw new ArgumentNullException(nameof(currentConnection));
            if (currentTransaction == null) throw new ArgumentNullException(nameof(currentTransaction));
            _currentConnection = currentConnection;
            _currentTransaction = currentTransaction;
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
        public void Complete()
        {
            _currentTransaction.Commit();
            _completed = true;
        }

        /// <summary>
        /// Rolls back the transaction if it hasn't been completed
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                if (!_completed)
                {
                    // must never fail!
                    try
                    {
                        _currentTransaction.Rollback();
                    }
                    catch { }
                }

                _currentTransaction.Dispose();
                _currentConnection.Dispose();
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}