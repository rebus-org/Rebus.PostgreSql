using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NpgsqlTypes;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Transport;
using System.Linq;
using Npgsql;
using Rebus.Extensions;
using Rebus.Serialization;
using Rebus.Time;

namespace Rebus.PostgreSql.Transport
{
    /// <summary>
    /// 
    /// </summary>
    public class PostgreSqlTransport : ITransport, IInitializable, IDisposable
    {
        private readonly PostgresConnectionHelper _connectionHelper;
        private readonly string _tableName;
        private readonly string _inputQueueName;
        private readonly IAsyncTaskFactory _asyncTaskFactory;
        bool _disposed;
        private ILog _log;

         const string CurrentConnectionKey = "postgresql-transport-current-connection";
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();
        /// <summary>
        /// 
        /// </summary>
        public static readonly TimeSpan DefaultExpiredMessagesCleanupInterval = TimeSpan.FromSeconds(20);

         readonly AsyncBottleneck _bottleneck = new AsyncBottleneck(20);

          const int OperationCancelledNumber = 3980;

        private readonly IAsyncTask _expiredMessagesCleanupTask;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionHelper"></param>
        /// <param name="tableName"></param>
        /// <param name="inputQueueName"></param>
        /// <param name="rebusLoggerFactory"></param>
        /// <param name="asyncTaskFactory"></param>
        public PostgreSqlTransport(PostgresConnectionHelper connectionHelper, string tableName, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory)
        {
            _connectionHelper = connectionHelper;
            _tableName = tableName;
            _inputQueueName = inputQueueName;
            _asyncTaskFactory = asyncTaskFactory;
            ExpiredMessagesCleanupInterval = DefaultExpiredMessagesCleanupInterval;
            _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup",
                PerformExpiredMessagesCleanupCycle, intervalSeconds: 60);

            _log = rebusLoggerFactory.GetCurrentClassLogger();
        }

        public void Initialize()
        {
            if (_inputQueueName == null) return;
            _expiredMessagesCleanupTask.Start();
        }

         /// <summary>
         /// 
         /// </summary>
         public TimeSpan ExpiredMessagesCleanupInterval { get; set; }

        public void CreateQueue(string address)
        {
            throw new System.NotImplementedException();
        }

        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var connection = await GetConnection(context);

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
INSERT INTO {_tableName}
(
    recipient,
    headers,
    body,
    priority,
    visible,
    expiration
)
VALUES
(
    @recipient,
    @headers,
    @body,
    @priority,
    now() + @ttlseconds,
    now() + @visible
)";

                var headers = message.Headers.Clone();

                var priority = GetMessagePriority(headers);
                var initialVisibilityDelay = GetInitialVisibilityDelay(headers);
                var ttlSeconds = GetTtlSeconds(headers);

                // must be last because the other functions on the headers might change them
                var serializedHeaders = HeaderSerializer.Serialize(headers);

                command.Parameters.Add("recipient", NpgsqlDbType.Text ).Value = destinationAddress;
                command.Parameters.Add("headers", NpgsqlDbType.Bytea).Value = serializedHeaders;
                command.Parameters.Add("body", NpgsqlDbType.Bytea).Value = message.Body;
                command.Parameters.Add("priority", NpgsqlDbType.Integer).Value = priority;
                command.Parameters.Add("ttlseconds", NpgsqlDbType.Interval).Value = $"{ttlSeconds} s";
                command.Parameters.Add("visible", NpgsqlDbType.Interval).Value = $"{initialVisibilityDelay} s";

                
                await command.ExecuteNonQueryAsync();
            }
        }

        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _bottleneck.Enter(cancellationToken))
            {
                var connection = await GetConnection(context);

                TransportMessage receivedTransportMessage;

                using (var selectCommand = connection.CreateCommand())
                {
                    selectCommand.CommandText = $@"
DELETE from {_tableName} 
where id = 
(
    select id from {_tableName}
    where recipient = @recipient
    and visible < now()
    and expiration > now()
    order by priority asc, id asc
    for update skip locked
    limit 1
)
returning id,
headers,
body
";

                    selectCommand.Parameters.Add("recipient", NpgsqlDbType.Text).Value = _inputQueueName;

                    try
                    {
                        using (var reader = await selectCommand.ExecuteReaderAsync(cancellationToken))
                        {
                            if (!await reader.ReadAsync(cancellationToken)) return null;

                            var headers = reader["headers"];
                            var headersDictionary = HeaderSerializer.Deserialize((byte[])headers);
                            var body = (byte[])reader["body"];

                            receivedTransportMessage = new TransportMessage(headersDictionary, body);
                        }
                    }
                    catch (SqlException sqlException) when (sqlException.Number == OperationCancelledNumber)
                    {
                        // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                        throw new TaskCanceledException("Receive operation was cancelled", sqlException);
                    }
                }

                return receivedTransportMessage;
            }
        }

        async Task PerformExpiredMessagesCleanupCycle()
        {
            var results = 0;
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                using (var connection = await _connectionHelper.GetConnection())
                {
                    int affectedRows;

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                            $@"
	            Delete FROM {_tableName} 
				WHERE recipient = @recipient 
				AND [expiration] < getdate()
";
                        command.Parameters.Add("recipient",  (NpgsqlDbType) DbType.String).Value = _inputQueueName;
                        affectedRows = await command.ExecuteNonQueryAsync();
                    }

                    results += affectedRows;
                    connection.Complete();

                    if (affectedRows == 0) break;
                }
            }

            if (results > 0)
            {
                _log.Info(
                    "Performed expired messages cleanup in {0} - {1} expired messages with recipient {2} were deleted",
                    stopwatch.Elapsed, results, _inputQueueName);
            }
        }

        public string Address { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void EnsureTableIsCreated()
        {
            try
            {
                CreateSchema();
            }
            catch (SqlException exception)
            {
                throw new RebusApplicationException(exception, $"Error attempting to initialize SQL transport schema with mesages table [dbo].[{_tableName}]");
            }
        }

        private void CreateSchema()
        {
            using (var connection = _connectionHelper.GetConnection().Result)
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_tableName, StringComparer.OrdinalIgnoreCase))
                {
                    _log.Info("Database already contains a table named '{0}' - will not create anything", _tableName);
                    return;
                }

                _log.Info("Table '{0}' does not exist - it will be created now", _tableName);

                ExecuteCommands(connection, $@"
CREATE TABLE {_tableName}
(
	id serial NOT NULL,
	recipient text NOT NULL,
	priority int NOT NULL,
    expiration timestamp NOT NULL,
    visible timestamp NOT NULL,
	headers bytea NOT NULL,
	body bytea NOT NULL,
    PRIMARY KEY (recipient, priority, id)
);
----
CREATE INDEX idx_receive_{_tableName} ON {_tableName}
(
	recipient ASC,
	priority ASC,
    visible ASC,
    expiration ASC,
	id ASC
);
----
CREATE INDEX idx_expiration_{_tableName} ON {_tableName}
(
    expiration ASC
);
");

                connection.Complete().Wait();
            }

        }

        static void ExecuteCommands(PostgresConnection connection, string sqlCommands)
        {
            foreach (var sqlCommand in sqlCommands.Split(new[] { "----" }, StringSplitOptions.RemoveEmptyEntries))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlCommand;

                    Execute(command);
                }
            }
        }

        static void Execute(IDbCommand command)
        {
            try
            {
                command.ExecuteNonQuery();
            }
            catch (NpgsqlException exception)
            {
                throw new RebusApplicationException(exception, $@"Error executing SQL command
{command.CommandText}
");
            }
        }

        Task<PostgresConnection> GetConnection(ITransactionContext context)
        {
            return context
                .GetOrAdd(CurrentConnectionKey,
                    async () =>
                    {
                        var dbConnection = await _connectionHelper.GetConnection();
                        context.OnCommitted(async () => await dbConnection.Complete());
                        context.OnDisposed(() =>
                        {
                            dbConnection.Dispose();
                        });
                        return dbConnection;
                    });
        }


        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _expiredMessagesCleanupTask.Dispose();
            }
            finally
            {
                _disposed = true;
            }
        }

        static int GetMessagePriority(Dictionary<string, string> headers)
        {
            var valueOrNull = headers.GetValueOrNull(MessagePriorityHeaderKey);
            if (valueOrNull == null) return 0;

            try
            {
                return int.Parse(valueOrNull);
            }
            catch (Exception exception)
            {
                throw new FormatException($"Could not parse '{valueOrNull}' into an Int32!", exception);
            }
        }

        static int GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            string deferredUntilDateTimeOffsetString;

            if (!headers.TryGetValue(Headers.DeferredUntil, out deferredUntilDateTimeOffsetString))
            {
                return 0;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            return (int)(deferredUntilTime - RebusTime.Now).TotalSeconds;
        }

        static int GetTtlSeconds(IReadOnlyDictionary<string, string> headers)
        {
            const int defaultTtlSecondsAbout60Years = int.MaxValue;

            if (!headers.ContainsKey(Headers.TimeToBeReceived))
                return defaultTtlSecondsAbout60Years;

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return (int)timeToBeReceived.TotalSeconds;
        }
    }
}
