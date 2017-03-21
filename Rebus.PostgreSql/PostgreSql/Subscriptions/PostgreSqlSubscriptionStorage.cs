using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Subscriptions;

namespace Rebus.PostgreSql.Subscriptions
{
    /// <summary>
    /// Implementation of <see cref="ISubscriptionStorage"/> that uses Postgres to do its thing
    /// </summary>
    public class PostgreSqlSubscriptionStorage : ISubscriptionStorage
    {
        const string UniqueKeyViolation = "23505";

        readonly PostgresConnectionHelper _connectionHelper;
        readonly string _tableName;
        readonly ILog _log;

        /// <summary>
        /// Constructs the subscription storage, storing subscriptions in the specified <paramref name="tableName"/>.
        /// If <paramref name="isCentralized"/> is true, subscribing/unsubscribing will be short-circuited by manipulating
        /// subscriptions directly, instead of requesting via messages
        /// </summary>
        public PostgreSqlSubscriptionStorage(PostgresConnectionHelper connectionHelper, string tableName, bool isCentralized, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (connectionHelper == null) throw new ArgumentNullException(nameof(connectionHelper));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionHelper = connectionHelper;
            _tableName = tableName;
            IsCentralized = isCentralized;
            _log = rebusLoggerFactory.GetLogger<PostgreSqlSubscriptionStorage>();
        }

        /// <summary>
        /// Creates the subscriptions table if no table with the specified name exists
        /// </summary>
        public void EnsureTableIsCreated()
        {
            using (var connection = _connectionHelper.GetConnection().Result)
            {
                var tableNames = connection.GetTableNames().ToHashSet();

                if (tableNames.Contains(_tableName)) return;

                _log.Info("Table '{0}' does not exist - it will be created now", _tableName);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
CREATE TABLE ""{_tableName
                            }"" (
	""topic"" VARCHAR(200) NOT NULL,
	""address"" VARCHAR(200) NOT NULL,
	PRIMARY KEY (""topic"", ""address"")
);
";
                    command.ExecuteNonQuery();
                }

                Task.Run(async () => await connection.Complete()).Wait();
            }
        }

        /// <summary>
        /// Gets all destination addresses for the given topic
        /// </summary>
        public async Task<string[]> GetSubscriberAddresses(string topic)
        {
            using (var connection = await _connectionHelper.GetConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"select ""address"" from ""{_tableName}"" where ""topic"" = @topic";

                command.Parameters.AddWithValue("topic", NpgsqlDbType.Text, topic);

                var endpoints = new List<string>();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        endpoints.Add((string)reader["address"]);
                    }
                }

                return endpoints.ToArray();
            }
        }

        /// <summary>
        /// Registers the given <paramref name="subscriberAddress" /> as a subscriber of the given topic
        /// </summary>
        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            using (var connection = await _connectionHelper.GetConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    $@"insert into ""{_tableName}"" (""topic"", ""address"") values (@topic, @address)";

                command.Parameters.AddWithValue("topic", NpgsqlDbType.Text, topic);
                command.Parameters.AddWithValue("address", NpgsqlDbType.Text, subscriberAddress);

                try
                {
                    command.ExecuteNonQuery();
                }
                catch (PostgresException exception) when (exception.SqlState == UniqueKeyViolation)
                {
                    // it's already there
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Unregisters the given <paramref name="subscriberAddress" /> as a subscriber of the given topic
        /// </summary>
        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            using (var connection = await _connectionHelper.GetConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    $@"delete from ""{_tableName}"" where ""topic"" = @topic and ""address"" = @address;";

                command.Parameters.AddWithValue("topic", NpgsqlDbType.Text, topic);
                command.Parameters.AddWithValue("address", NpgsqlDbType.Text, subscriberAddress);

                try
                {
                    command.ExecuteNonQuery();
                }
                catch (NpgsqlException exception)
                {
                    Console.WriteLine(exception);
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Gets whether the subscription storage is centralized and thus supports bypassing the usual subscription request
        /// </summary>
        public bool IsCentralized { get; }
    }
}
