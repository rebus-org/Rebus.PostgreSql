using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Rebus.Internals;
using Rebus.Logging;
using Rebus.Subscriptions;

namespace Rebus.PostgreSql.Subscriptions;

/// <summary>
/// Implementation of <see cref="ISubscriptionStorage"/> that uses Postgres to do its thing
/// </summary>
public class PostgreSqlSubscriptionStorage : ISubscriptionStorage
{
    const string UniqueKeyViolation = "23505";

    readonly IPostgresConnectionProvider _connectionHelper;
    readonly TableName _tableName;
    readonly ILog _log;

    /// <summary>
    /// Constructs the subscription storage, storing subscriptions in the specified <paramref name="tableName"/>.
    /// If <paramref name="isCentralized"/> is true, subscribing/unsubscribing will be short-circuited by manipulating
    /// subscriptions directly, instead of requesting via messages
    /// </summary>
    public PostgreSqlSubscriptionStorage(IPostgresConnectionProvider connectionHelper, string tableName, bool isCentralized, IRebusLoggerFactory rebusLoggerFactory, string schemaName = null)
    {
        if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
        _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
        _tableName = new TableName(schemaName ?? TableName.DefaultSchemaName, tableName ?? throw new ArgumentNullException(nameof(tableName)));
        IsCentralized = isCentralized;
        _log = rebusLoggerFactory.GetLogger<PostgreSqlSubscriptionStorage>();
    }

    /// <summary>
    /// Creates the subscriptions table if no table with the specified name exists
    /// </summary>
    public void EnsureTableIsCreated()
    {
        AsyncHelpers.RunSync(async () =>
        {
            using var connection = await _connectionHelper.GetConnection();

            var tableNames = connection.GetTableNames();

            if (tableNames.Contains(_tableName)) return;

            _log.Info("Table {tableName} does not exist - it will be created now", _tableName);

            var schemaNames = connection.GetSchemas();

            if (!schemaNames.Contains(_tableName.Schema))
            {
                _log.Info("Schema {schemaName} does not exist - it will be created now", _tableName.Schema);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"CREATE SCHEMA ""{_tableName.Schema}"";";

                    command.ExecuteNonQuery();
                }
            }

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
CREATE TABLE {_tableName} (
	""topic"" VARCHAR(200) NOT NULL,
	""address"" VARCHAR(200) NOT NULL,
	PRIMARY KEY (""topic"", ""address"")
);
";
                command.ExecuteNonQuery();
            }

            await connection.Complete();
        });
    }

    /// <summary>
    /// Gets all destination addresses for the given topic
    /// </summary>
    public async Task<IReadOnlyList<string>> GetSubscriberAddresses(string topic)
    {
        using var connection = await _connectionHelper.GetConnection();

        using var command = connection.CreateCommand();

        command.CommandText = $@"select ""address"" from {_tableName} where ""topic"" = @topic";
        command.Parameters.AddWithValue("topic", NpgsqlDbType.Text, topic);

        var endpoints = new List<string>();

        await using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            endpoints.Add((string)reader["address"]);
        }

        return endpoints.ToArray();
    }

    /// <summary>
    /// Registers the given <paramref name="subscriberAddress" /> as a subscriber of the given topic
    /// </summary>
    public async Task RegisterSubscriber(string topic, string subscriberAddress)
    {
        using var connection = await _connectionHelper.GetConnection();

        using var command = connection.CreateCommand();

        command.CommandText = $@"insert into {_tableName} (""topic"", ""address"") values (@topic, @address)";

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

    /// <summary>
    /// Unregisters the given <paramref name="subscriberAddress" /> as a subscriber of the given topic
    /// </summary>
    public async Task UnregisterSubscriber(string topic, string subscriberAddress)
    {
        using var connection = await _connectionHelper.GetConnection();

        using var command = connection.CreateCommand();

        command.CommandText = $@"delete from {_tableName} where ""topic"" = @topic and ""address"" = @address;";

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

    /// <summary>
    /// Gets whether the subscription storage is centralized and thus supports bypassing the usual subscription request
    /// </summary>
    public bool IsCentralized { get; }
}