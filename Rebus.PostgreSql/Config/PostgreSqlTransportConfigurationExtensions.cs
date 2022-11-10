using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.PostgreSql;
using Rebus.PostgreSql.Transport;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Timeouts;
using Rebus.Transport;
using System;

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for the SQL transport
/// </summary>
public static class PostgreSqlTransportConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to use PostgreSql as its transport. The table specified by <paramref name="tableName"/> will be used to
    /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
    /// The message table will automatically be created if it does not exist.
    /// </summary>
    public static void UsePostgreSql(this StandardConfigurer<ITransport> configurer, string connectionString, string tableName, string inputQueueName, TimeSpan? expiredMessagesCleanupInterval = null)
    {
        UsePostgreSql(configurer, new PostgresConnectionHelper(connectionString), tableName, inputQueueName, expiredMessagesCleanupInterval);
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql as its transport. The table specified by <paramref name="tableName"/> will be used to
    /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
    /// The message table will automatically be created if it does not exist.
    /// </summary>
    public static void UsePostgreSql(this StandardConfigurer<ITransport> configurer, IPostgresConnectionProvider connectionProvider, string tableName, string inputQueueName, TimeSpan? expiredMessagesCleanupInterval = null)
    {
        Configure(configurer, connectionProvider, tableName, inputQueueName, expiredMessagesCleanupInterval);
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql to transport messages as a one-way client (i.e. will not be able to receive any messages).
    /// The table specified by <paramref name="tableName"/> will be used to store messages.
    /// The message table will automatically be created if it does not exist.
    /// </summary>
    public static void UsePostgreSqlAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionString, string tableName, TimeSpan? expiredMessagesCleanupInterval = null)
    {
        UsePostgreSqlAsOneWayClient(configurer, new PostgresConnectionHelper(connectionString), tableName, expiredMessagesCleanupInterval);
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql to transport messages as a one-way client (i.e. will not be able to receive any messages).
    /// The table specified by <paramref name="tableName"/> will be used to store messages.
    /// The message table will automatically be created if it does not exist.
    /// </summary>
    public static void UsePostgreSqlAsOneWayClient(this StandardConfigurer<ITransport> configurer, IPostgresConnectionProvider connectionProvider, string tableName, TimeSpan? expiredMessagesCleanupInterval = null)
    {
        Configure(configurer, connectionProvider, tableName, null, expiredMessagesCleanupInterval);
        OneWayClientBackdoor.ConfigureOneWayClient(configurer);
    }

    static void Configure(StandardConfigurer<ITransport> configurer, IPostgresConnectionProvider connectionProvider, string tableName, string inputQueueName, TimeSpan? expiredMessagesCleanupInterval, string schemaName = null)
    {
        configurer.Register(context =>
        {
            var rebusLoggerFactory = context.Get<IRebusLoggerFactory>();
            var asyncTaskFactory = context.Get<IAsyncTaskFactory>();
            var rebusTime = context.Get<IRebusTime>();
            var transport = new PostgreSqlTransport(connectionProvider, tableName, inputQueueName, rebusLoggerFactory, asyncTaskFactory, rebusTime, expiredMessagesCleanupInterval, schemaName);
            transport.EnsureTableIsCreated();
            return transport;
        });

        configurer.OtherService<ITimeoutManager>().Register(c => new DisabledTimeoutManager());

        configurer.OtherService<IPipeline>().Decorate(c =>
        {
            var pipeline = c.Get<IPipeline>();

            return new PipelineStepRemover(pipeline)
                .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
        });
    }
}