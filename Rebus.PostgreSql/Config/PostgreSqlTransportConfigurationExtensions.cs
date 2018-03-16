using System;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Threading;
using Rebus.Timeouts;
using Rebus.Transport;

namespace Rebus.PostgreSql.Transport
{
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
        public static void UsePostgreSql(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionOrConnectionStringName, string tableName, string inputQueueName)
        {
            UsePostgreSql(configurer,  new PostgresConnectionHelper(connectionStringOrConnectionOrConnectionStringName), tableName, inputQueueName);
        }

        /// <summary>
        /// Configures Rebus to use PostgreSql as its transport. The table specified by <paramref name="tableName"/> will be used to
        /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UsePostgreSql(this StandardConfigurer<ITransport> configurer, IPostgresConnectionProvider connectionProvider, string tableName, string inputQueueName)
        {
            Configure(configurer, connectionProvider, tableName, inputQueueName);
        }

        /// <summary>
        /// Configures Rebus to use PostgreSql to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The table specified by <paramref name="tableName"/> will be used to store messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UsePostgreSqlAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionStringName, string tableName)
        {
            UsePostgreSqlAsOneWayClient(configurer, new PostgresConnectionHelper(connectionStringOrConnectionStringName), tableName);
        }

        /// <summary>
        /// Configures Rebus to use PostgreSql to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The table specified by <paramref name="tableName"/> will be used to store messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UsePostgreSqlAsOneWayClient(this StandardConfigurer<ITransport> configurer, IPostgresConnectionProvider  connectionProvider, string tableName)
        {
            Configure(configurer, connectionProvider, tableName, null);
            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        static void Configure(StandardConfigurer<ITransport> configurer, IPostgresConnectionProvider connectionProvider, string tableName, string inputQueueName)
        {
            configurer.Register(context =>
            {
                var rebusLoggerFactory = context.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = context.Get<IAsyncTaskFactory>();
                var transport = new PostgreSqlTransport(connectionProvider, tableName, inputQueueName, rebusLoggerFactory, asyncTaskFactory);
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
}
