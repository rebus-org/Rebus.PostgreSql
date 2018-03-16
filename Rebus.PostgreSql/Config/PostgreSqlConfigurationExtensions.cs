using Rebus.Auditing.Sagas;
using System;
using Npgsql;
using Rebus.Logging;
using Rebus.PostgreSql;
using Rebus.PostgreSql.Sagas;
using Rebus.PostgreSql.Subscriptions;
using Rebus.PostgreSql.Timeouts;
using Rebus.Sagas;
using Rebus.Subscriptions;
using Rebus.Timeouts;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for Postgres persistence
    /// </summary>
    public static class PostgreSqlConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use PostgreSQL to store saga data snapshots, using the specified table to store the data
        /// </summary>
        public static void StoreInPostgres(this StandardConfigurer<ISagaSnapshotStorage> configurer, string connectionString, string tableName, bool automaticallyCreateTables = true, Action<NpgsqlConnection> additionalConnectionSetup = null)
        {
            var provider = new PostgresConnectionHelper(connectionString, additionalConnectionSetup);
            
            StoreInPostgres(configurer, provider, tableName, automaticallyCreateTables);
        }

        /// <summary>
        /// Configures Rebus to use PostgreSQL to store saga data snapshots, using the specified table to store the data
        /// </summary>
        public static void StoreInPostgres(this StandardConfigurer<ISagaSnapshotStorage> configurer, IPostgresConnectionProvider connectionProvider, string tableName, bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var sagaStorage = new PostgreSqlSagaSnapshotStorage(connectionProvider, tableName);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTableIsCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use PostgreSQL to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInPostgres(this StandardConfigurer<ISagaStorage> configurer, string connectionString, string dataTableName, string indexTableName, bool automaticallyCreateTables = true, Action<NpgsqlConnection> additionalConnectionSetup = null)
        {
            var provider = new PostgresConnectionHelper(connectionString, additionalConnectionSetup);
            
            StoreInPostgres(configurer, provider, dataTableName, indexTableName, automaticallyCreateTables);
        }

        /// <summary>
        /// Configures Rebus to use PostgreSQL to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInPostgres(this StandardConfigurer<ISagaStorage> configurer, IPostgresConnectionProvider connectionProvider, string dataTableName, string indexTableName, bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var sagaStorage = new PostgreSqlSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use PostgreSQL to store timeouts.
        /// </summary>
        public static void StoreInPostgres(this StandardConfigurer<ITimeoutManager> configurer, string connectionString, string tableName, bool automaticallyCreateTables = true, Action<NpgsqlConnection> additionalConnectionSetup = null)
        {
            var provider = new PostgresConnectionHelper(connectionString, additionalConnectionSetup);

            StoreInPostgres(configurer, provider, tableName, automaticallyCreateTables);
        }

        /// <summary>
        /// Configures Rebus to use PostgreSQL to store timeouts.
        /// </summary>
        public static void StoreInPostgres(this StandardConfigurer<ITimeoutManager> configurer, IPostgresConnectionProvider connectionProvider, string tableName, bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var subscriptionStorage = new PostgreSqlTimeoutManager(connectionProvider, tableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use PostgreSQL to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
        /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
        /// default behavior.
        /// </summary>
        public static void StoreInPostgres(this StandardConfigurer<ISubscriptionStorage> configurer, string connectionString, string tableName, bool isCentralized = false, bool automaticallyCreateTables = true, Action<NpgsqlConnection> additionalConnectionSetup = null)
        {
            var provider = new PostgresConnectionHelper(connectionString, additionalConnectionSetup);

            StoreInPostgres(configurer, provider, tableName, automaticallyCreateTables);
        }

        /// <summary>
        /// Configures Rebus to use PostgreSQL to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
        /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
        /// default behavior.
        /// </summary>
        public static void StoreInPostgres(this StandardConfigurer<ISubscriptionStorage> configurer, IPostgresConnectionProvider connectionProvider, string tableName, bool isCentralized = false, bool automaticallyCreateTables = true)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var subscriptionStorage = new PostgreSqlSubscriptionStorage(connectionProvider, tableName, isCentralized, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }

    }
}