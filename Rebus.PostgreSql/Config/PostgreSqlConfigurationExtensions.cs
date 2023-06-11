﻿using Rebus.Auditing.Sagas;
using System;
using Npgsql;
using Rebus.Logging;
using Rebus.PostgreSql;
using Rebus.PostgreSql.Sagas;
using Rebus.PostgreSql.Subscriptions;
using Rebus.PostgreSql.Timeouts;
using Rebus.Sagas;
using Rebus.Subscriptions;
using Rebus.Time;
using Rebus.Timeouts;
// ReSharper disable UnusedMember.Global

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for Postgres persistence
/// </summary>
public static class PostgreSqlConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to use PostgreSql to store saga data snapshots, using the specified table to store the data
    /// </summary>
    public static void StoreInPostgres(this StandardConfigurer<ISagaSnapshotStorage> configurer, string connectionString, string tableName, bool automaticallyCreateTables = true, Action<NpgsqlConnection> additionalConnectionSetup = null)
    {
        var provider = new PostgresConnectionHelper(connectionString, additionalConnectionSetup);
            
        StoreInPostgres(configurer, provider, tableName, automaticallyCreateTables);
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql to store saga data snapshots, using the specified table to store the data
    /// </summary>
    public static void StoreInPostgres(this StandardConfigurer<ISagaSnapshotStorage> configurer, IPostgresConnectionProvider connectionProvider, string tableName, bool automaticallyCreateTables = true, string schemaName = null)
    {
        configurer.Register(c =>
        {
            var sagaStorage = new PostgreSqlSagaSnapshotStorage(connectionProvider, tableName, schemaName);

            if (automaticallyCreateTables)
            {
                sagaStorage.EnsureTableIsCreated();
            }

            return sagaStorage;
        });
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql to store sagas, using the tables specified to store data and indexed properties respectively.
    /// </summary>
    public static void StoreInPostgres(this StandardConfigurer<ISagaStorage> configurer, string connectionString, string dataTableName, string indexTableName, bool automaticallyCreateTables = true, Action<NpgsqlConnection> additionalConnectionSetup = null, string schemaName = null)
    {
        var provider = new PostgresConnectionHelper(connectionString, additionalConnectionSetup);
            
        StoreInPostgres(configurer, provider, dataTableName, indexTableName, automaticallyCreateTables, schemaName);
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql to store sagas, using the tables specified to store data and indexed properties respectively.
    /// </summary>
    public static void StoreInPostgres(this StandardConfigurer<ISagaStorage> configurer, IPostgresConnectionProvider connectionProvider, string dataTableName, string indexTableName, bool automaticallyCreateTables = true, string schemaName = null)
    {
        configurer.Register(c =>
        {
            var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
            var serializer = c.Has<ISagaSerializer>(false) ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

            var sagaStorage = new PostgreSqlSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory, serializer, schemaName);

            if (automaticallyCreateTables)
            {
                sagaStorage.EnsureTablesAreCreated();
            }

            return sagaStorage;
        });
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql to store timeouts.
    /// </summary>
    public static void StoreInPostgres(this StandardConfigurer<ITimeoutManager> configurer, string connectionString, string tableName, bool automaticallyCreateTables = true, Action<NpgsqlConnection> additionalConnectionSetup = null)
    {
        var provider = new PostgresConnectionHelper(connectionString, additionalConnectionSetup);

        StoreInPostgres(configurer, provider, tableName, automaticallyCreateTables);
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql to store timeouts.
    /// </summary>
    public static void StoreInPostgres(this StandardConfigurer<ITimeoutManager> configurer, IPostgresConnectionProvider connectionProvider, string tableName, bool automaticallyCreateTables = true, string schemaName = null)
    {
        configurer.Register(c =>
        {
            var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
            var rebusTime = c.Get<IRebusTime>();
            var subscriptionStorage = new PostgreSqlTimeoutManager(connectionProvider, tableName, rebusLoggerFactory, rebusTime, schemaName);

            if (automaticallyCreateTables)
            {
                subscriptionStorage.EnsureTableIsCreated();
            }

            return subscriptionStorage;
        });
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
    /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
    /// default behavior.
    /// </summary>
    public static void StoreInPostgres(this StandardConfigurer<ISubscriptionStorage> configurer, string connectionString, string tableName, bool isCentralized = false, bool automaticallyCreateTables = true, Action<NpgsqlConnection> additionalConnectionSetup = null)
    {
        var provider = new PostgresConnectionHelper(connectionString, additionalConnectionSetup);

        StoreInPostgres(configurer, provider, tableName, isCentralized, automaticallyCreateTables);
    }

    /// <summary>
    /// Configures Rebus to use PostgreSql to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
    /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
    /// default behavior.
    /// </summary>
    public static void StoreInPostgres(this StandardConfigurer<ISubscriptionStorage> configurer, IPostgresConnectionProvider connectionProvider, string tableName, bool isCentralized = false, bool automaticallyCreateTables = true, string schemaName = null)
    {
        configurer.Register(c =>
        {
            var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
            var subscriptionStorage = new PostgreSqlSubscriptionStorage(connectionProvider, tableName, isCentralized, rebusLoggerFactory, schemaName);

            if (automaticallyCreateTables)
            {
                subscriptionStorage.EnsureTableIsCreated();
            }

            return subscriptionStorage;
        });
    }

    public static void UseSagaSerializer(this StandardConfigurer<ISagaStorage> configurer, ISagaSerializer serializer = null)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (serializer == null)
        {
            serializer = new DefaultSagaSerializer();
        }

        configurer.OtherService<ISagaSerializer>().Decorate((c) => serializer);
    }
}