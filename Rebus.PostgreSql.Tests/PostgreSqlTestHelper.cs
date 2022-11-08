using System;
using Npgsql;
using Rebus.Internals;
using Rebus.Tests.Contracts;

namespace Rebus.PostgreSql.Tests;

public class PostgreSqlTestHelper
{
    const string TableDoesNotExist = "42P01";
    static readonly IPostgresConnectionProvider PostgresConnectionHelper = new PostgresConnectionHelper(ConnectionString);

    public static string DatabaseName => $"rebus2_test_{TestConfig.Suffix}".TrimEnd('_');

    public static string ConnectionString => GetConnectionStringForDatabase(DatabaseName);

    public static IPostgresConnectionProvider ConnectionHelper => PostgresConnectionHelper;

    public static void DropAllTables()
    {
        AsyncHelpers.RunSync(async () =>
        {
            using var connection = await PostgresConnectionHelper.GetConnection();
            var tables = connection.GetTableNames();

            foreach (var table in tables)
            {
                try
                {
                    await using var command = connection.CreateCommand();
                    command.CommandText = $@"DROP TABLE {table}";
                    command.ExecuteNonQuery();

                    Console.WriteLine("Dropped postgres table '{0}'", table);
                }
                catch (PostgresException exception) when (exception.SqlState == TableDoesNotExist)
                {
                }
            }

            await connection.Complete();
        });
    }

    public static void DropTable(string tableName)
    {
        AsyncHelpers.RunSync(async () =>
        {
            using var connection = await PostgresConnectionHelper.GetConnection();

            await using var command = connection.CreateCommand();

            command.CommandText = $@"drop table ""{tableName}"";";

            try
            {
                command.ExecuteNonQuery();

                Console.WriteLine("Dropped postgres table '{0}'", tableName);
            }
            catch (PostgresException exception) when (exception.SqlState == TableDoesNotExist)
            {
            }

            await connection.Complete();
        });
    }

    static string GetConnectionStringForDatabase(string databaseName)
    {
        return Environment.GetEnvironmentVariable("REBUS_POSTGRES")
               ?? $"server=localhost; database={databaseName}; user id=postgres; password=postgres;maximum pool size=30;";
    }
}