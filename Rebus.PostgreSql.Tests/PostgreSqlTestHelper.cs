using System;
using Npgsql;
using Rebus.Tests.Contracts;

namespace Rebus.PostgreSql.Tests
{
    public class PostgreSqlTestHelper
    {
        static readonly IPostgresConnectionProvider PostgresConnectionHelper = new PostgresConnectionHelper(ConnectionString);
        
        const string TableDoesNotExist = "42P01";

        public static string DatabaseName => $"rebus2_test_{TestConfig.Suffix}".TrimEnd('_');

        public static string ConnectionString => GetConnectionStringForDatabase(DatabaseName);

        public static IPostgresConnectionProvider ConnectionHelper => PostgresConnectionHelper;

        public static void DropTable(string tableName)
        {
            try
            {
                using (var connection = PostgresConnectionHelper.GetConnection().Result)
                {
                    using (var comand = connection.CreateCommand())
                    {
                        comand.CommandText = $@"drop table ""{tableName}"";";

                        try
                        {
                            comand.ExecuteNonQuery();

                            Console.WriteLine("Dropped postgres table '{0}'", tableName);
                        }
                        catch (PostgresException exception) when (exception.SqlState == TableDoesNotExist)
                        {
                        }
                    }

                    connection.Complete().Wait();
                }
            }
            catch (Exception exception)
            {
                throw new ApplicationException($"Could not drop table '{tableName}'", exception);
            }
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return Environment.GetEnvironmentVariable("REBUS_POSTGRES")
                   ?? $"server=localhost; database={databaseName}; user id=postgres; password=postgres;maximum pool size=30;";
        }
    }
}