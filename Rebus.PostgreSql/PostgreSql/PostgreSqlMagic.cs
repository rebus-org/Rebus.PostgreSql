using System;
using System.Collections.Generic;
using Npgsql;
using NpgsqlTypes;

namespace Rebus.PostgreSql;

static class PostgreSqlMagic
{
    public static List<TableName> GetTableNames(this PostgresConnection connection)
    {
        using var command = connection.CreateCommand();
        return GetTableNames(command);
    }

    public static List<TableName> GetTableNames(this NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        
        return GetTableNames(command);
    }

    private static List<TableName> GetTableNames(NpgsqlCommand command)
    {
        var tableNames = new List<TableName>();
        command.CommandText = "select * from information_schema.tables where table_schema not in ('pg_catalog', 'information_schema')";

        using var reader = command.ExecuteReader();
        
        while (reader.Read())
        {
            var schemaName = reader["table_schema"].ToString();
            var tableName = reader["table_name"].ToString();

            tableNames.Add(new TableName(schemaName, tableName));
        }

        return tableNames;
    }
    
    public static List<string> GetSchemas(this PostgresConnection connection)
    {
        using var command = connection.CreateCommand();
        return GetSchemas(command);
    }

    public static List<string> GetSchemas(this NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        
        return GetSchemas(command);
    }

    private static List<string> GetSchemas(NpgsqlCommand command)
    {
        var schemaNames = new List<string>();
        command.CommandText = "SELECT schema_name FROM information_schema.schemata;";

        using var reader = command.ExecuteReader();
        
        while (reader.Read())
        {
            var schemaName = reader["schema_name"].ToString();

            schemaNames.Add(schemaName);
        }

        return schemaNames;
    }
    
    /// <summary>
    /// Gets the names of all tables in the current database
    /// </summary>
    public static Dictionary<string, NpgsqlDbType> GetColumns(this NpgsqlConnection connection, string schema, string tableName, NpgsqlTransaction transaction = null)
    {
        var results = new Dictionary<string, NpgsqlDbType>();

        using var command = connection.CreateCommand();
        if (transaction != null)
        {
            command.Transaction = transaction;
        }

        command.CommandText = $"SELECT [COLUMN_NAME] AS 'name', [DATA_TYPE] AS 'type' FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE [TABLE_SCHEMA] = '{schema}' AND [TABLE_NAME] = '{tableName}'";

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var name = (string)reader["name"];
            var typeString = (string)reader["type"];
            var type = GetDbType(typeString);

            results[name] = type;
        }

        return results;
    }

    private static NpgsqlDbType GetDbType(string typeString)
    {
        try
        {
            return (NpgsqlDbType)Enum.Parse(typeof(NpgsqlDbType), typeString, true);
        }
        catch (Exception exception)
        {
            throw new FormatException($"Could not parse '{typeString}' into {typeof(NpgsqlDbType)}", exception);
        }
    }
}