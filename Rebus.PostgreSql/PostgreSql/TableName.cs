﻿using System;
using System.Text.RegularExpressions;

namespace Rebus.PostgreSql;

/// <summary>
/// Represents a (possibly schema-qualified) table name in PostgreSql Server
/// </summary>
public class TableName : IEquatable<TableName>
{
    /// <summary>
    /// Default schema name for postgres
    /// </summary>
    public const string DefaultSchemaName = "public";
    
    /// <summary>
    /// Gets the schema name of the table
    /// </summary>
    public string Schema { get; }

    /// <summary>
    /// Gets the table's name
    /// </summary>
    public string Name { get; }

    internal string QualifiedName => string.IsNullOrEmpty(Schema) ? $"\"{Name}\"" : $"\"{Schema}\".\"{Name}\"";

    /// <summary>
    /// Creates a <see cref="TableName"/> object with the given schema and table names
    /// </summary>
    public TableName(string tableName)
        : this(DefaultSchemaName, tableName)
    {
    }
    
    /// <summary>
    /// Creates a <see cref="TableName"/> object with the given schema and table names
    /// </summary>
    public TableName(string schema, string tableName)
    {
        if (schema == null) throw new ArgumentNullException(nameof(schema));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));

        Schema = StripQuotes(schema);
        Name = StripQuotes(tableName);
    }

    /// <summary>
    /// Parses the given name into a <see cref="TableName"/>, defaulting to using the 'dbo' schema unless the name is schema-qualified.
    /// E.g. 'table' will result in a <see cref="TableName"/> representing the '[dbo].[table]' table, whereas 'accounting.messages' will
    /// represent the '[accounting].[messages]' table.
    /// </summary>
    public static TableName Parse(string name)
    {
        // special case: bare table name, or schema and table name separated by . (but without any brackets)
        if (!(name.StartsWith("\"") && name.EndsWith("\"")))
        {
            var parts = name.Split('.');

            return TableNameFromParts(name, parts);
        }
        else
        {
            // name has [ and ] around it - we remove those
            var nameWithoutOutermostBrackets = name.Substring(1, name.Length - 2);

            // now the name either looks like this
            //   'name'
            // or like this 
            //   'schema].[name'
            // or even like this (because there can be spaces between the parts
            //   'schema]    .          [name'
            //
            // there we split with this regex
            var parts = Regex.Split(nameWithoutOutermostBrackets, "\"[ ]*\\.[ ]*\"", RegexOptions.Compiled);

            return TableNameFromParts(name, parts);
        }
    }

    static TableName TableNameFromParts(string name, string[] parts)
    {
        if (parts.Length == 1)
        {
            return new TableName(parts[0]);
        }

        if (parts.Length == 2)
        {
            return new TableName(parts[0], parts[1]);
        }

        throw new ArgumentException(
            $"The table name '{name}' cannot be used because it contained multiple '.' characters - if you intend to use '.' as part of a table name, please be sure to enclose the name in brackets, e.g. like this: '[Table name with spaces and .s]'");
    }

    static string StripQuotes(string value)
    {
        if (value.StartsWith("\""))
        {
            value = value.Substring(1);
        }
        if (value.EndsWith("\""))
        {
            value = value.Substring(0, value.Length - 1);
        }

        return value;
    }

    /// <inheritdoc />
    public override string ToString() => QualifiedName;

    /// <inheritdoc />
    public bool Equals(TableName other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return string.Equals(Schema, other.Schema, StringComparison.Ordinal)
               && string.Equals(Name, other.Name, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override bool Equals(object obj)
    {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((TableName)obj);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        unchecked
        {
            return (Schema.GetHashCode() * 397) ^ Name.GetHashCode();
        }
    }

    /// <summary>
    /// Checks whether the two <see cref="TableName"/> objects are equal (i.e. represent the same table)
    /// </summary>
    public static bool operator ==(TableName left, TableName right)
    {
        return Equals(left, right);
    }

    /// <summary>
    /// Checks whether the two <see cref="TableName"/> objects are not equal (i.e. do not represent the same table)
    /// </summary>
    public static bool operator !=(TableName left, TableName right)
    {
        return !Equals(left, right);
    }
}