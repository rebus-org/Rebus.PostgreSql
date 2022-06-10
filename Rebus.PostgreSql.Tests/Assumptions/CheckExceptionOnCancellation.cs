using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NUnit.Framework;
using Rebus.Tests.Contracts;

namespace Rebus.PostgreSql.Tests.Assumptions;

[TestFixture]
public class CheckExceptionOnCancellation : FixtureBase
{
    [Test]
    [Description("Checks that the right exception is thrown by Npgsql when operation is cancelled")]
    public async Task VerifyItIsAnOrdinaryOperationCancelledException()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;
        await using var connection = new NpgsqlConnection(PostgreSqlTestHelper.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = @"select table_name from information_schema.tables";

        cancellationTokenSource.Cancel();

        var exception = Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                Console.WriteLine($"{reader["table_name"]}");
            }
        });

        Console.WriteLine(exception);
    }
}