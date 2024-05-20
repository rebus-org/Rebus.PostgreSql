using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Testcontainers.PostgreSql;

namespace Rebus.PostgreSql.Tests;

[SetUpFixture]
public class PostgresTestContainerManager
{
    static PostgreSqlContainer _container;

    public static Lazy<string> TestContainerConnectionString = new(() =>
    {
        _container = new PostgreSqlBuilder().Build();

        _container.StartAsync().GetAwaiter().GetResult();

        return _container.GetConnectionString();
    });

    [OneTimeTearDown]
    public void StopContainer()
    {
        async Task StopAndDispose()
        {
            try
            {
                await _container.StopAsync();
            }
            finally
            {
                await _container.DisposeAsync();
                _container = null;
            }
        }

        Task.Run(StopAndDispose).GetAwaiter().GetResult();
    }
}