using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Tests.Contracts.Extensions;
using Testcontainers.PostgreSql;

namespace Rebus.PostgreSql.Tests;

[SetUpFixture]
public class PostgresTestContainerManager
{
    static PostgreSqlContainer _container;

    public static Lazy<string> TestContainerConnectionString = new(() =>
    {
        _container = new PostgreSqlBuilder().Build();

        ExceptionDispatchInfo exceptionDispatchInfo = null;

        using var done = new ManualResetEvent(initialState: false);

        Task.Run(async () =>
        {
            try
            {
                await _container.StartAsync();
            }
            catch (Exception exception)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(exception);
            }
            finally
            {
                done.Set();
            }
        });

        done.WaitOrDie(TimeSpan.FromMinutes(1), "PostgreSQL container did not start within 1 minute");
        exceptionDispatchInfo?.Throw();

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