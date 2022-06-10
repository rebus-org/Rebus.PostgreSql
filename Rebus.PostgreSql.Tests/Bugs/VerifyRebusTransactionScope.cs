using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.PostgreSql.Transport;
using Rebus.Tests.Contracts;
using Rebus.Transport;

#pragma warning disable 1998

namespace Rebus.PostgreSql.Tests.Bugs;

[TestFixture]
public class VerifyRebusTransactionScope : FixtureBase
{
    IBus _bus;
    PostgresConnectionCounter _counter;

    protected override void SetUp()
    {
        var activator = new BuiltinHandlerActivator();

        Using(activator);

        activator.Handle<string>(async _ => { });

        _counter = new PostgresConnectionCounter(
            new PostgresConnectionHelper(PostgreSqlTestHelper.ConnectionString));

        _bus = Configure.With(activator)
            .Transport(t => t.UsePostgreSql(_counter, "messages", "atomicity"))
            .Options(o => o.SetNumberOfWorkers(0))
            .Start();
    }

    [Test]
    public async Task CommitsMultipleSendsAsOneTransactionWhenUsingTransactionScope()
    {
        _counter.Reset();

        Assert.That(_counter.Connections, Is.EqualTo(0));

        using (var scope = new RebusTransactionScope())
        {
            await _bus.SendLocal("HEJ");
            await _bus.SendLocal("HEJ");
            await _bus.SendLocal("HEJ");
            await _bus.SendLocal("HEJ");
            await _bus.SendLocal("HEJ");

            await scope.CompleteAsync();
        }

        Assert.That(_counter.Connections, Is.EqualTo(1));
    }

    [Test]
    public async Task DoesNotCommitMultipleSendsAsOneTransactionWhenThereIsNoTransactionScope()
    {
        _counter.Reset();

        Assert.That(_counter.Connections, Is.EqualTo(0));

        await _bus.SendLocal("HEJ");
        await _bus.SendLocal("HEJ");
        await _bus.SendLocal("HEJ");
        await _bus.SendLocal("HEJ");
        await _bus.SendLocal("HEJ");

        Assert.That(_counter.Connections, Is.EqualTo(5));
    }

    class PostgresConnectionCounter : IPostgresConnectionProvider
    {
        readonly PostgresConnectionHelper _innerPostgresConnectionHelper;

        long _connections;

        public PostgresConnectionCounter(PostgresConnectionHelper innerPostgresConnectionHelper)
        {
            _innerPostgresConnectionHelper = innerPostgresConnectionHelper;
        }

        public Task<PostgresConnection> GetConnection()
        {
            Interlocked.Increment(ref _connections);
            return _innerPostgresConnectionHelper.GetConnection();
        }

        public void Reset() => Interlocked.Exchange(ref _connections, 0);

        public long Connections => Interlocked.Read(ref _connections);
    }
}