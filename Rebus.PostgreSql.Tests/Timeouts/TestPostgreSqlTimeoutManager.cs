using System;
using NUnit.Framework;
using Rebus.Logging;
using Rebus.PostgreSql.Timeouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Time;
using Rebus.Timeouts;

namespace Rebus.PostgreSql.Tests.Timeouts
{
    [TestFixture, Category(TestCategory.Postgres)]
    public class TestPostgreSqlTimeoutManager : BasicStoreAndRetrieveOperations<PostgreSqlTimeoutManagerFactory>
    {
    }

    public class PostgreSqlTimeoutManagerFactory : ITimeoutManagerFactory
    {
        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();

        public PostgreSqlTimeoutManagerFactory()
        {
            PostgreSqlTestHelper.DropTable("timeouts");
        }

        public ITimeoutManager Create()
        {
            var postgreSqlTimeoutManager = new PostgreSqlTimeoutManager(PostgreSqlTestHelper.ConnectionHelper, "timeouts", new ConsoleLoggerFactory(false), _fakeRebusTime);
            postgreSqlTimeoutManager.EnsureTableIsCreated();
            return postgreSqlTimeoutManager;
        }

        public void Cleanup()
        {
            PostgreSqlTestHelper.DropTable("timeouts");
        }

        public string GetDebugInfo()
        {
            return "could not provide debug info for this particular timeout manager.... implement if needed :)";
        }

        public void FakeIt(DateTimeOffset fakeTime)
        {
            _fakeRebusTime.SetNow(fakeTime);
        }

        class FakeRebusTime : IRebusTime
        {
            Func<DateTimeOffset> _nowFactory = () => DateTimeOffset.Now;

            public DateTimeOffset Now => _nowFactory();

            public void SetNow(DateTimeOffset fakeTime) => _nowFactory = () => fakeTime;
        }
    }
}