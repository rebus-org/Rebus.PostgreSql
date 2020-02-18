using System;
using System.Collections.Generic;
using NUnit.Framework;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.PostgreSql.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.PostgreSql.Tests.Transport
{
    public class PostgreSqlTransportFactory : ITransportFactory
    {

         readonly HashSet<string> _tablesToDrop = new HashSet<string>();
        readonly List<IDisposable> _disposables = new List<IDisposable>();


        [TestFixture, Category(Categories.PostgreSql)]
        public class PostgreSqlTransportBasicSendReceive : BasicSendReceive<PostgreSqlTransportFactory> { }

        [TestFixture, Category(Categories.PostgreSql)]
        public class PostgreSqlTransportMessageExpiration : MessageExpiration<PostgreSqlTransportFactory> { }


        public ITransport CreateOneWayClient()
        {
            var tableName = ("rebus_messages_" + TestConfig.Suffix).TrimEnd('_');
             _tablesToDrop.Add(tableName);

            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionHelper = new PostgresConnectionHelper(PostgreSqlTestHelper.ConnectionString);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new PostgreSqlTransport(connectionHelper, tableName, null, consoleLoggerFactory, asyncTaskFactory, new DefaultRebusTime());

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public ITransport Create(string inputQueueAddress)
        {
            var tableName = ("rebus_messages_" + TestConfig.Suffix).TrimEnd('_');

            _tablesToDrop.Add(tableName);

            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionHelper = new PostgresConnectionHelper(PostgreSqlTestHelper.ConnectionString);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new PostgreSqlTransport(connectionHelper, tableName, inputQueueAddress, consoleLoggerFactory, asyncTaskFactory, new DefaultRebusTime());

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public void CleanUp()
        {
            _disposables.ForEach(d => d.Dispose());
            _disposables.Clear();

            foreach (var tableName in _tablesToDrop)
            {
                PostgreSqlTestHelper.DropTable(tableName);
            }

            _tablesToDrop.Clear();
        }
    }
}
