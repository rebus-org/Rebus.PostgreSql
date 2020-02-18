using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using NUnit.Framework;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.PostgreSql.Transport;
using Rebus.Tests.Contracts;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Transport;

namespace Rebus.PostgreSql.Tests.Bugs
{
    [TestFixture, Category(Categories.PostgreSql)]
    public class PublishWithinTransactionScopeTests : FixtureBase
    {
        readonly string _tableName = "messages" + TestConfig.Suffix;
        PostgreSqlTransport _transport;
        CancellationToken _cancellationToken;
        const string QueueName = "input";

        protected override void SetUp()
        {
            PostgreSqlTestHelper.DropTable(_tableName);
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var connectionHelper = new PostgresConnectionHelper(PostgreSqlTestHelper.ConnectionString);
            _transport = new PostgreSqlTransport(connectionHelper, _tableName, QueueName, consoleLoggerFactory, asyncTaskFactory);
            _transport.EnsureTableIsCreated();

            Using(_transport);

            _transport.Initialize();
            _cancellationToken = new CancellationTokenSource().Token;
        }

        [Test]
        public async Task ReceivesSentMessageWhenTransactionIsCommittedFromWithinAmbientDotNetTransactionScope()
        {
            using (var txScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var scope = new RebusTransactionScope())
                {
                    await _transport.Send(QueueName, RecognizableMessage(), scope.TransactionContext);

                    await scope.CompleteAsync();
                }
                txScope.Complete();
            }

            using (var scope = new RebusTransactionScope())
            {
                var transportMessage = await _transport.Receive(scope.TransactionContext, _cancellationToken);

                await scope.CompleteAsync();

                AssertMessageIsRecognized(transportMessage);
            }
        }

        void AssertMessageIsRecognized(TransportMessage transportMessage)
        {
            Assert.That(transportMessage.Headers.GetValue("recognizzle"), Is.EqualTo("hej"));
        }

        static TransportMessage RecognizableMessage(int id = 0)
        {
            var headers = new Dictionary<string, string>
            {
                {"recognizzle", "hej"},
                {"id", id.ToString()}
            };
            return new TransportMessage(headers, new byte[] { 1, 2, 3 });
        }
    }
}
