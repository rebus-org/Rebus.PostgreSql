using Rebus.Config.Outbox;
using Rebus.Pipeline;
using Rebus.Transport;
using System;
using System.Threading.Tasks;
using System.Transactions;

namespace Rebus.PostgreSql.Outbox;

class OutboxOutgoingStep : IOutgoingStep
{
    readonly IOutboxConnectionProvider _outboxConnectionProvider;

    public OutboxOutgoingStep(IOutboxConnectionProvider outboxConnectionProvider)
    {
        _outboxConnectionProvider = outboxConnectionProvider;
    }

    public Task Process(OutgoingStepContext context, Func<Task> next)
    {
        var transactionContext = context.Load<ITransactionContext>();

        //in Rebus handler context, outbox is initialized by Incomming step
        //not in Rebus handler context and a rebus outbox transaction is explicitly (UseOutbox) created and outbox is initialized
        if (transactionContext.Items.ContainsKey(OutboxExtensions.CurrentOutboxConnectionKey))
        {
            //all is done
            return next();
        }

        //if an ambient transaction exists, create an oubox connection without transaction to connection enroll in current transaction
        if (Transaction.Current != null)
        {
            var outboxConnection = _outboxConnectionProvider.GetDbConnectionWithoutTransaction();
            transactionContext.Items[OutboxExtensions.CurrentOutboxConnectionKey] = outboxConnection;

            transactionContext.OnDisposed(_ =>
            {
                outboxConnection.Connection.Dispose();
            });
        }

        return next();
    }
}

