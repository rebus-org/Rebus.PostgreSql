namespace Rebus.PostgreSql.Outbox;

interface IOutboxConnectionProvider
{
    OutboxConnection GetDbConnection();
}