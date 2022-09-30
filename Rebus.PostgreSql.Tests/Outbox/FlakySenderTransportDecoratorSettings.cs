namespace Rebus.PostgreSql.Tests.Outbox;

class FlakySenderTransportDecoratorSettings
{
    public double SuccessRate { get; set; } = 1;
}