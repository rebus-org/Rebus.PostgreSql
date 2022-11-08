using System;

namespace Rebus.PostgreSql.Tests.Outbox;

class RandomUnluckyException : ApplicationException
{
    public RandomUnluckyException() : base("You were unfortunate")
    {
    }
}