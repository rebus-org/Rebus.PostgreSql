using System;

// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.PostgreSql;

static class MathExtensions
{
    public static int RoundUpToNextPowerOfTwo(this int number) => 1 << (int)Math.Ceiling(Math.Log(number, newBase: 2));
}
