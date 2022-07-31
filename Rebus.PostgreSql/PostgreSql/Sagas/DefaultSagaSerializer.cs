using Rebus.Serialization;

namespace Rebus.PostgreSql.Sagas;

/// <summary>
/// The default serializer for serializing sql saga data,
/// Implement <seealso cref="ISagaSerializer"/> to make your own custom serializer and register it using the UseSagaSerializer extension method.
/// <seealso cref="Rebus.Config.PostgresSagaConfigurationExtensions.UseSagaSerializer"/>
/// </summary>
public class DefaultSagaSerializer : ObjectSerializer, ISagaSerializer
{
}