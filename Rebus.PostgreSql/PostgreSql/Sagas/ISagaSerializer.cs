namespace Rebus.PostgreSql.Sagas;

public interface ISagaSerializer
{
    /// <summary>
    /// Serializes the given object into a byte[]
    /// </summary>
    byte[] Serialize(object obj);

    /// <summary>
    /// Serializes the given object into a string
    /// </summary>
    string SerializeToString(object obj);

    /// <summary>
    /// Deserializes the given byte[] into an object
    /// </summary>
    object Deserialize(byte[] bytes);

    /// <summary>
    /// Deserializes the given string into an object
    /// </summary>
    object DeserializeFromString(string str);
}