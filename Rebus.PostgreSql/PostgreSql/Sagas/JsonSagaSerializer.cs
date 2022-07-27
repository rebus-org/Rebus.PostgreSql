using System;
using System.Text;
using Newtonsoft.Json;

namespace Rebus.PostgreSql.Sagas;

public class JsonSagaSerializer : ISagaSerializer
{
    readonly JsonSerializerSettings _settings;
    
    static readonly Encoding TextEncoding = Encoding.UTF8;

    static readonly JsonSerializerSettings DefaultSettings = new JsonSerializerSettings
    {
        TypeNameHandling = TypeNameHandling.All
    };
    
    public JsonSagaSerializer(JsonSerializerSettings jsonSerializerSettings = null)
    {
        _settings = jsonSerializerSettings ?? DefaultSettings;
    }
    /// <summary>
    /// Serializes the given object into a byte[]
    /// </summary>
    public byte[] Serialize(object obj)
    {
        var jsonString = SerializeToString(obj);

        return TextEncoding.GetBytes(jsonString);
    }

    /// <summary>
    /// Serializes the given object into a string
    /// </summary>
    public string SerializeToString(object obj)
    {
        return JsonConvert.SerializeObject(obj, _settings);
    }

    /// <summary>
    /// Deserializes the given byte[] into an object
    /// </summary>
    public object Deserialize(byte[] bytes)
    {
        var jsonString = TextEncoding.GetString(bytes);

        return DeserializeFromString(jsonString);
    }

    /// <summary>
    /// Deserializes the given string into an object
    /// </summary>
    public object DeserializeFromString(string str)
    {
        try
        {
            return JsonConvert.DeserializeObject(str, _settings);
        }
        catch (Exception exception)
        {
            throw new JsonSerializationException($"Could not deserialize '{str}'", exception);
        }
    }
}