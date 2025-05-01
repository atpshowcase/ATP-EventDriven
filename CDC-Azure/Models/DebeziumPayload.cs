using System.Text.Json;

namespace CDC_Azure.Models
{
    public class DebeziumPayload<T>
    {
        public Dictionary<string, object> before { get; set; }
        public Dictionary<string, object> after { get; set; }
        public SourceMetadata source { get; set; }
        public string op { get; set; }
        public long ts_ms { get; set; }
        public object transaction { get; set; }

        public T GetAfter()
        {
            var json = JsonSerializer.Serialize(after);
            return JsonSerializer.Deserialize<T>(json);
        }

        public T GetBefore()
        {
            var json = JsonSerializer.Serialize(before);
            return JsonSerializer.Deserialize<T>(json);
        }
    }

}
