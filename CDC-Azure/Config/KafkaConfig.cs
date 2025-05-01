namespace CDC_Azure.Config
{
    public static class KafkaConfig
    {
        public static string BootstrapServers => Environment.GetEnvironmentVariable("KafkaBootstrapServers");
        public static string GroupId => Environment.GetEnvironmentVariable("KafkaGroupId");
    }
}
