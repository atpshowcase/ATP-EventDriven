using Microsoft.Extensions.Logging;
using CDC_Azure.Consumers;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading;

internal class Program
{
    public Program()
    {
    }

    private static async Task Main(string[] args)
    {
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole(); 
        });
        
        var logger = loggerFactory.CreateLogger<OrderConsumer>();

        Debug.WriteLine(message: $"Starting Kafka Consumer...");
        System.Diagnostics.Debug.WriteLine($"Order Received:");

        
        var orderConsumer = new OrderConsumer(logger);
        var tenantConsumer = new TenantConsumer(); 

        
        var cancellationTokenSource = new CancellationTokenSource();

        
        Task task1 = Task.Run(() => orderConsumer.Start(cancellationTokenSource.Token));
        Task task2 = Task.Run(() => tenantConsumer.Start(cancellationTokenSource.Token));

        
        try
        {
            await Task.WhenAll(task1, task2);
        }
        catch (OperationCanceledException)
        {
            Debug.WriteLine("Consumer tasks were cancelled.");
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Error occurred: {ex.Message}");
        }

        
        Console.WriteLine("Press Enter to stop the consumer...");
        Console.ReadLine();

        
        cancellationTokenSource.Cancel(); 

        
        try
        {
            await Task.WhenAll(task1, task2);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Error occurred after cancellation: {ex.Message}");
        }
    }
}
