using System;
using CDC_Azure.Helpers;
using CDC_Azure.Models;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;  // Add this namespace for logging

namespace CDC_Azure.Consumers
{
    public class OrderConsumer : BaseConsumer<mstOrder>
    {
        private readonly ILogger<OrderConsumer> _logger;

        // Inject ILogger into the constructor
        public OrderConsumer(ILogger<OrderConsumer> logger) : base("fullfillment.Test_DB.dbo.mstOrder")
        {
            _logger = logger;
        }

        // Update HandleMessage to include logging
        protected override async Task HandleMessage(mstOrder data)
        {
            _logger.LogInformation($"Processing Order: {data.id}");  // Log information message

            try
            {
                // Trigger Airflow
                var trigger = new AirflowTrigger();
                await trigger.TriggerDagAsync("order_process", new
                {
                    orderId = data.id,
                    customerName = data.customer_name
                    //total = data.Total
                });

                _logger.LogInformation($"Successfully triggered Airflow for Order {data.id} - {data.customer_name}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error occurred while processing Order {data.id}: {ex.Message}");
                throw; // Rethrow the exception if you want it to be handled elsewhere
            }
        }
    }
}
