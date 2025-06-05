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
        public OrderConsumer(ILogger<OrderConsumer> logger) : base("fullfillment.TBiGSys.dbo.mstOrder")
        {
            _logger = logger;
        }

        // Update HandleMessage to include logging
        protected override async Task HandleMessage(mstOrder data)
        {
            _logger.LogInformation($"Processing Order: {data.ID}");  // Log information message

            try
            {
                // Trigger Airflow
                var trigger = new AirflowTrigger();
                await trigger.TriggerDagAsync("dag_order_process", new
                {
                    orderId = data.ID,
                    customerName = data.SONumber
                    //total = data.Total
                });

                _logger.LogInformation($"Successfully triggered Airflow for Order {data.ID} - {data.SONumber}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error occurred while processing Order {data.ID}: {ex.Message}");
                throw; // Rethrow the exception if you want it to be handled elsewhere
            }
        }
    }
}
