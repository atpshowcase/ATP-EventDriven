namespace CDC_Azure.Models
{
    public class mstOrder
    {
        public int id { get; set; }
        public string order_number { get; set; }
        public string customer_name { get; set; }
        public decimal total_amount { get; set; }
        public DateTime order_date { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
