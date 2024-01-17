using System;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Npgsql;

class Program
{
    static void Main()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        // Declare queues
        channel.QueueDeclare(queue: "OrderQueue", durable: true, exclusive: false, autoDelete: false);
        channel.QueueDeclare(queue: "PaymentQueue", durable: true, exclusive: false, autoDelete: false);

        // Declare exchanges
        channel.ExchangeDeclare(exchange: "direct_exchange", type: ExchangeType.Direct);

        // Bind queues to exchanges
        channel.QueueBind(queue: "OrderQueue", exchange: "direct_exchange", routingKey: "OrderEvent");
        channel.QueueBind(queue: "PaymentQueue", exchange: "direct_exchange", routingKey: "PaymentEvent");

        // Set up consumer for OrderQueue
        var orderConsumer = new EventingBasicConsumer(channel);
        orderConsumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            var orderEvent = JsonSerializer.Deserialize<OrderEvent>(message);

            // Store order in PostgreSQL DB
            StoreOrderInPostgres(orderEvent);

            channel.BasicAck(ea.DeliveryTag, false);
        };
        channel.BasicConsume(queue: "OrderQueue", autoAck: false, consumer: orderConsumer);

        // Set up consumer for PaymentQueue
        var paymentConsumer = new EventingBasicConsumer(channel);
        paymentConsumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            var paymentEvent = JsonSerializer.Deserialize<PaymentEvent>(message);
            // Check if related order records are fully paid
            CheckAndPrintPaymentStatus(paymentEvent);

            channel.BasicAck(ea.DeliveryTag, false);
        };
        channel.BasicConsume(queue: "PaymentQueue", autoAck: false, consumer: paymentConsumer);

        Console.WriteLine("Press [enter] to exit.");
        Console.ReadLine();
    }

    static void StoreOrderInPostgres(OrderEvent orderEvent)
    {
        // Implement code to store order in PostgreSQL DB
        using (var connection = new NpgsqlConnection("OrderEvent"))
        {
            connection.Open();

            using (var cmd = new NpgsqlCommand())
            {
                cmd.Connection = connection;
                cmd.CommandText = "INSERT INTO Orders (OrderId, ProductId, TotalAmount) VALUES (@OrderId, @ProductId, @TotalAmount)";
                cmd.Parameters.AddWithValue("@OrderId", orderEvent.OrderId);
                cmd.Parameters.AddWithValue("@ProductId", orderEvent.ProductId);
                cmd.Parameters.AddWithValue("@TotalAmount", orderEvent.TotalAmount);
                cmd.Parameters.AddWithValue("@Currency", orderEvent.Currency);

                cmd.ExecuteNonQuery();
            }
        }

        Console.WriteLine($"Order record created: {orderEvent.OrderId}");
    }

    static void CheckAndPrintPaymentStatus(PaymentEvent paymentEvent)
    {
        // Implement code to check if related order records are fully paid and print the payment status
        using (var connection = new NpgsqlConnection("OrderEvent"))
        {
            connection.Open();

            using (var cmd = new NpgsqlCommand())
            {
                cmd.Connection = connection;
                cmd.CommandText = "SELECT * FROM Orders WHERE OrderId = @OrderId";
                cmd.Parameters.AddWithValue("@OrderId", paymentEvent.OrderId);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        var orderId = reader["OrderId"].ToString();
                        var productId = reader["ProductId"].ToString();
                        var totalAmount = Convert.ToDecimal(reader["TotalAmount"]);
                        var currency = reader["Currency"].ToString();
                        var status = Convert.ToBoolean(reader["IsPaid"]) ? "PAID" : "UNPAID";

                        Console.WriteLine($"Order: {orderId}, Product: {productId}, Total: {totalAmount} {currency}, Status: {status}");
                    }
                }
            }
        }
    }
}

// Define OrderEvent and PaymentEvent classes
class OrderEvent
{
    public string OrderId { get; set; }
    public string ProductId { get; set; }
    public decimal TotalAmount { get; set; }
    public string Currency { get; set; }
}

class PaymentEvent
{
    public string OrderId { get; set; }
    public decimal PaymentAmount { get; set; }
}
