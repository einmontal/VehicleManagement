using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using VehicleManagement.Application.Messages;

namespace VehicleManagement.Application.Publishers;

public class CarMessagePublisher
{
    private readonly IChannel _channel;

    private const string ExchangeName = "car_exchange";

    public CarMessagePublisher(IChannel channel)
    {
        _channel = channel;

        _channel.ExchangeDeclareAsync(
            exchange: ExchangeName, 
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false
            );
    }

    public async Task PublishMessageAsync(CarMessage message, CancellationToken cancellationToken)
    {
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        await _channel.BasicPublishAsync(
            exchange: ExchangeName, 
            routingKey: string.Empty,
            body: body,
            cancellationToken: cancellationToken
            );
    }
}
