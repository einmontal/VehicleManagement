using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System.Text;
using Microsoft.Extensions.Hosting;
using System.Text.Json;
using VehicleManagement.Application.Messages;
using VehicleManagement.Application.Events.Car.CreateOrUpdate;
using MediatR;

namespace VehicleManagement.Application.Consumers;

public class CarMessageConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly IChannel _channel;

    private const string ExchangeName = "car_exchange";
    private const string QueueName = "car_management_queue";

    public CarMessageConsumer(
        IServiceScopeFactory serviceScopeFactory,
        IChannel channel
        )
    {
        _serviceScopeFactory = serviceScopeFactory;

        _channel = channel;

        _channel.ExchangeDeclareAsync(
            exchange: ExchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false
            );

        _channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
            );

        _channel.QueueBindAsync(
            queue: QueueName,
            exchange: ExchangeName,
            routingKey: string.Empty
            );
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += async (obj, eventArgs) =>
        {
            try
            {
                var body = eventArgs.Body.ToArray();
                var stringMessage = Encoding.UTF8.GetString(body);

                var message = JsonSerializer.Deserialize<CarMessage>(stringMessage);
                if (message != null)
                {
                    using var scope = _serviceScopeFactory.CreateScope();
                    var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                    var carEvent = new CarCreateOrUpdateEvent
                    {
                        Id = message.Id,
                        Title = message.Title,
                        Gearbox = message.Gearbox,
                        IsActive = message.IsActive,
                        IsDeleted = message.IsDeleted,
                    };
                    await mediator.Publish(carEvent, stoppingToken);

                    await _channel.BasicAckAsync(
                        deliveryTag: eventArgs.DeliveryTag,
                        multiple: false,
                        cancellationToken: stoppingToken
                        );
                }
            }
            catch (Exception)
            {
                await _channel.BasicNackAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    requeue: true,
                    cancellationToken: stoppingToken
                    );
            }
        };

        await _channel.BasicConsumeAsync(
            queue: QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken
            );
    }
}
