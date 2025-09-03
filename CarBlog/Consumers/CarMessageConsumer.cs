using CarBlog.Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace CarBlog.Consumers;

public class CarMessageConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly IChannel _channel;

    private const string ExchangeName = "car_exchange";
    private const string QueueName = "car_blog_queue";

    public CarMessageConsumer(IServiceScopeFactory serviceScopeFactory, IChannel channel)
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
                    var db = scope.ServiceProvider.GetRequiredService<CarBlogDBContext>();

                    var entity = await db.Cars.FindAsync([message.Id], stoppingToken);
                    if (entity is null)
                    {
                        entity = new Car
                        {
                            Id = message.Id,
                            Title = message.Title,
                            Gearbox = message.Gearbox,
                            IsActive = message.IsActive,
                        };

                        await db.Cars.AddAsync(entity, stoppingToken);
                    }
                    else
                    {
                        if (message.IsDeleted)
                        {
                            db.Remove(entity);
                        }
                        else
                        {
                            entity.Title = message.Title;
                            entity.Gearbox = message.Gearbox;
                            entity.IsActive = message.IsActive;

                            db.Update(entity);
                        }
                    }
                    await db.SaveChangesAsync(stoppingToken);
                }

                await _channel.BasicAckAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    cancellationToken: stoppingToken
                    );
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
