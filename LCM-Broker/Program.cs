using Microsoft.Data.SqlClient;
using MQTTnet;
using MQTTnet.Server;
using System.Net;
using System.Text;
using System.Text.Json;

// Configuração do Broker
var mqttFactory = new MqttFactory();
var mqttServerOptions = new MqttServerOptionsBuilder()
    .WithDefaultEndpoint()
    .WithDefaultEndpointPort(1883)
    .Build();

var broker = mqttFactory.CreateMqttServer(mqttServerOptions);

// Eventos do Broker
broker.ValidatingConnectionAsync += HandleConnectionValidation;
broker.ClientConnectedAsync += HandleClientConnected;
broker.InterceptingPublishAsync += HandleMessageReceived;

try
{
    await broker.StartAsync();
    Console.WriteLine($"🚀 Broker ativo em tcp://{IPAddress.Any}:1883");
    Console.WriteLine("Pressione ENTER para encerrar...");
    Console.ReadLine();
}
finally
{
    await broker.StopAsync();
    Console.WriteLine("🔌 Broker desligado");
}

// Métodos auxiliares
Task HandleConnectionValidation(ValidatingConnectionEventArgs e)
{
    Console.WriteLine($"🔌 Conexão solicitada - VIN: {e.ClientId}");
    e.ReasonCode = MQTTnet.Protocol.MqttConnectReasonCode.Success;
    return Task.CompletedTask;
}

Task HandleClientConnected(ClientConnectedEventArgs e)
{
    Console.WriteLine($"✅ [{DateTime.Now:HH:mm:ss}] Cliente conectado: {e.ClientId}");
    return Task.CompletedTask;
}

Task HandleMessageReceived(InterceptingPublishEventArgs e)
{
    try
    {
        var vin = e.ClientId;
        var topic = e.ApplicationMessage.Topic;
        var payloadStr = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

        switch (topic)
        {
            case "motas/telemetria":
                var telemetria = JsonSerializer.Deserialize<MotaTelemetria>(payloadStr);
                Console.WriteLine($"\n🏍️ [Telemetria] VIN: {vin}");
                Console.WriteLine($"🔋 Bateria: {telemetria.Battery}%");
                Console.WriteLine($"🛣️ Km: {telemetria.Kilometers}");
                Console.WriteLine($"📍 Local: {telemetria.Latitude}, {telemetria.Longitude}");

                // Guardar na BD
                _ = GuardarTelemetriaNaBD(vin, telemetria);
                break;

            case "motas/alerta":
                Console.WriteLine($"\n🚨 [Alerta] VIN: {vin} - Mensagem: {payloadStr}");
                break;

            default:
                Console.WriteLine($"\n📩 [Outro Tópico] {topic} - Mensagem: {payloadStr}");
                break;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"⚠️ Erro ao processar mensagem: {ex.Message}");
    }

    return Task.CompletedTask;
}


// Inserir dados no SQL Server
async Task GuardarTelemetriaNaBD(string vin, MotaTelemetria dados)
{
    try
    {
        var connectionString = "Server=172.20.0.16,1433;Database=MotasDB;User Id=sa;Password=Fulgora3000;TrustServerCertificate=True;";
        var query = @"
            INSERT INTO Motas (VIN, Battery, Kilometers, Latitude, Longitude)
            VALUES (@VIN, @Battery, @Kilometers, @Latitude, @Longitude)";

        using var connection = new SqlConnection(connectionString);
        using var command = new SqlCommand(query, connection);

        command.Parameters.AddWithValue("@VIN", vin);
        command.Parameters.AddWithValue("@Battery", dados.Battery);
        command.Parameters.AddWithValue("@Kilometers", dados.Kilometers);
        command.Parameters.AddWithValue("@Latitude", dados.Latitude);
        command.Parameters.AddWithValue("@Longitude", dados.Longitude);

        await connection.OpenAsync();
        await command.ExecuteNonQueryAsync();

        Console.WriteLine("💾 Dados inseridos na base de dados com sucesso.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Erro ao inserir na base de dados: {ex.Message}");
    }
}

// Classe para deserialização
public class MotaTelemetria
{
    public int Battery { get; set; }
    public int Kilometers { get; set; }
    public double Latitude { get; set; }
    public double Longitude { get; set; }
}
