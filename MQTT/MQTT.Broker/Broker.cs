using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MQTT.Broker
{
    public class Broker
    {
        IMqttServer _mqttServer;
        MqttServerOptionsBuilder _mqttServerOptionsBuilder;
        public Broker()
        {
            _mqttServerOptionsBuilder = new MqttServerOptionsBuilder()
                .WithConnectionValidator(c =>
                {
                    //Bağlanan Client ip adresi ClientId si konsol ekranından görmek için yazıyoruz.
                    Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} Endpoint: {c.Endpoint} ==> ClientId: {c.ClientId}");
                    if (c.Username == "halil" && c.Password == "Halil123*")
                        c.ReasonCode = MqttConnectReasonCode.Success; // Bağlanmaya çalışan kişiye username ve password doğru ise bağlanma izni vermemizi sağlayacak
                    else
                        c.ReasonCode = MqttConnectReasonCode.NotAuthorized; //Bağlanmaya çalışan kişiye username ve password yanlış ise bağlanma izni verilmeyecek
                })
                .WithApplicationMessageInterceptor(async context =>
                {
                    Console.WriteLine($"Id: {context.ClientId} ==>\nTopic: {context.ApplicationMessage.Topic} \nPayload==>{Encoding.UTF8.GetString(context.ApplicationMessage.Payload)}");
                    // await SendToApi(context); // Gelen-Giden Veri trafiğinin kontrolü  ve Gerekirse bir servise veri gönderme işlemi burdan yapılabilir.
                })
                .WithConnectionBacklog(1000) //Aynı anda kaç bağlantının kuyrukta tutulacağı.;
                .WithDefaultEndpointBoundIPAddress(System.Net.IPAddress.Parse("127.0.0.1")) //Server ip adres ya da bilgisayarın localhostu kullanılabilir.
                .WithDefaultEndpointPort(1884); //Bilgisayar ya da server'dan bağlanan port bilgisi. 
        }
        public void Start()
        {
            _mqttServer = new MqttFactory().CreateMqttServer();
            _mqttServer.StartAsync(_mqttServerOptionsBuilder.Build()).Wait();

            //MQTT
            Console.WriteLine($"Mqtt Broker Oluşturuldu: Host: {_mqttServer.Options.DefaultEndpointOptions.BoundInterNetworkAddress} Port: {_mqttServer.Options.DefaultEndpointOptions.Port}");

            //Start Server
            Task.Run(() => Thread.Sleep(Timeout.Infinite)).Wait();
        }
        public void Stop()
        {
            _mqttServer.StopAsync().Wait();
        }
    }
}
