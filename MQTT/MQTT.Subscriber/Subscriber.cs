using MQTTnet.Client.Options;
using MQTTnet.Client;
using MQTTnet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MQTT.Subscriber
{
    public class Subscriber
    {
        private IMqttClient _client;
        private IMqttClientOptions _options;

        public Subscriber()
        {
            var factory = new MqttFactory();
            _client = factory.CreateMqttClient(); // mqttFactory kullanarak bir factory oluşturuyorum. 

            //Configure options
            _options = new MqttClientOptionsBuilder()
                .WithClientId("subscriber") // Bağlanan IoT'nin benzersiz numarası.
                .WithTcpServer("127.0.0.1", 1884) // Bağlanacağı port ve ip adresi (sunucucu yada bilgisayar) biz local çalıştığımız için bu şekilde local ip girdik
                .WithCredentials("halil", "Halil123*")
                .WithCleanSession()
                .Build();

            //Handlers
            _client.UseConnectedHandler(e => // Broker'a baglandığında çalışacak kod
            {
                Console.WriteLine("Subscriber");
                Console.WriteLine("MQTT Broker'a başarılı bir şekilde bağlanıldı...");
                _client.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic("Subject_1").Build()).Wait();
                //_client.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic("Subject_2").Build()).Wait(); //Burada hangi konularla ilgili mesajlara abone olacağını belirliyoruz.
            });
            _client.UseDisconnectedHandler(e => // Broker'dan bağlantısı koptuğunda çalışacak kod
            {
                Console.WriteLine("MQTT Broker'dan başarılı bir şekilde ayrıldı...");
            });
            _client.UseApplicationMessageReceivedHandler(e => // Aşağıdaki konfigurasyon ile Topic ve Veri alanı olan Payload doldurulup client ile gönderilir
            {
                Console.WriteLine("**** Gelen Mesaj ****");
                Console.WriteLine($"+Topic= {e.ApplicationMessage.Topic}");
                Console.WriteLine($"+Payload= {Encoding.UTF8.GetString(e.ApplicationMessage.Payload)}");
                Console.WriteLine($"+QoS= {e.ApplicationMessage.QualityOfServiceLevel}");
                Console.WriteLine($"+Retain= {e.ApplicationMessage.Retain}");
                Console.WriteLine();

            });


            //Connect
            _client.ConnectAsync(_options).Wait(); // Broker'a bağlanmak için asenkron olarak bağlantı başlatıyor.
            StartStream(); // yayın yapmayı simule etmek için aşağıdaki şekilde bir while döngüsü ile yayın kuyruğu oluşturuyorum. 
            Task.Run(() => Thread.Sleep(Timeout.Infinite)).Wait();
        }
        public void StartStream()
        {

            while (true)
            {
                Console.Write("Yaymak istediğiniz mesajı yazın: "); // Gönderilecek olan veriyi kullanıcıdan almak için mesaj 
                var counter = Console.ReadLine(); // Kullanıcıdan mesaj alınıyor..
                var testMessage = new MqttApplicationMessageBuilder() // Veri göndermek için bir yapı kuruyoruz. 
                    .WithTopic("Subject_1") // Belirtilen bir konu ile ilgili bir mesaj yayıyoruz. İlerde Subscriber lar bu konu ile veri alacaklar
                    .WithPayload($"Payload: {counter}") // Konunun dışında asıl alınacak olan veri. Bu bir json yada xml verisi olabilir
                    .WithExactlyOnceQoS() // Verinin iletilmesinin doğruluğunun kontrolü (Quality of Service)
                    .WithRetainFlag(false) // Gönderilen mesajın kalıcı olup olmama durumu. Flag false olur ise kalıcı olmaz ve o anda bu konuya bağlı bir subscriber yok ise veri ona iletilmez silinir gider
                    .Build();


                if (_client.IsConnected) // Publisher halen broker'a bağlımı onu kontrol ediyoruz. 
                {
                    Console.WriteLine($"Yayın tarihi: {DateTime.Now}, Topic: {testMessage.Topic}, Payload: {System.Text.Encoding.ASCII.GetString(testMessage.Payload)}"); // Gönderilen verinin ekrana yazılması
                    _client.PublishAsync(testMessage); // Mesajı broker'a gönderilmesi
                }
            }
        }

        void StopConnection()
        {
            _client.DisconnectAsync().Wait(); // Broker ile bağlantının kesilmesi
            Console.WriteLine("Broker ile bağlantı kesildi...");

        }
    }
}
