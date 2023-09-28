using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MQTT.Publisher
{
    public class Publisher
    {
        private IMqttClient _client;
        private IMqttClientOptions _options;
        public Publisher()
        {
            var factory = new MqttFactory();
            _client = factory.CreateMqttClient(); //mqttFactory kullanılarak bir factory oluşturuyorum.

            //Configure options
            _options = new MqttClientOptionsBuilder()
                            .WithClientId("publisher") // Bağlanan IoT'nin benzersiz numarası.
                            .WithTcpServer("127.0.0.1", 1884) // Bağlanacağı port ve ip adresi (sunucucu yada bilgisayar) biz local çalıştığımız için bu şekilde local ip girdik
                            .WithCredentials("halil", "Halil123*")
                            .WithCleanSession()
                            .Build();
            //Handlers
            _client.UseConnectedHandler(e => //Brokera bağlanıldığında çalışacak kod.
            {
                Console.WriteLine("Publisher");
                Console.WriteLine("MQTT Broker'a başarılı bir şekilde bağlanıldı...");
            });
            _client.UseDisconnectedHandler(e => // Broker'dan bağlantısı koptuğunda çalışacak kod
            {
                Console.WriteLine("MQTT Broker'dan başarılı bir şekilde ayrıldı...");
            });
            _client.UseApplicationMessageReceivedHandler(e => //Aşağıdaki konfigurasyon ile Topic ve Veri alanı olan Payload doldurulup client ile gönderilir
            {
                // publisher aynı zaman subscriber olabildiği için onunda veri alma fonksiyonunu aşağıdaki şekilde oluşturuyoruz. 
                // ancak biz bu projeyi sadece Publisher olarak kullandığımız için aşağıdaki kod blogunu kapattım


                ////string topic = e.ApplicationMessage.Topic; // baglanılan konu (topic)
                ////if (string.IsNullOrWhiteSpace(topic) == false)
                ////{
                ////    string payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload); // Gelen veri veri
                ////    Console.WriteLine($"Topic: {topic}. Gelen Mesaj: {payload}");
                ////}


            });

            //Connect
            _client.ConnectAsync(_options).Wait(); //Brokera bağlanmak için asenkron olarak bağlantı başlatılıyor.
            StartStream();//// yayın yapmayı simule etmek için aşağıdaki şekilde bir while döngüsü ile yayın kuyruğu oluşturuyorum 
            Task.Run(()=>Thread.Sleep(Timeout.Infinite)).Wait();
        }

        public void StartStream()
        {
            while (true)
            {
                Console.Write("Yaymak istediğiniz mesajı yazın: "); //Gönderilecek olan veriyi kullanıcıdan almak için mesaj
                var counter = Console.ReadLine(); //Kullanıcıdan mesaj alınıyor.
                var testMessage = new MqttApplicationMessageBuilder() // Veri göndermek için bir yapı kuruyoruz.  
                    .WithTopic("Subject_1") // Belirtilen bir konu ile ilgili bir mesaj yayıyoruz. İlerde Subscriber lar bu konu ile veri alacaklar
                    .WithPayload($"Payload: {counter}") // Konunun dışında asıl alınacak olan veri. Bu bir json yada xml verisi olabilir
                    .WithExactlyOnceQoS() // Verinin iletilmesinin doğruluğunun kontrolü (Quality of Service)
                    .WithRetainFlag(false) // Gönderilen mesajın kalıcı olup olmama durumu. flag false olur ise kalıcı olmaz ve o anda bu konuya bağlı bir subscriber yok ise veri ona iletilmez silinir gider
                    .Build();
                if(_client.IsConnected) //Publisher hala brokera bağlı mı onu kontrol ediyoruz.
                {
                    Console.WriteLine($"Yayın Tarihi: {DateTime.Now}, Topic: {testMessage.Topic}, Payload: {System.Text.Encoding.ASCII.GetString(testMessage.Payload)}"); //Gönderilen verinin ekrana yazılması.
                    _client.PublishAsync(testMessage); //Mesajın brokera gönderilmesi
                }
            }
        }
        void StopConnection()
        {
            _client.DisconnectAsync().Wait(); //Broker ile bağlantının kesilmesi
            Console.WriteLine("Broker ile bağlantı kesildi!!!");
        }
    }
}
