using CsvHelper;
using Newtonsoft.Json;
using Producer.Models;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Producer
{
    static class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory
            {
                Uri = new Uri("amqps://rnjlwzmf:BG54g2oGW3oLhkVdpfoGDL-D9EAT2YYD@cow.rmq2.cloudamqp.com/rnjlwzmf"),
               
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare("measurements",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var SensorID = Guid.Parse(ConfigurationManager.AppSettings["SensorID"]);

                TextReader textReader = new StreamReader("sensor.csv");
                var csvReader = new CsvReader(textReader, new CultureInfo("en-US"), false);
                var records = csvReader.GetRecords<float>();
               
                int count = 1;
                foreach (var record in records)
                {
                    var timeStamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds();
                    var measurement = new Measurement
                    {
                        SensorID = SensorID,
                        Value = record,
                        TimeStamp = timeStamp+3600
                    };
                    
                   // var message = new { Measurement = measurement };
                    var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(measurement));



                    channel.BasicPublish("", "measurements", null, body);
                    Console.WriteLine("\nMeasure no: "  + count +"\nSensor ID: "+ SensorID +"\nValue: " + record);
                    count++;
                    Thread.Sleep(2000);
                }
               
            }

            Console.WriteLine("Sending finished");


        }
    }
}
