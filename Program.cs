using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Naveego.Pipeline;
using Naveego.Pipeline.Protocol;
using Naveego.Pipeline.Publishers;

namespace PipelineExamples
{
    public class Program
    {

        public static void Main(string[] args)
        {

            var publisherInstance = new PublisherInstance
            {
                Name = "My CSV Publisher",
                Description = "Publishes data from a CSV file",
                Type = "csv",
                Settings = new Dictionary<string, object>
                {
                    { "config", "config.json" },
                    { "input",  "Data.csv" }
                }
            };

            // Create our publisher instance
            IPublisher publisher = new CsvPublisher();

            // First let's test the connection
            var testResponse = publisher.TestConnection(
                new TestConnectionRequest { Settings = publisherInstance.Settings }
            );

            Console.WriteLine(string.Format("Testing Connection: Success={0} Message='{1}'", testResponse.Success, testResponse.Message));

            // Let's get the shapes 
            var shapeResponse = publisher.Shapes(
                new DiscoverPublisherShapesRequest { PublisherInstance = publisherInstance }
            );

            var shapeNames = string.Join(",", shapeResponse.Shapes.Select(s => s.Name));
            Console.WriteLine(string.Format("Found {0} Shapes ({1})", shapeResponse.Shapes.Count, shapeNames));

            // Let's get the data
            publisher.Publish(
                new PublishRequest
                {
                    PublisherInstance = publisherInstance,
                    Shape = shapeResponse.Shapes[0]
                },
                new ConsoleDataTransport()
            );

            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }
    
    }
}
