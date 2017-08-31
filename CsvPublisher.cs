using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using Naveego.Pipeline;
using Naveego.Pipeline.Protocol;
using Naveego.Pipeline.Publishers;
using Naveego.Pipeline.Publishers.Transport;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace PipelineExamples
{
    public class CsvPublisher : AbstractPublisher
    {

        public override TestConnectionResponse TestConnection(TestConnectionRequest request)
        {
           
            try
            {
                var input = (string)request.Settings["input"];
                if (File.Exists(input))
                {
                    using (File.OpenRead(input))
                    {
                    }
                }
            }
            catch (Exception ex)
            {
                return new TestConnectionResponse
                {
                    Success = false,
                    Message = string.Format("Could not connect to file: {0}", ex.Message)
                };
            }

            return new TestConnectionResponse
            {
                Success = true,
                Message = "Connected to file successfully"
            };

        }

        public override DiscoverShapesResponse Shapes(DiscoverPublisherShapesRequest request)
        {
            var configFile = (string)request.PublisherInstance.Settings["config"];
            var configStr = File.ReadAllText(configFile);
            var config = JObject.Parse(configStr);
            var shapeObj = (JObject)config["shape"];

            var shapes = new List<ShapeDefinition>();
            shapes.Add(shapeObj.ToObject<ShapeDefinition>());

            return new DiscoverShapesResponse
            {
                Shapes = shapes
            };
        }

        public override PublishResponse Publish(PublishRequest request, IDataTransport dataTransport)
        {
            var input = (string)request.PublisherInstance.Settings["input"];

            var dataPoints = new List<DataPoint>();

            using (var sr = new StreamReader(File.OpenRead(input)))
            using (var csvReader = new CsvReader(sr))
            {
                // read past header row
                csvReader.Read();

                // loop over data fields
                while (csvReader.Read())
                {
                    var dp = new DataPoint
                    {
                        TenantID = "mytenantid",
                        Action = DataPointAction.Upsert,
                        Entity = request.Shape.Name,
                        Source = "CRM",
                        KeyNames = request.Shape.Keys,
                        Data = new Dictionary<string, object>()
                    };

                    for (var i = 0; i < request.Shape.Properties.Count; i++)
                    {
                        var prop = request.Shape.Properties[i];

                        if (prop.Type == "number")
                        {
                            dp.Data[prop.Name] = csvReader.GetField<int>(i);
                        }
                        else
                        {
                            dp.Data[prop.Name] = csvReader.GetField<string>(i);
                        }
                    }

                    dataPoints.Add(dp);
                }
            }

            dataTransport.Send(dataPoints);

            return new PublishResponse();
        }

    }
}
