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

        private IDictionary<string, object> _settings;
        private IList<ShapeDefinition> _shapes;

        public override InitializeResponse Init(InitializePublisherRequest request)
        {
            _settings = request.Settings;
            return new InitializeResponse();
        }

        public override TestConnectionResponse TestConnection(TestConnectionRequest request)
        {

            Logger.LogInfo("Testing Connection");
            try
            {
                var input = (string)_settings["input"];
                if (File.Exists(input))
                {
                    using (File.OpenRead(input))
                    {
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.LogError("Could not test connection", ex);
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
            Logger.LogInfo("Discovering Shapes");
            _settings = request.Settings;
            var configFile = (string)_settings["config"];
            var configStr = File.ReadAllText(configFile);
            var config = JObject.Parse(configStr);
            var shapeObj = (JObject)config["shape"];

            var shapes = new List<ShapeDefinition>();
            shapes.Add(shapeObj.ToObject<ShapeDefinition>());

            _shapes = shapes;
            return new DiscoverShapesResponse
            {
                Shapes = shapes
            };
        }

        public override PublishResponse Publish(PublishRequest request, IDataTransport dataTransport)
        {
            Logger.LogInfo("Calling Publish");
            var input = (string)_settings["input"];

            var dataPoints = new List<DataPoint>();

            var shape = _shapes.FirstOrDefault(s => s.Name.Equals(request.ShapeName, StringComparison.OrdinalIgnoreCase));

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
                        Entity = shape.Name,
                        Source = "CRM",
                        KeyNames = shape.Keys,
                        Data = new Dictionary<string, object>()
                    };

                    for (var i = 0; i < shape.Properties.Count; i++)
                    {
                        var prop = shape.Properties[i];

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
