using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Naveego.Pipeline;
using Naveego.Pipeline.Publishers.Transport;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace PipelineExamples
{
    public class ConsoleDataTransport : IDataTransport
    {   
        private JsonSerializer _serializer = new JsonSerializer();

        public ConsoleDataTransport()
        {
            _serializer = new JsonSerializer();
            _serializer.Converters.Add(new StringEnumConverter());
        }

        public void Send(IList<DataPoint> dataPoints)
        {
            foreach (var dp in dataPoints)
            {
                var sb = new StringBuilder();

                using (var jw = new JsonTextWriter(new StringWriter(sb)))
                {
                    _serializer.Serialize(jw, dp);
                }

                Console.WriteLine(sb.ToString());
            }
        }
    }
}
