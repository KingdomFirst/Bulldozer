using Rock.Model;

namespace Bulldozer.Model
{
    public class MetricValuePartitionImport
    {
        public MetricValue MetricValue { get; set; }

        public MetricPartition MetricPartition { get; set; }

        public string EntityId { get; set; }

        public string ForeignKey { get; set; }

        public string CsvMetricValueId { get; set; }
    }
}