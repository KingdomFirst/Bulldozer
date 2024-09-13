using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class MetricValueCsv
    {
        public string Id { get; set; }

        public string MetricId { get; set; }

        public string XValue { get; set; }

        public decimal? YValue { get; set; }

        public DateTime? ValueDateTime { get; set; }

        public string Partition1EntityId { get; set; }

        public string Partition2EntityId { get; set; }

        public string Partition3EntityId { get; set; }

        public string Note { get; set; }

    }
}