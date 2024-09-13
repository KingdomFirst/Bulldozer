using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class MetricCsv
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public string Subtitle { get; set; }

        public string Category { get; set; }

        public string IconCssClass { get; set; }

        public string ParentCategory { get; set; }

        public string Partition1Id { get; set; }

        public string Partition1Label { get; set; }

        public string Partition1EntityTypeName { get; set; }

        public bool? Partition1IsRequired { get; set; } = false;

        public string Partition2Id { get; set; }

        public string Partition2Label { get; set; }

        public string Partition2EntityTypeName { get; set; }

        public bool? Partition2IsRequired { get; set; } = false;

        public string Partition3Id { get; set; }

        public string Partition3Label { get; set; }

        public string Partition3EntityTypeName { get; set; }

        public bool? Partition3IsRequired { get; set; } = false;

    }
}