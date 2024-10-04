using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class CategoryCsv
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public string EntityTypeName { get; set; }

        public string ParentCategoryId { get; set; }

        public string EntityTypeQualifierColumn { get; set; }

        public string EntityTypeQualifierValue { get; set; }

        public string IconCssClass { get; set; }

    }
}