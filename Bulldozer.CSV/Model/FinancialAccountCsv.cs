using System;
using System.Collections.Generic;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class FinancialAccountCsv
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public bool? IsTaxDeductible { get; set; } = true;

        public CampusCsv Campus { get; set; }

        public string ParentAccountId { get; set; }

        public string GLAccount { get; set; }

        public bool? IsActive { get; set; } = true;

        public DateTime? StartDate { get; set; }

        public DateTime? EndDate { get; set; }

        public int? Order { get; set; }

        public string PublicName { get; set; }
    }
}