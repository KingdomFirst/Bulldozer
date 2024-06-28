using System;
using System.Collections.Generic;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class FinancialBatchCsv
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public CampusCsv Campus { get; set; }

        public DateTime? StartDate { get; set; }

        public DateTime? EndDate { get; set; }

        public string Status { get; set; }

        public string CreatedByPersonId { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public string ModifiedByPersonId { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public decimal ControlAmount { get; set; }

        public List<FinancialTransactionCsv> FinancialTransactions { get; set; }
    }
}