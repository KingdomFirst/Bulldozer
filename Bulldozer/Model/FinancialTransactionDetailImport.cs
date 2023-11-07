using System;

namespace Bulldozer.Model
{
    public class FinancialTransactionDetailImport
    {
        public string FinancialTransactionDetailForeignKey { get; set; }

        public string FinancialAccountForeignKey { get; set; }

        public decimal Amount { get; set; }

        public string Summary { get; set; }

        public string CreatedByPersonForeignKey { get; set; }

        public int? EntityTypeId { get; set; }

        public int? EntityId { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public string ModifiedByPersonForeignKey { get; set; }

        public DateTime? ModifiedDateTime { get; set; }
    }
}