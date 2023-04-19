using System;
using System.Collections.Generic;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class FinancialTransactionCsv
    {
        public string Id { get; set; }

        public string BatchId { get; set; }

        public string AuthorizedPersonId { get; set; }

        public DateTime TransactionDate { get; set; }

        public TransactionType TransactionType { get; set; }

        public string TransactionSource { get; set; }

        public string CurrencyType { get; set; }

        public string Summary { get; set; }

        public string TransactionCode { get; set; }

        public string CreatedByPersonId { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public string ModifiedByPersonId { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public List<FinancialTransactionDetailCsv> FinancialTransactionDetails { get; set; } = new List<FinancialTransactionDetailCsv>();

    }
}