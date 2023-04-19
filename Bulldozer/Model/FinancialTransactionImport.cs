using System;
using System.Collections.Generic;

namespace Bulldozer.Model
{
    public class FinancialTransactionImport
    {
        public string FinancialTransactionForeignKey { get; set; }

        public string BatchForeignKey { get; set; }

        public string AuthorizedPersonForeignKey { get; set; }

        public DateTime TransactionDate { get; set; }

        public int TransactionTypeValueId { get; set; }

        public int TransactionSourceValueId { get; set; }

        public int CurrencyTypeValueId { get; set; }

        public int? NonCashAssetValueId { get; set; }

        public string Summary { get; set; }

        public string TransactionCode { get; set; }

        public List<FinancialTransactionDetailImport> FinancialTransactionDetailImports { get; set; }

        public string CreatedByPersonForeignKey { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public string ModifiedByPersonForeignKey { get; set; }

        public DateTime? ModifiedDateTime { get; set; }
    }
}