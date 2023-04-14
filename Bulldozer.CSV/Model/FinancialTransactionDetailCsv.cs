using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bulldozer.Model
{
    public class FinancialTransactionDetailCsv
    {
        public string Id { get; set; }

        public string TransactionId { get; set; }

        public string AccountId { get; set; }

        public decimal Amount { get; set; }

        public string Summary { get; set; }

        public string CreatedByPersonId { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public string ModifiedByPersonId { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

    }
}