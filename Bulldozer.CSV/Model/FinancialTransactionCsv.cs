using System;
using System.Collections.Generic;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class FinancialTransactionCsv
    {
        private string _transactionType = string.Empty;
        private TransactionType _transactionTypeEnum = CSV.CSVInstance.TransactionType.Contribution;
        private bool _isValidTransactionType = false;

        public string Id { get; set; }

        public string BatchId { get; set; }

        public string AuthorizedPersonId { get; set; }

        public DateTime TransactionDate { get; set; }

        public string TransactionType
        {
            get
            {
                return _transactionType;
            }
            set
            {
                _transactionType = value;
                _isValidTransactionType = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _transactionTypeEnum );
            }
        }

        public TransactionType? TransactionTypeEnum
        {
            get
            {
                return _transactionTypeEnum;
            }
            set
            {
                _transactionTypeEnum = value.Value;
                _transactionType = _transactionTypeEnum.ToString();
            }
        }

        public bool IsValidTransactionType
        {
            get
            {
                return _isValidTransactionType;
            }
        }

        public string TransactionSource { get; set; }

        public string CurrencyType { get; set; }

        public string NonCashAssetType { get; set; }

        public string CreditCardType { get; set; }

        public string Summary { get; set; }

        public string TransactionCode { get; set; }

        public bool? IsAnonymous { get; set; }

        public string ScheduledTransactionId { get; set; }

        public string GatewayId { get; set; }

        public string CreatedByPersonId { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public string ModifiedByPersonId { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public List<FinancialTransactionDetailCsv> FinancialTransactionDetails { get; set; } = new List<FinancialTransactionDetailCsv>();

    }
}