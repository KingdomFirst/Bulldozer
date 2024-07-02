using System;
using System.Collections.Generic;
using Rock.Model;

namespace Bulldozer.Model
{
    public class FinancialBatchCsv
    {
        private string _status = string.Empty;
        private BatchStatus _statusEnum = BatchStatus.Closed;
        private bool _isValidStatus = false;

        public string Id { get; set; }

        public string Name { get; set; }

        public CampusCsv Campus { get; set; }

        public DateTime? StartDate { get; set; }

        public DateTime? EndDate { get; set; }

        public string Status
        {
            get
            {
                return _status;
            }
            set
            {
                _status = value;
                _isValidStatus = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _statusEnum );
            }
        }

        public BatchStatus? StatusEnum
        {
            get
            {
                return _statusEnum;
            }
            set
            {
                _statusEnum = value.Value;
                _status = _statusEnum.ToString();
            }
        }

        public bool IsValidStatus
        {
            get
            {
                return _isValidStatus;
            }
        }

        public string CreatedByPersonId { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public string ModifiedByPersonId { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public decimal ControlAmount { get; set; }

        public List<FinancialTransactionCsv> FinancialTransactions { get; set; }
    }
}