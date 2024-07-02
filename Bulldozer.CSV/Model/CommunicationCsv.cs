using System;
using Rock.Model;

namespace Bulldozer.Model
{
    public class CommunicationCsv
    {
        private string _communicationType = string.Empty;
        private CommunicationType _communicationTypeEnum = Rock.Model.CommunicationType.RecipientPreference;
        private bool _isValidCommunicationType = false;

        public string CommunicationId { get; set; }

        public string FromName { get; set; }

        public string FromEmail { get; set; }

        public string ReplyToEmail { get; set; }

        public string CCEmails { get; set; }

        public string BCCEmails { get; set; }

        public string Subject { get; set; }

        public string EmailMessage { get; set; }

        public string SMSMessage { get; set; }

        public string CommunicationType
        {
            get
            {
                return _communicationType;
            }
            set
            {
                _communicationType = value;
                _isValidCommunicationType = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _communicationTypeEnum );
            }
        }

        public CommunicationType? CommunicationTypeEnum
        {
            get
            {
                return _communicationTypeEnum;
            }
            set
            {
                _communicationTypeEnum = value.Value;
                _communicationType = _communicationTypeEnum.ToString();
            }
        }

        public bool IsValidCommunicationType
        {
            get
            {
                return _isValidCommunicationType;
            }
        }

        public bool? IsBulkCommunication { get; set; } = false;

        public string SenderPersonId { get; set; }

        public DateTime? SentDateTime { get; set; }

        public string CreatedByPersonId { get; set; }

        public DateTime? CreatedDateTime { get; set; }
    }
}