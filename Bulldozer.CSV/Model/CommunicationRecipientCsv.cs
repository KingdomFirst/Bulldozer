using System;
using Rock.Model;

namespace Bulldozer.Model
{
    public class CommunicationRecipientCsv
    {
        private string _recipientStatus = string.Empty;
        private CommunicationRecipientStatus _recipientStatusEnum = CommunicationRecipientStatus.Delivered;
        private bool _isValidRecipientStatus = false;

        public string CommunicationRecipientId { get; set; }

        public string CommunicationId { get; set; }

        public string RecipientPersonId { get; set; }

        public DateTime? SentDateTime { get; set; }

        public string RecipientStatus
        {
            get
            {
                return _recipientStatus;
            }
            set
            {
                _recipientStatus = value;
                _isValidRecipientStatus = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _recipientStatusEnum  );
            }
        }

        public CommunicationRecipientStatus? RecipientStatusEnum
        {
            get
            {
                return _recipientStatusEnum;
            }
            set
            {
                _recipientStatusEnum = value.Value;
                _recipientStatus = _recipientStatusEnum.ToString();
            }
        }

        public bool IsValidRecipientStatus
        {
            get
            {
                return _isValidRecipientStatus;
            }
        }
    }
}