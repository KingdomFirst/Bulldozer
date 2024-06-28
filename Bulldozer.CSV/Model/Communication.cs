using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class CommunicationCsv
    {
        public string CommunicationId { get; set; }

        public string FromName { get; set; }

        public string FromEmail { get; set; }

        public string ReplyToEmail { get; set; }

        public string CCEmails { get; set; }

        public string BCCEmails { get; set; }

        public string Subject { get; set; }

        public string EmailMessage { get; set; }

        public string SMSMessage { get; set; }

        public string CommunicationType { get; set; }

        public bool? IsBulkCommunication { get; set; } = false;

        public string SenderPersonId { get; set; }

        public DateTime? SentDateTime { get; set; }

        public string CreatedByPersonId { get; set; }

        public DateTime? CreatedDateTime { get; set; }
    }
}