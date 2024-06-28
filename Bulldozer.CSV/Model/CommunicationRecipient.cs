using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class CommunicationRecipientCsv
    {
        public string CommunicationRecipientId { get; set; }

        public string CommunicationId { get; set; }

        public string RecipientPersonId { get; set; }

        public DateTime? SentDateTime { get; set; }

        public string RecipientStatus { get; set; }
    }
}