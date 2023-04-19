using System;

namespace Bulldozer.Model
{
    public class PersonHistoryCsv
    {
        public string HistoryId { get; set; }

        public string HistoryPersonId { get; set; }

        public string HistoryCategory { get; set; }

        public string ChangedByPersonId { get; set; }

        public string Verb { get; set; }

        public string Caption { get; set; }

        public string ChangeType { get; set; }

        public string ValueName { get; set; }

        public string RelatedEntityType { get; set; }

        public string RelatedEntityId { get; set; }

        public string NewValue { get; set; }

        public string OldValue { get; set; }

        public DateTime? HistoryDateTime { get; set; }

        public bool IsSensitive { get; set; } = false;

    }
}