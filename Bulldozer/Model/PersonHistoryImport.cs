using System;

namespace Bulldozer.Model
{
    public class PersonHistoryImport
    {
        public int PersonId { get; set; }

        public int? CategoryId { get; set; }

        public int? ChangedByPersonAliasId { get; set; }

        public string Verb { get; set; }

        public string Caption { get; set; }

        public string ChangeType { get; set; }

        public string ValueName { get; set; }

        public int? RelatedEntityTypeId { get; set; }

        public int? RelatedEntityId { get; set; }

        public string NewValue { get; set; }

        public string OldValue { get; set; }

        public DateTime? HistoryDateTime { get; set; }

        public bool IsSensitive { get; set; }

        public int? ForeignId { get; set; }

        public string ForeignKey { get; set; }
    }
}