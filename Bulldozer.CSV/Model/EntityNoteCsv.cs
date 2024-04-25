using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class EntityNoteCsv
    {
        public string Id { get; set; }

        public string NoteType { get; set; }

        public string Caption { get; set; }

        public bool? IsAlert { get; set; } = false;

        public bool? IsPrivateNote { get; set; } = false;

        public string Text { get; set; }

        public string EntityTypeName { get; set; }

        public string EntityId { get; set; }

        public DateTime? DateTime { get; set; }

        public string CreatedByPersonId { get; set; }
    }
}