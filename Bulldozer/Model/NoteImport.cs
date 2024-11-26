using System;

namespace Bulldozer.Model
{
    public class NoteImport
    {
        public int? NoteForeignId { get; set; }

        public string NoteForeignKey { get; set; }

        public int NoteTypeId { get; set; }

        public int EntityTypeId { get; set; }

        public string EntityForeignKey { get; set; }

        public string Caption { get; set; }

        public bool IsAlert { get; set; } = false;

        public bool IsPrivateNote { get; set; } = false;

        public bool NotificationSent { get; set; } = false;

        public string Text { get; set; }

        public DateTime? DateTime { get; set; }

        public string CreatedByPersonForeignKey { get; set; }
    }
}