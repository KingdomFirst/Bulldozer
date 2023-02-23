using System;

namespace Bulldozer.Model
{
    public class AttendanceImport
    {
        public int? AttendanceForeignId { get; set; }

        public string AttendanceForeignKey { get; set; }

        public string PersonForeignKey { get; set; }

        public string GroupForeignKey { get; set; }

        public string LocationForeignKey { get; set; }

        public string ScheduleForeignKey { get; set; }

        public DateTime StartDateTime { get; set; }

        public DateTime? EndDateTime { get; set; }

        public string Note { get; set; }

        public int? CampusId { get; set; }
    }
}