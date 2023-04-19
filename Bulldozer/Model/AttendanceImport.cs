using System;

namespace Bulldozer.Model
{
    public class AttendanceImport
    {
        public int? AttendanceForeignId { get; set; }

        public string AttendanceForeignKey { get; set; }

        public int? OccurrenceForeignId { get; set; }

        public string OccurrenceForeignKey { get; set; }

        public int PersonAliasId { get; set; }

        public int GroupId { get; set; }

        public int? LocationId { get; set; }

        public int? ScheduleId { get; set; }

        public DateTime StartDateTime { get; set; }

        public DateTime? EndDateTime { get; set; }

        public string Note { get; set; }

        public int? CampusId { get; set; }
    }
}