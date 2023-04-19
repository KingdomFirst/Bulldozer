using System;

namespace Bulldozer.Model
{
    public class AttendanceCsv
    {
        public string AttendanceId { get; set; }

        public string PersonId { get; set; }

        public string GroupId { get; set; }

        public string LocationId { get; set; }

        public string ScheduleId { get; set; }

        public DateTime? StartDateTime { get; set; }

        public DateTime? EndDateTime { get; set; }

        public string Note { get; set; }

        public string CampusId { get; set; }
    }
}