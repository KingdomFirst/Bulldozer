using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class ScheduleCsv
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public bool IsActive { get; set; }

        public string MeetingDay { get; set; }

        public TimeSpan? MeetingTime { get; set; }

        public string Description { get; set; }

        public int? ForeignId { get; set; }

        public string ForeignKey { get; set; }

    }
}