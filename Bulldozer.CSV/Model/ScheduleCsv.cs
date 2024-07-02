﻿using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class ScheduleCsv
    {
        private string _meetingDay = string.Empty;
        private DayOfWeek _meetingDayEnum = DayOfWeek.Sunday;
        private bool _isValidmeetingDay = false;

        public string Id { get; set; }

        public string Name { get; set; }

        public bool IsActive { get; set; }

        public string MeetingDay
        {
            get
            {
                return _meetingDay;
            }
            set
            {
                _meetingDay = value;
                Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _meetingDayEnum );
            }
        }

        public DayOfWeek? MeetingDayEnum
        {
            get
            {
                return _meetingDayEnum;
            }
            set
            {
                _meetingDayEnum = value.Value;
                _meetingDay = _meetingDayEnum.ToString();
            }
        }

        public bool IsValidmeetingDay
        {
            get
            {
                return _isValidmeetingDay;
            }
        }

        public TimeSpan? MeetingTime { get; set; }

        public string Description { get; set; }

        public int? ForeignId { get; set; }

        public string ForeignKey { get; set; }

    }
}