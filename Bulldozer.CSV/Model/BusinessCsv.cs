using CsvHelper.Configuration;
using Rock.Model;
using System;
using System.Collections.Generic;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class BusinessCsv
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Email { get; set; }

        public string RecordStatus { get; set; }

        public string InactiveReasonNote { get; set; }

        public string InactiveReason { get; set; }

        public string EmailPreference { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public CampusCsv Campus { get; set; }

        public string Note { get; set; }

        public bool? IsEmailActive { get; set; }
    }
}