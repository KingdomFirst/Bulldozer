using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class PersonPhoneCsv
    {
        public string PersonId { get; set; }

        public string PhoneType { get; set; }

        public string PhoneNumber { get; set; }

        public bool? IsMessagingEnabled { get; set; }

        public bool? IsUnlisted { get; set; }

        public string PhoneId { get; set; }

        public int? CountryCode { get; set; }

        public string Extension { get; set; }

    }

    public class PersonPhoneCsvMap : ClassMap<PersonPhoneCsv>
    {
        public PersonPhoneCsvMap()
        {
            Map( m => m.PersonId );
            Map( m => m.PhoneType );
            Map( m => m.PhoneNumber );
            Map( m => m.IsMessagingEnabled );
            Map( m => m.IsUnlisted );
            Map( m => m.PhoneId ).Optional();
            Map( m => m.CountryCode ).Optional();
            Map( m => m.Extension ).Optional();
        }
    }
}