using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class GroupAddressCsv
    {
        public string GroupId { get; set; }

        public string Street1 { get; set; }

        public string Street2 { get; set; }

        public string City { get; set; }

        public string State { get; set; }

        public string PostalCode { get; set; }

        public string Country { get; set; }

        public string Latitude { get; set; }

        public string Longitude { get; set; }

        public bool IsMailing { get; set; }

        public AddressType AddressType { get; set; }

        public string AddressId { get; set; } = null;

    }

    public class GroupAddressCsvMap : ClassMap<GroupAddressCsv>
    {
        public GroupAddressCsvMap()
        {
            Map( m => m.GroupId );
            Map( m => m.Street1 );
            Map( m => m.Street2 );
            Map( m => m.City );
            Map( m => m.State );
            Map( m => m.PostalCode );
            Map( m => m.Country );
            Map( m => m.Latitude ).Optional();
            Map( m => m.Longitude ).Optional();
            Map( m => m.IsMailing );
            Map( m => m.AddressType );
            Map( m => m.AddressId ).Optional();
        }
    }
}