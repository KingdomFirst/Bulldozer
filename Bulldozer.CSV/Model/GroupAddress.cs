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
}