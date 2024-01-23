using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class LocationCsv
    {
        public string Id { get; set; }

        public string ParentLocationId { get; set; }

        public string Name { get; set; }

        public string Street1 { get; set; }

        public string Street2 { get; set; }

        public string City { get; set; }

        public string State { get; set; }

        public string PostalCode { get; set; }

        public string Country { get; set; }

        public string County { get; set; }

        public string LocationType { get; set; }

        public bool IsActive { get; set; }

    }
}