﻿namespace Bulldozer.Model
{
    public class GroupAddressImport
    {
        public int GroupLocationTypeValueId { get; set; }

        public bool IsMailingLocation { get; set; }

        public bool IsMappedLocation { get; set; }

        public string Street1 { get; set; }

        public string Street2 { get; set; }

        public string City { get; set; }

        public string County { get; set; }

        public string State { get; set; }

        public string Country { get; set; }

        public string PostalCode { get; set; }

        public double? Latitude { get; set; }

        public double? Longitude { get; set; }

        public int? FamilyId { get; set; }

        public int? GroupId { get; set; }

        public string AddressForeignKey { get; set; }
    }
}