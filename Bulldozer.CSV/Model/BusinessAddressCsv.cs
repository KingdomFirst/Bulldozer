using System;
using CsvHelper.Configuration;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class BusinessAddressCsv
    {
        private string _addressType = string.Empty;
        private AddressType _addressTypeEnum = CSV.CSVInstance.AddressType.Other;
        private bool _isValidAddressType = false;

        public string BusinessId { get; set; }

        public string Street1 { get; set; }

        public string Street2 { get; set; }

        public string City { get; set; }

        public string State { get; set; }

        public string PostalCode { get; set; }

        public string Country { get; set; }

        public string Latitude { get; set; } = null;

        public string Longitude { get; set; } = null;

        public bool IsMailing { get; set; }

        public string AddressType
        {
            get
            {
                return _addressType;
            }
            set
            {
                _addressType = value;
                _isValidAddressType = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _addressTypeEnum );
            }
        }

        public AddressType? AddressTypeEnum
        {
            get
            {
                return _addressTypeEnum;
            }
            set
            {
                _addressTypeEnum = value.Value;
                _addressType = _addressTypeEnum.ToString();
            }
        }

        public bool IsValidAddressType
        {
            get
            {
                return _isValidAddressType;
            }
        }

        public string AddressId { get; set; } = null;

    }
}