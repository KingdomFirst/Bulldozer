using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class GroupAddressCsv
    {
        private string _addressType = string.Empty;
        private LocationType _addressTypeEnum = CSV.CSVInstance.LocationType.MeetingLocation;
        private bool _isValidAddressType = false;
        private string _groupMemberAddressType = string.Empty;
        private LocationType _groupMemberAddressTypeEnum;
        private bool _isValidGroupMemberAddressType;

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

        public LocationType? AddressTypeEnum
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

        public string GroupMemberPersonId { get; set; } = null;

        public string GroupMemberAddressType
        {
            get
            {
                return _groupMemberAddressType;
            }
            set
            {
                _groupMemberAddressType = value;
                _isValidGroupMemberAddressType = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _groupMemberAddressTypeEnum );
            }
        }

        public LocationType? GroupMemberAddressTypeEnum
        {
            get
            {
                return _groupMemberAddressTypeEnum;
            }
            set
            {
                _groupMemberAddressTypeEnum = value.Value;
                _groupMemberAddressType = _groupMemberAddressTypeEnum.ToString();
            }
        }
        public bool IsValidGroupMemberAddressType
        {
            get
            {
                return _isValidGroupMemberAddressType;
            }
        }
    }
}