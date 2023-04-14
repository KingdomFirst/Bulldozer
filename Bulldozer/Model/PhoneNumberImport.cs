using System;

namespace Bulldozer.Model
{
    public class PhoneNumberImport
    {
        public int PersonId { get; set; }
        
        public string Number { get; set; }

        public string Extension { get; set; }

        public int? NumberTypeValueId { get; set; }

        public bool IsMessagingEnabled { get; set; }

        public bool IsUnlisted { get; set; }

        public int? CountryCode { get; set; }

        public string PhoneId { get; set; }
    }
}