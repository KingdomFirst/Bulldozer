﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class BusinessPhoneCsv
    {
        public string BusinessId { get; set; }

        public string PhoneType { get; set; }

        public string PhoneNumber { get; set; }

        public bool? IsMessagingEnabled { get; set; }

        public bool? IsUnlisted { get; set; }

        public int? CountryCode { get; set; }

        public string PhoneId { get; set; }

    }
}