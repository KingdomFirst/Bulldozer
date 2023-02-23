using Rock.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class BusinessCsv
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Email { get; set; }

        public RecordStatus RecordStatus { get; set; }

        public string InactiveReasonNote { get; set; }

        public string InactiveReason { get; set; }

        public EmailPreference EmailPreference { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public CampusCsv Campus { get; set; }

        public string Note { get; set; }

        public bool? IsEmailActive { get; set; }

        public List<BusinessContactCsv> Contacts { get; set; }

        public List<BusinessAddressCsv> Addresses { get; set; }

        public List<BusinessPhoneCsv> PhoneNumbers { get; set; }

        public List<BusinessAttributeValueCsv> Attributes { get; set; }

    }
}