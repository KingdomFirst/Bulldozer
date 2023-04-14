using CsvHelper.Configuration;
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

        //public List<BusinessAttributeValueCsv> Attributes { get; set; }

    }

    public class BusinessCsvMap : ClassMap<BusinessCsv>
    {
        public BusinessCsvMap()
        {
            Map( m => m.Id );
            Map( m => m.Name );
            Map( m => m.Email );
            Map( m => m.RecordStatus );
            Map( m => m.InactiveReasonNote ).Optional();
            Map( m => m.InactiveReason ).Optional();
            Map( m => m.EmailPreference ).Optional();
            Map( m => m.CreatedDateTime ).Optional();
            Map( m => m.ModifiedDateTime ).Optional();
            Map( m => m.Campus.CampusId );
            Map( m => m.Campus.CampusName );
            Map( m => m.Note ).Optional();
            Map( m => m.IsEmailActive ).Optional();
        }
    }
}