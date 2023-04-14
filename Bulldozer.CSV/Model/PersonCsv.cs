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
    public class PersonCsv
    {
        public readonly int DefaultBirthdateYear;

        public string Id { get; set; }

        public string FamilyId { get; set; }

        public string FamilyName { get; set; }

        public string FamilyImageUrl { get; set; }

        public FamilyRole FamilyRole { get; set; }

        public string FirstName { get; set; }

        public string NickName { get; set; }

        public string LastName { get; set; }

        public string MiddleName { get; set; }

        public string Salutation { get; set; }

        public string Suffix { get; set; }

        public string Email { get; set; }

        public Gender Gender { get; set; }

        public string MaritalStatus { get; set; }

        public DateTime? Birthdate { get; set; }

        public DateTime? AnniversaryDate { get; set; }

        public RecordStatus RecordStatus { get; set; }

        public string InactiveReasonNote { get; set; }

        public string InactiveReason { get; set; }

        public string ConnectionStatus { get; set; }

        public EmailPreference? EmailPreference { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public string PersonPhotoUrl { get; set; }

        public CampusCsv Campus { get; set; }

        public string Note { get; set; }

        public string Grade { get; set; }

        public bool? GiveIndividually { get; set; }

        public bool? IsDeceased { get; set; }

        public bool? IsEmailActive { get; set; }

        public string PreviousPersonIds { get; set; }

        public List<PersonPhoneCsv> PhoneNumbers { get; set; }

        public List<PersonSearchKeyCsv> PersonSearchKeys { get; set; }
    }

    public class PersonCsvMap : ClassMap<PersonCsv>
    {
        public PersonCsvMap()
        {
            Map( m => m.Id );
            Map( m => m.FamilyId );
            Map( m => m.FamilyName ).Optional();
            Map( m => m.FamilyImageUrl ).Optional();
            Map( m => m.FamilyRole );
            Map( m => m.FirstName );
            Map( m => m.NickName );
            Map( m => m.LastName );
            Map( m => m.MiddleName ).Optional();
            Map( m => m.Salutation ).Optional();
            Map( m => m.Suffix ).Optional();
            Map( m => m.Email );
            Map( m => m.Gender );
            Map( m => m.MaritalStatus );
            Map( m => m.Birthdate );
            Map( m => m.AnniversaryDate ).Optional();
            Map( m => m.RecordStatus );
            Map( m => m.InactiveReasonNote ).Optional();
            Map( m => m.InactiveReason ).Optional();
            Map( m => m.ConnectionStatus );
            Map( m => m.EmailPreference ).Optional();
            Map( m => m.CreatedDateTime ).Optional();
            Map( m => m.ModifiedDateTime ).Optional();
            Map( m => m.PersonPhotoUrl ).Optional();
            Map( m => m.Campus.CampusId );
            Map( m => m.Campus.CampusName );
            Map( m => m.Note ).Optional();
            Map( m => m.Grade ).Optional();
            Map( m => m.GiveIndividually ).Optional();
            Map( m => m.IsDeceased ).Optional();
            Map( m => m.IsEmailActive ).Optional();
            Map( m => m.PreviousPersonIds ).Optional();
        }
    }
}