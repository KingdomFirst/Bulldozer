using Rock.Model;
using System;
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

        public Gender? Gender { get; set; }

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

    }
}