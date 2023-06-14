using System;
using Rock.Model;
using System.Collections.Generic;

namespace Bulldozer.Model
{
    public class PersonImport
    {
        public int? PersonForeignId { get; set; }

        public string PersonForeignKey { get; set; }

        public int? FamilyForeignId { get; set; }

        public string FamilyForeignKey { get; set; }

        public int GroupRoleId { get; set; }

        public bool? GivingIndividually { get; set; }

        public int? RecordTypeValueId { get; set; }

        public int? RecordStatusValueId { get; set; }

        public DateTime? RecordStatusLastModifiedDateTime { get; set; }

        public int? RecordStatusReasonValueId { get; set; }

        public int? ConnectionStatusValueId { get; set; }

        public int? ReviewReasonValueId { get; set; }

        public bool IsDeceased { get; set; }

        public int? TitleValueId { get; set; }

        public string FirstName { get; set; }

        public string NickName { get; set; }

        public string MiddleName { get; set; }

        public string LastName { get; set; }

        public int? SuffixValueId { get; set; }

        public int? BirthDay { get; set; }

        public int? BirthMonth { get; set; }

        public int? BirthYear { get; set; }

        public Gender? Gender { get; set; }

        public int? MaritalStatusValueId { get; set; }

        public DateTime? AnniversaryDate { get; set; }

        public int? GraduationYear { get; set; }

        public string Grade { get; set; }

        public string Email { get; set; }

        public bool IsEmailActive { get; set; }

        public string EmailNote { get; set; }

        public EmailPreference EmailPreference { get; set; }

        public string InactiveReasonNote { get; set; }

        public ICollection<PhoneNumberImport> PhoneNumbers { get; set; }

        public ICollection<GroupAddressImport> Addresses { get; set; }

        public ICollection<AttributeValueImport> AttributeValues { get; set; }

        public DateTime? CreatedDateTime { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public string Note { get; set; }

        public List<int> PreviousPersonIds { get; set; } = new List<int>();
    }

}