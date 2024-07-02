using Rock.Model;
using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class PersonCsv
    {
        private string _familyRole = string.Empty;
        private FamilyRole _familyRoleEnum = CSV.CSVInstance.FamilyRole.Adult;

        private string _gender = string.Empty;
        private Gender _genderEnum = Rock.Model.Gender.Unknown;

        private string _recordStatus = string.Empty;
        private RecordStatus _recordStatusEnum = CSV.CSVInstance.RecordStatus.Active;
        private bool _isValidRecordStatus = false;

        private string _emailPreference = string.Empty;
        private EmailPreference _emailPreferenceEnum = Rock.Model.EmailPreference.EmailAllowed;
        private bool _isValidEmailPreference = false;

        public readonly int DefaultBirthdateYear;

        public string Id { get; set; }

        public string FamilyId { get; set; }

        public string FamilyName { get; set; }

        public string FamilyImageUrl { get; set; }

        public string FamilyRole
        {
            get
            {
                return _familyRole;
            }
            set
            {
                _familyRole = value;
                Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _familyRoleEnum );
            }
        }

        public FamilyRole? FamilyRoleEnum
        {
            get
            {
                return _familyRoleEnum;
            }
            set
            {
                _familyRoleEnum = value.Value;
                _familyRole = _familyRoleEnum.ToString();
            }
        }

        public string FirstName { get; set; }

        public string NickName { get; set; }

        public string LastName { get; set; }

        public string MiddleName { get; set; }

        public string Salutation { get; set; }

        public string Suffix { get; set; }

        public string Email { get; set; }

        public string Gender
        {
            get
            {
                return _gender;
            }
            set
            {
                _gender = value;
                Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _genderEnum );
            }
        }

        public Gender? GenderEnum
        {
            get
            {
                return _genderEnum;
            }
            set
            {
                _genderEnum = value.Value;
                _gender = _genderEnum.ToString();
            }
        }

        public string MaritalStatus { get; set; }

        public DateTime? Birthdate { get; set; }

        public DateTime? AnniversaryDate { get; set; }

        public string RecordStatus
        {
            get
            {
                return _recordStatus;
            }
            set
            {
                _recordStatus = value;
                _isValidRecordStatus = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _recordStatusEnum );
            }
        }

        public RecordStatus? RecordStatusEnum
        {
            get
            {
                return _recordStatusEnum;
            }
            set
            {
                _recordStatusEnum = value.Value;
                _recordStatus = _recordStatusEnum.ToString();
            }
        }

        public bool IsValidRecordStatus
        {
            get
            {
                return _isValidRecordStatus;
            }
        }

        public string InactiveReasonNote { get; set; }

        public string InactiveReason { get; set; }

        public string ConnectionStatus { get; set; }

        public string EmailPreference
        {
            get
            {
                return _emailPreference;
            }
            set
            {
                _emailPreference = value;
                _isValidEmailPreference = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _emailPreferenceEnum );
            }
        }

        public EmailPreference? EmailPreferenceEnum
        {
            get
            {
                return _emailPreferenceEnum;
            }
            set
            {
                _emailPreferenceEnum = value.Value;
                _emailPreference = _emailPreferenceEnum.ToString();
            }
        }

        public bool IsValidEmailPreference
        {
            get
            {
                return _isValidEmailPreference;
            }
        }

        public DateTime? CreatedDateTime { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public string PersonPhotoUrl { get; set; }

        public CampusCsv Campus { get; set; }

        public string Note { get; set; }

        public string Grade { get; set; }

        public int? GraduationYear { get; set; }

        public bool? GiveIndividually { get; set; }

        public bool? IsDeceased { get; set; }

        public bool? IsEmailActive { get; set; }

        public string PreviousPersonIds { get; set; }

    }
}