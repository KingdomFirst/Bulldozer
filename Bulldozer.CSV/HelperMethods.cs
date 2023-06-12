using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Bulldozer.Utility.CachedTypes;

namespace Bulldozer.CSV
{
    partial class CSVComponent : BulldozerComponent
    {
        /// <summary>
        /// Updates the person properties from person import.
        /// </summary>
        /// <param name="personImport">The person import.</param>
        /// <param name="person">The person.</param>
        /// <param name="recordTypePersonId">The Id for the Person RecordType.</param>
        private string InitializePersonFromPersonImport( PersonImport personImport, Person person )
        {
            var errors = string.Empty;
            person.RecordTypeValueId = personImport.RecordTypeValueId ?? PersonRecordTypeId;
            person.RecordStatusValueId = personImport.RecordStatusValueId;
            person.RecordStatusLastModifiedDateTime = personImport.RecordStatusLastModifiedDateTime.ToSQLSafeDate();
            person.RecordStatusReasonValueId = personImport.RecordStatusReasonValueId;
            person.ConnectionStatusValueId = personImport.ConnectionStatusValueId;
            person.ReviewReasonValueId = personImport.ReviewReasonValueId;
            person.IsDeceased = personImport.IsDeceased;
            person.TitleValueId = personImport.TitleValueId;
            person.FirstName = personImport.FirstName.FixCase().Left( 50 );
            person.NickName = personImport.NickName.FixCase().Left( 50 );

            if ( person.NickName.IsNullOrWhiteSpace() )
            {
                person.NickName = person.FirstName.Left( 50 );
            }

            if ( person.FirstName.IsNullOrWhiteSpace() )
            {
                person.FirstName = person.NickName.Left( 50 );
            }

            person.MiddleName = personImport.MiddleName.FixCase().Left( 50 );
            person.LastName = personImport.LastName.FixCase().Left( 50 );
            person.SuffixValueId = personImport.SuffixValueId;
            person.BirthDay = personImport.BirthDay;
            person.BirthMonth = personImport.BirthMonth;
            person.BirthYear = personImport.BirthYear;
            person.Gender = ( Gender ) personImport.Gender;
            person.MaritalStatusValueId = personImport.MaritalStatusValueId;
            person.AnniversaryDate = personImport.AnniversaryDate.ToSQLSafeDate();
            person.GraduationYear = personImport.GraduationYear;
            person.Email = personImport.Email.IsNotNullOrWhiteSpace() ? personImport.Email.Left( 75 ) : null;
            person.InactiveReasonNote = personImport.InactiveReasonNote.Left( 1000 );
            person.CreatedDateTime = personImport.CreatedDateTime.ToSQLSafeDate();
            person.ModifiedDateTime = personImport.ModifiedDateTime.ToSQLSafeDate();
            person.SystemNote = personImport.Note;
            person.ForeignId = personImport.PersonForeignId;
            person.ForeignKey = personImport.PersonForeignKey;

            // validate email or Rock will kick it back
            if ( person.Email != null && person.Email.IsValidEmail() && person.Email.IsEmail( this.EmailRegex ) )
            {
                person.IsEmailActive = personImport.IsEmailActive;
                person.EmailNote = personImport.EmailNote.Left( 250 );
                person.EmailPreference = ( EmailPreference ) personImport.EmailPreference;
            }
            else if ( person.Email != null )
            {
                errors += string.Format( "{0},{1},\"{2}\"\r\n", DateTime.Now.ToString(), "InvalidPersonEmail", $"PersonId: {person.ForeignKey.Substring( person.ForeignKey.IndexOf( "_" ) + 1 )} - Email: {person.Email}" );
                person.Email = null;
            }
            return errors;
        }

        /// <summary>
        /// Updates the phone number from phone number import.
        /// </summary>
        /// <param name="phoneNumberImport">The phone number import.</param>
        /// <param name="phoneNumberToInsert">The phone number to insert.</param>
        /// <param name="importDateTime">The import date time.</param>
        private void UpdatePhoneNumberFromPhoneNumberImport( PhoneNumberImport phoneNumberImport, PhoneNumber phoneNumberToInsert, DateTime importDateTime )
        {
            phoneNumberToInsert.NumberTypeValueId = phoneNumberImport.NumberTypeValueId;
            phoneNumberToInsert.CountryCode = phoneNumberImport.CountryCode?.ToString();
            phoneNumberToInsert.Number = PhoneNumber.CleanNumber( phoneNumberImport.Number );
            phoneNumberToInsert.NumberFormatted = PhoneNumber.FormattedNumber( phoneNumberToInsert.CountryCode, phoneNumberToInsert.Number );
            phoneNumberToInsert.Extension = phoneNumberImport.Extension;
            phoneNumberToInsert.IsMessagingEnabled = phoneNumberImport.IsMessagingEnabled;
            phoneNumberToInsert.IsUnlisted = phoneNumberImport.IsUnlisted;
            phoneNumberToInsert.CreatedDateTime = importDateTime;
            phoneNumberToInsert.ModifiedDateTime = importDateTime;
            phoneNumberToInsert.ForeignKey = this.ImportInstanceFKPrefix + "_" + phoneNumberImport.PhoneId;
            phoneNumberToInsert.ForeignId = phoneNumberImport.PhoneId.AsIntegerOrNull();
        }

        /// <summary>
        /// Updates the business properties from business import.
        /// </summary>
        /// <param name="businessImport">The person import.</param>
        /// <param name="business">The person.</param>
        /// <param name="recordTypeBusinessId">The Id for the Business RecordType.</param>
        private string InitializeBusinessFromPersonImport( PersonImport businessImport, Person business, string emailErrors = "" )
        {
            business.RecordTypeValueId = businessImport.RecordTypeValueId ?? BusinessRecordTypeId;
            business.RecordStatusValueId = businessImport.RecordStatusValueId;
            business.RecordStatusLastModifiedDateTime = businessImport.RecordStatusLastModifiedDateTime.ToSQLSafeDate();
            business.RecordStatusReasonValueId = businessImport.RecordStatusReasonValueId;
            business.ConnectionStatusValueId = businessImport.ConnectionStatusValueId;
            business.ReviewReasonValueId = businessImport.ReviewReasonValueId;
            business.IsDeceased = businessImport.IsDeceased;
            business.LastName = businessImport.LastName.FixCase().Left( 50 );
            business.Gender = ( Gender ) businessImport.Gender;
            business.Email = businessImport.Email.IsNotNullOrWhiteSpace() ? businessImport.Email.Left( 75 ) : null;

            // validate email or Rock will kick it back
            if ( business.Email != null && business.Email.IsValidEmail() && business.Email.IsEmail( this.EmailRegex ) )
            {
                business.IsEmailActive = businessImport.IsEmailActive;
                business.EmailNote = businessImport.EmailNote.Left( 250 );
                business.EmailPreference = ( EmailPreference ) businessImport.EmailPreference;
            }
            else if ( business.Email != null )
            {
                emailErrors += string.Format( "{0},{1},\"{2}\"\r\n", DateTime.Now.ToString(), "InvalidBusinessEmail", $"BusinessId: {businessImport.PersonForeignKey.Substring( businessImport.PersonForeignKey.IndexOf( "_" ) + 1 )} - Email: {businessImport.Email}" );
                business.Email = null;
            }
            business.InactiveReasonNote = businessImport.InactiveReasonNote.Left( 1000 );
            business.CreatedDateTime = businessImport.CreatedDateTime.ToSQLSafeDate();
            business.ModifiedDateTime = businessImport.ModifiedDateTime.ToSQLSafeDate();
            business.SystemNote = businessImport.Note;
            business.ForeignId = businessImport.PersonForeignId;
            business.ForeignKey = businessImport.PersonForeignKey;

            return emailErrors;
        }


        private void InitializeGroupFromGroupImport( Group group, GroupImport groupImport, DateTime importedDateTime )
        {
            group.ForeignId = groupImport.GroupForeignId;
            group.ForeignKey = groupImport.GroupForeignKey;
            group.GroupTypeId = groupImport.GroupTypeId;

            if ( groupImport.Name.Length > 100 )
            {
                group.Name = groupImport.Name.Left( 100 );
                group.Description = $"{groupImport.Name} - {groupImport.Description}";
            }
            else
            {
                group.Name = groupImport.Name;
                group.Description = groupImport.Description;
            }

            if ( groupImport.CreatedDate.HasValue )
            {
                group.CreatedDateTime = groupImport.CreatedDate;
            }

            group.Order = groupImport.Order.GetValueOrDefault();
            group.CampusId = groupImport.CampusId;
            group.IsActive = groupImport.IsActive;
            group.IsPublic = groupImport.IsPublic;
            group.GroupCapacity = groupImport.Capacity;
            group.Guid = Guid.NewGuid();
        }
    }
}
