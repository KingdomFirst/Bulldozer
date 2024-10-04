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
                person.EmailPreference = personImport.EmailPreference;
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
                business.EmailPreference = businessImport.EmailPreference;
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
            else
            {
                group.CreatedDateTime = importedDateTime;
            }

            group.Order = groupImport.Order.GetValueOrDefault();
            group.CampusId = groupImport.CampusId;
            group.IsActive = groupImport.IsActive;
            group.IsPublic = groupImport.IsPublic;
            group.GroupCapacity = groupImport.Capacity;
            group.ModifiedDateTime = importedDateTime;
            group.Guid = Guid.NewGuid();
        }

        private void InitializeCategoryFromCategoryCsv( Category category, CategoryCsv categoryCsv, DateTime importedDateTime )
        {
            category.Name = categoryCsv.Name;
            category.Description = categoryCsv.Description;
            category.EntityTypeQualifierColumn = categoryCsv.EntityTypeQualifierColumn;
            category.EntityTypeQualifierValue = categoryCsv.EntityTypeQualifierValue;
            category.IconCssClass = categoryCsv.IconCssClass;
            category.ForeignKey = $"{this.ImportInstanceFKPrefix}^{categoryCsv.Id}";
            category.ForeignId = categoryCsv.Id.AsIntegerOrNull();
            category.CreatedDateTime = importedDateTime;
            category.Guid = Guid.NewGuid();
        }

        public List<Tuple<string, Dictionary<string, string>>> GetAttributeDefinedValuesDictionary( RockContext rockContext, int attributeEntityTypeId )
        {
            var definedTypeDict = this.DefinedTypeDict.Values.ToDictionary( k => k.Id, v => v );
            var attributeDefinedValuesDict = new AttributeService( rockContext ).Queryable()
                                                                                .Where( a => a.FieldTypeId == DefinedValueFieldTypeId && a.EntityTypeId == attributeEntityTypeId )
                                                                                .AsEnumerable()
                                                                                .Select( a => new Tuple<string, Dictionary<string, string>>(
                                                                                    a.Key,
                                                                                    definedTypeDict
                                                                                        .GetValueOrNull( a.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value.AsIntegerOrNull().Value )?
                                                                                        .DefinedValues.DistinctBy( dv => dv.Value )
                                                                                        .ToDictionary( d => d.Value, d => d.Guid.ToString() ) )
                                                                                )
                                                                                .ToList();
            return attributeDefinedValuesDict;
        }

        public List<Tuple<int,int>> GetAttributeValueLookup( RockContext rockContext, int attributeEntityTypeId = -1 )
        {
            var attributeValueLookup = new AttributeValueService( rockContext )
                                        .Queryable()
                                        .Where( v => v.EntityId.HasValue && ( attributeEntityTypeId == -1 || v.Attribute.EntityTypeId == attributeEntityTypeId ) )
                                        .Select( v => new { v.AttributeId, EntityId = v.EntityId.Value } )
                                        .AsEnumerable()
                                        .Select( v => new Tuple<int, int>( v.AttributeId, v.EntityId ) )
                                        .ToList();
            return attributeValueLookup;
        }

        /// <summary>
        /// Get the Group Type Id by testing int, guid, and name.
        /// If not found, return the General Group Type Id.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        /// <param name="groupType">The type.</param>
        /// <returns></returns>
        private static int? LoadGroupTypeId( RockContext lookupContext, string groupType, string foreignKeyPrefix, bool includeDefault )
        {
            int typeId;
            int? groupTypeId = null;

            //
            // Try to figure out what group type they want, we accept Rock Group Type ID numbers,
            // GUIDs and type names.
            //
            if ( int.TryParse( groupType, out typeId ) )
            {
                groupTypeId = GroupTypeCache.Get( typeId, lookupContext ).Id;
            }
            else if ( Guid.TryParse( groupType, out Guid groupTypeGuid ) )
            {
                groupTypeId = GroupTypeCache.Get( groupTypeGuid, lookupContext ).Id;
            }
            else
            {
                var groupTypeByName = new GroupTypeService( lookupContext ).Queryable().AsNoTracking().FirstOrDefault( gt => gt.Name.Equals( groupType, StringComparison.OrdinalIgnoreCase ) );
                if ( groupTypeByName != null )
                {
                    groupTypeId = groupTypeByName.Id;
                }
                else
                {
                    var groupTypeByKey = new GroupTypeService( lookupContext ).Queryable().AsNoTracking().FirstOrDefault( gt => gt.ForeignKey.Equals( foreignKeyPrefix + "^" + groupType ) );
                    if ( groupTypeByKey != null )
                    {
                        groupTypeId = groupTypeByKey.Id;
                    }
                }
            }

            if ( !groupTypeId.HasValue && includeDefault )
            {
                //
                // Default to the "General Groups" type if we can't find what they want.
                //
                groupTypeId = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_GENERAL.AsGuid(), lookupContext ).Id;
            }

            return groupTypeId;
        }

        public List<Category> GetCategoriesByFKOrName( RockContext lookupContext, string categoryForeignKey, string categoryName, List<Category> categoryLookup = null, bool searchByName = true, int? entityTypeId = null )
        {
            List<Category> result = new List<Category>();
            if ( categoryLookup == null )
            {
                categoryLookup = new CategoryService( lookupContext )
                                        .Queryable()
                                        .AsNoTracking()
                                        .Where( c => !entityTypeId.HasValue || c.EntityTypeId == entityTypeId.Value )
                                        .ToList();
            }

            if ( categoryForeignKey.IsNotNullOrWhiteSpace() )
            {
                result = categoryLookup.Where( c => c.ForeignKey == categoryForeignKey ).ToList();
            }

            if ( result.Count == 0 && searchByName && categoryName.IsNotNullOrWhiteSpace() )
            {
                result = categoryLookup.Where( c => c.Name == categoryName ).ToList();
            }

            return result;
        }

        public MetricPartition CreateMetricPartition( string entityTypeName, int metricId, string partitionLabel, string partitionForeignKey, List<EntityTypeCache> entityTypes, bool partitionRequired = false, int partitionOrder = 0 )
        {
            MetricPartition newMetricPartition = null;
            if ( entityTypes.Count == 0 )
            {
                entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();
            }
            var entityType = entityTypes.FirstOrDefault( et => et.Name.Equals( entityTypeName ) );
            if ( entityType != null )
            {
                newMetricPartition = new MetricPartition
                {
                    MetricId = metricId,
                    Label = partitionLabel.IsNotNullOrWhiteSpace() ? partitionLabel : entityType.FriendlyName,
                    EntityTypeId = entityType.Id,
                    ForeignKey = partitionForeignKey,
                    IsRequired = partitionRequired,
                    Order = partitionOrder
                };
            }

            return newMetricPartition;
        }

        public MetricValuePartitionImport CreateMetricValuePartitionImport(  MetricValueCsv metricValueCsv, MetricValue metricValue, string PartitionEntityId, int partitionNumber, Dictionary<string,MetricPartition> metricPartitionLookup, Dictionary<string, MetricValuePartition> metricValuePartitionLookup )
        {
            MetricValuePartitionImport newMetricPartition = null;
            var metricPartition = metricPartitionLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricValueCsv.MetricId}_{partitionNumber}" );
            var metricValuePartitionForeignKey = $"{this.ImportInstanceFKPrefix}^{metricValueCsv.MetricId}_{partitionNumber}_{metricValueCsv.Id}";
            var existingMetricValuePartion = metricValuePartitionLookup.GetValueOrNull( metricValuePartitionForeignKey );
            if ( metricPartition != null && existingMetricValuePartion == null )
            {
                newMetricPartition = new MetricValuePartitionImport
                {
                    MetricValue = metricValue,
                    MetricPartition = metricPartition,
                    EntityId = PartitionEntityId,
                    CsvMetricValueId = metricValueCsv.Id,
                    ForeignKey = metricValuePartitionForeignKey
                };
            }

            return newMetricPartition;
        }
    }
}
