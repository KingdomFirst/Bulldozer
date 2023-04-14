using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the address import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Import person phones.
        /// </summary>
        /// <returns></returns>
        public int ImportPersonPhoneList()
        {
            var unmatchedphones = PersonPhoneCsvList.Where( ph => !PersonCsvList.Select( p => p.Id ).Any( i => i == ph.PersonId ) );
            var additionalPhones = unmatchedphones.Where( ph => PersonDict.ContainsKey( ImportInstanceFKPrefix + "_" + ph.PersonId ) ).ToList();
            if ( additionalPhones.Any() )
            {
                // Convert to phone import list
                var phoneImportList = additionalPhones
                .Select( p => new PhoneNumberImport
                {
                    NumberTypeValueId = this.PhoneNumberTypeDVDict[p.PhoneType].Id,
                    Number = p.PhoneNumber,
                    IsMessagingEnabled = p.IsMessagingEnabled ?? false,
                    IsUnlisted = p.IsUnlisted ?? false,
                    CountryCode = p.CountryCode,
                    Extension = p.Extension,
                    PersonId = p.PersonId.ToIntSafe( 0 ),
                    PhoneId = p.PhoneId
                } )
                .ToList();

                ReportProgress( 0, string.Format( "Begin processing {0} address records tied to previously imported people...", phoneImportList.Count ) );
                return ImportPersonPhoneList( phoneImportList );
            }
            return 0;
        }
        /// <summary>
        /// Import person phones.
        /// </summary>
        /// <param name="phoneImports">The person address imports.</param>
        /// <returns></returns>
        public int ImportPersonPhoneList( List<PhoneNumberImport> phoneImports )
        {
            // Slice data into chunks and process
            var phonesRemainingToProcess = phoneImports.Count;
            var phonesCompleted = 0;

            while ( phonesRemainingToProcess > 0 )
            {
                if ( phonesCompleted > 0 && phonesCompleted % ( this.PersonChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} Person Phone records processed.", phonesCompleted ) );
                }

                if ( phonesCompleted % ( this.PersonChunkSize ) < 1 )
                {
                    var phoneChunk = phoneImports.Take( Math.Min( this.PersonChunkSize, phoneImports.Count ) ).ToList();
                    phonesCompleted += BulkPhoneImport( phoneChunk );
                    phonesRemainingToProcess -= phoneChunk.Count;
                    phoneImports.RemoveRange( 0, phoneChunk.Count );
                    ReportPartialProgress();
                }
            }
            return phonesCompleted;
        }

        /// <summary>
        /// Bulk import of phone numbers.
        /// </summary>
        /// <param name="phoneImports">The person phone imports.</param>
        /// <returns></returns>
        public int BulkPhoneImport( List<PhoneNumberImport> phoneImports )
        {
            var rockContext = new RockContext();
            var importDateTime = RockDateTime.Now;
            var phoneNumbersToInsert = new List<PhoneNumber>();

            foreach ( var phoneNumberImport in phoneImports )
            {

                var newPhoneNumber = new PhoneNumber();
                newPhoneNumber.PersonId = PersonDict[ImportInstanceFKPrefix + "_" + phoneNumberImport.PersonId].Id;
                UpdatePhoneNumberFromPhoneNumberImport( phoneNumberImport, newPhoneNumber, importDateTime );
                phoneNumbersToInsert.Add( newPhoneNumber );
            }

            rockContext.BulkInsert( phoneNumbersToInsert );
            return phoneImports.Count;
        }
    }
}
