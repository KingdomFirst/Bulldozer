using Rock.Model;
using System;

namespace Bulldozer.Model
{
    public class BusinessContactObject
    {
        public Person Business { get; set; }

        public Person Contact { get; set; }

        public Group BusinessKnownRelationshipGroup { get; set; }

        public Group ContactKnownRelationshipGroup { get; set; }

        public string BusinessRelationshipForeignKey { get; set; }

        public string ContactRelationshipForeignKey { get; set; }

        public Guid? BusinessKnownRelationshipGroupGuid { get; set; }

        public Guid? ContactKnownRelationshipGroupGuid { get; set; }

        public BusinessContactCsv BusinessContactCsv { get; set; }
    }
}