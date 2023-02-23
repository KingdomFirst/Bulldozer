using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class PersonSearchKeyCsv
    {
        public string PersonId { get; set; }

        public string SearchValue { get; set; }

        public PersonSearchKeyType SearchType { get; set; }

    }
}