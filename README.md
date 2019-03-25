# Welcome to Bulldozer!

![](/Bulldozer.png)

Bulldozer converts data into [Rock RMS](http://www.rockrms.com/) from other church management systems.

Imports from FellowshipOne and CSV are currently supported.

## What You'll Need To Get Started

- Read the [wiki](https://github.com/KingdomFirst/Bulldozer/wiki) to know how to use each component
- A local or hosted version of Rock
- A login to the Rock SQL database
- Your old database in a supported format
- Bulldozer files ( see [Bulldozer Releases](https://github.com/KingdomFirst/Bulldozer/releases) )

## What If I Have A Problem?
- If you have a problem with the import, please file an issue on Github: [Bulldozer Issues](https://github.com/KingdomFirst/Bulldozer/issues)
- Please include the type of import (F1 or CSV) in the title and your Windows environment settings in the body.
- Example issue: "CSV: Can't import prefix with special characters"

Please note that the master branch contains fully-tested code; develop branch is beta/in-progress.

## Does Bulldozer crash after opening or trying to use it?

Downloaded .exe and .dll files on Windows are blocked to prevent users from accidentally downloading and running malicious applications. If you've just downloaded Bulldozer.zip, follow the guide below to unblock all the contents of the zip file at once.

If you've already extracted the contents of Bulldozer.zip, make sure to unblock each file by doing the following:

For each .exe and .dll file in the directory:

1. Right-click on the individual file
2. Select Properties to open the properties window.
3. Check the "Unblock" checkbox.
4. Click "Apply".
5. Click "OK".

![Unblock each file in the directory.](/UnblockTutorialAnimation.gif?raw=true "Optional Title")

## Attribute Processing
### Attribute Caret Parsing Logic
```
                    if ( pairs.Length == 1 )
                    {
                        attributeName = pairs[0];
                    }
                    else if ( pairs.Length == 2 )
                    {
                        attributeName = pairs[0];
                        attributeTypeString = pairs[1];
                    }
                    else if ( pairs.Length >= 3 )
                    {
                        categoryName = pairs[1];
                        attributeName = pairs[2];
                        if ( pairs.Length >= 4 )
                        {
                            attributeTypeString = pairs[3];
                        }
                        if ( pairs.Length >= 5 )
                        {
                            attributeForeignKey = pairs[4];
                        }
                        if ( pairs.Length >= 6 )
                        {
                            definedValueForeignIdString = pairs[5];
                        }
                    }
```
### Attribute Caret Parsing in plain English
1. If no caret is in the Attribute Name (`Baptism Pastor`), just tries to match to an existing attribute by the name.  If created, it's created as `Text`
2. If only one caret is in the Attribute Name (`Baptism Pastor^V`), the Attribute Name and Attribute Type are used.
3. If two carets are in the Attribute Name (`^Membership^Baptism Pastor`), Attribute Category and Attribute Name are used.
4. If three carets are in the Attribute Name (`^Membership^Baptism Pastor^V`), Attribute Category, Attribute Name, and Attribute Type are used.
5. If four carets are in the Attribute Name (`^Membership^Baptism Pastor^V^123`), Attribute Category, Attribute Name, Attribute Type, and Attribute Foreign Key are used.
6. If five carets are in the Attribute Name (`^Membership^Baptism Pastor^V^123^456`), Attribute Category, Attribute Name, Attribute Type, Attribute Foreign Key, and Defined Type Foreign Key are used.

### Notes
The power of the five carets is that the Defined Type list created for an attribute can be used by multiple Defined Value Attributes.  This is helpful if there are attributes used as surveys which have the same options used multiple times.

## License
### Copyright 2019 by Kingdom First Solutions  

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0  

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
