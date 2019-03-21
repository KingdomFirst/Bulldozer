### Binary Data

The Binary File import works with three different doc types: `Person Images`, `Transaction Images` (check images), and `Ministry Documents` (any personal documents).  You'll upload a ZIP file to Bulldozer named by one of those doc types: `PersonImages.zip`, or multiple `PersonImages 2009.zip`, etc if the ZIP file is over 1.5GB.

Those files will be mapped to Rock.BinaryFiles and any respective Transaction/Person links: Transaction images mapped to their transaction, Person images mapped to their person, and Ministry Documents mapped to a Person attribute without a category (you'll need to set security and category for each attribute).  Background checks are included from F1 in the Individual Requirements document folder.  The easiest option is to import everything to the Database storage provider, but Bulldozer can import to a file share or AWS if the Binary File Type is created ahead of time and the storage provider is set via `Bulldozer.exe.config`. 

1. Check Images
    - Should be named by the ForeignId of the transaction, e.g. 592572969.tif.
    - This will create a FinancialTransactionImage and add it to the matching FinancialTransaction. 
2. Person Images
   - Should be named by the ForeignId of the person, e.g. 692015.jpg.
   - This will create a BinaryFile of the type Person Image and set the Person.PhotoId to this image.
3. Ministry Documents
   - All other document types, including Background Check files.

Possible known issues:
    - 1.5GB ZIP file limitation
    - fragile filesystem imports (permission issues that were/are hard to replicate).  
    
    