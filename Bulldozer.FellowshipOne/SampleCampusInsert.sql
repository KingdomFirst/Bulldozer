/* ================================================================ *
	This script adds custom campuses to the base Rock install so other
	fields can use the name and shortcodes as lookup values.

	Rock v8.7
* ================================================================ */

DECLARE @True bit = 1
DECLARE @False bit = 0
DECLARE @Order int = 0
DECLARE @CampusLocationType int = ( SELECT [Id] FROM [DefinedValue] WHERE [Guid] = 'C0D7AE35-7901-4396-870E-3AAF472AAE88' )


-- Insert Campus Locations
INSERT [dbo].[Location] ([Name], [Guid], [IsActive], [LocationTypeValueId])
VALUES
	('Online', NEWID(), @True, @CampusLocationType),
	('Downtown', NEWID(), @True, @CampusLocationType),
	('West Side', NEWID(), @True, @CampusLocationType),
	('East Side', NEWID(), @True, @CampusLocationType)


-- Insert Additional Campuses
INSERT [dbo].[Campus] ([IsSystem], [Name], [ShortCode], [Guid], [IsActive])
VALUES 
	(@False, 'Online', 'Online', NEWID(), @True),
	(@False, 'Downtown', 'DT', NEWID(), @True),
	(@False, 'West Side', 'WS', NEWID(), @True),
	(@False, 'East Side', 'ES', NEWID(), @True)


-- Update Campus Location
UPDATE c
SET c.[LocationId] = l.[Id]
FROM [dbo].[Campus] c
JOIN [dbo].[Location] l ON c.[Name] LIKE l.[Name]
