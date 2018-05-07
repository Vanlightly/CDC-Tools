USE [CdcToRedshift]
GO

INSERT INTO [dbo].[Person]([PersonId],[FirstName],[Surname],[DateOfBirth])
VALUES
(1, 'Jack','Hopkins','19970126'),
(2, 'Jon','Burlington','19880123'),
(3, 'James','Hackney','19640521'),
(4, 'Jim','Smith','19551009')

INSERT INTO [dbo].[Person]([PersonId],[FirstName],[Surname],[DateOfBirth])
VALUES
(5, 'Kerry','Harry','19780503'),
(6, 'Katie','Jones','19720918'),
(7, 'Kelly','Maguire','20011201'),
(8,'Kathryn','May','19520702')

DELETE FROM [dbo].[Person]
WHERE FirstName LIKE 'J%'

UPDATE [dbo].[Person]
SET DateOfBirth = '19700228'
WHERE FirstName = 'Jack'

INSERT INTO [dbo].[PersonAddress]([AddressId],[Addressline1],[City],[Country],[Postalcode],[PersonId])
     VALUES
(1, '12 Ocean Drive', 'Los Angeles', 'USA', '90210', 1),
(2, '13 Mullholand Drive', 'Los Angeles', 'USA', '92340', 2),
(3, '99 Seaview Road', 'Southampton', 'SO12 4GH', 'UK', 3),
(4, '15 Woodland Close', 'Birmingam', 'BR34 D2R', 'UK', 4)

INSERT INTO [dbo].[PersonAddress]([AddressId],[Addressline1],[City],[Country],[Postalcode],[PersonId])
     VALUES
(5, '117 Fish Street', 'Lancaster', 'LA23 4BH', 'UK', 5),
(6, '3 Whale Road', 'Glasgow', 'GL23 6TR', 'UK', 6),
(7, '4 Orca Street', 'Perth', '12345', 'Australia', 7),
(8, '14 Narwhal Drive', 'Sydney', '54321','Australia', 8)

DELETE FROM [dbo].[PersonAddress]