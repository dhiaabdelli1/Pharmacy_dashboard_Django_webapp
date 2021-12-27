ClientQuery="""
SELECT [ClientPK]
      ,[CNSS]
  FROM [pharma_DW].[dbo].[DimClient]
"""

VenteQuery = """SELECT * 
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimClient C on V.ClientPK = C.ClientPK
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
   INNER JOIN DimMutuelle M on V.MutuellePK = M.MutuellePK 
    INNER JOIN DimDate D on V.DatePK = D.DatePK  """

MonthlySalesQuery = """SELECT Date, A.PrixVenteTTC*V.Qte as Montant

  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimClient C on V.ClientPK = C.ClientPK
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
   INNER JOIN DimMutuelle M on V.MutuellePK = M.MutuellePK 
    INNER JOIN DimDate D on V.DatePK = D.DatePK """


CategorySalesQuery = """SELECT TOP 5 A.LibelleCategorie, SUM(ROUND(A.PrixVenteTTC*V.Qte,2)) as Montant
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimClient C on V.ClientPK = C.ClientPK
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
   INNER JOIN DimMutuelle M on V.MutuellePK = M.MutuellePK 
    INNER JOIN DimDate D on V.DatePK = D.DatePK GROUP BY A.LibelleCategorie ORDER BY Montant DESC
"""
ProductSalesQuery = """SELECT TOP 10 A.Designation,A.LibelleCategorie,A.PrixVenteTTC, SUM(A.PrixVenteTTC*V.Qte) as Montant, SUM(V.Qte) as Quantity
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimClient C on V.ClientPK = C.ClientPK
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
   INNER JOIN DimMutuelle M on V.MutuellePK = M.MutuellePK 
    INNER JOIN DimDate D on V.DatePK = D.DatePK GROUP BY A.Designation, A.LibelleCategorie, A.PrixVenteTTC ORDER BY Montant DESC"""

LaboSalesQuery = """SELECT  A.LibelleForme,SUM(A.PrixVenteTTC*V.Qte) as Montant, Month, Year
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
  INNER JOIN DimDate D on V.DatePK = D.DatePK 
  WHERE A.LibelleForme in (SELECT TOP 5 A.LibelleForme
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK GROUP BY A.LibelleForme ORDER BY SUM(A.PrixVenteTTC*V.Qte) DESC ) 
  GROUP BY A.LibelleForme, Month, Year ORDER BY Month,Year"""

totalSalesQuery = """
SELECT  SUM(A.PrixVenteTTC*V.Qte) as TOTAL
  FROM [pharma_DW].[dbo].[FactVente] V 
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK ;
  """

CategoryStockQuery="""SELECT [LibelleCategorie]
      ,SUM(ABS([Qte])) Qte
	  ,Month
	  ,Year
  FROM [pharma_DW].[dbo].[FactStock] as fs
  inner join DimDate as d on fs.DatePK=d.DatePK
  inner join DimArticle as a on fs.ArticlePK=a.ArticlePK
  WHERE LibelleCategorie in (SELECT TOP 5 LibelleCategorie  FROM [pharma_DW].[dbo].[FactStock] as fs
  inner join DimArticle as a on fs.ArticlePK=a.ArticlePK group by LibelleCategorie order by SUM(ABS([Qte])) DESC)
  group by LibelleCategorie, Month, Year order by Month, Year"""

InOutQuery="""SELECT Month
		,Year
	,[TypeMouvement]
	  ,SUM([Qte]) Qte
  FROM [pharma_DW].[dbo].[FactStock] as fs
  inner join DimDate as d on fs.DatePK=d.DatePK
  inner join DimArticle as a on fs.ArticlePK=a.ArticlePK
  where TypeMouvement <>'Modification'
  group by Month,Year,TypeMouvement"""

Stock2016Query="""
SELECT Top 8
Designation
    ,sum([AncienStock]) ancien
   ,sum([NouveauStock]) nouveau

  FROM [pharma_DW].[dbo].[FactStock] as c
  inner join [pharma_DW].dbo.DimDate as a on a.DatePk=c.DatePK
  inner join pharma_DW.DBO.DimArticle as b on b.ArticlePK=c.ArticlePK
  where year='2016'
  group by year, Designation order by sum([NouveauStock]) desc 
"""


MonthlyPurchaseQuery= """
SELECT Date, A.PrixAchatTTC*C.Qte as Montant

  FROM [pharma_DW].[dbo].[FactAchat] C
  INNER JOIN DimArticle A on C.ArticlePK = A.ArticlePK 
   INNER JOIN DimFournisseur F on C.FournisseurPK = F.FournisseurPK 
    INNER JOIN DimDate D on C.DatePK = D.DatePK
"""

CategoryPurchaseQuery = """
SELECT TOP 5 A.LibelleCategorie, SUM(ROUND(A.PrixAchatTTC*V.Qte,2)) as Montant
  FROM [pharma_DW].[dbo].[FactAchat] V 

  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
  inner join DimFournisseur b on b.FournisseurPK=V.FournisseurPK
  inner join DimPersonnel c on c.PersonnelPK=V.PersonnelPK
    INNER JOIN DimDate D on V.DatePK = D.DatePK GROUP BY A.LibelleCategorie ORDER BY Montant DESC
"""

ProductPurchasesQuery = """
SELECT TOP 10 A.Designation,A.LibelleCategorie,A.PrixAchatTTC, SUM(A.PrixAchatTTC*V.Qte) as Montant, SUM(V.Qte) as Quantity
  FROM [pharma_DW].[dbo].[FactAchat] V 

  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
   INNER JOIN DimFournisseur B ON B.FournisseurPK=V.FournisseurPK
   inner join DimPersonnel p on p.PersonnelPK=V.PersonnelPK
    INNER JOIN DimDate D on V.DatePK = D.DatePK GROUP BY A.Designation, A.LibelleCategorie, A.PrixAchatTTC ORDER BY Montant DESC
"""

queryFournisseur="""
SELECT f.NomFournisseur, SUM(A.PrixAchatTTC*V.Qte) as Montant, Month, Year
  FROM [pharma_DW].[dbo].[FactAchat] V 
  INNER JOIN DimArticle A on V.ArticlePK = A.ArticlePK 
  INNER JOIN DimDate D on V.DatePK = D.DatePK 
  inner join DimFournisseur f on F.FournisseurPK=V.FournisseurPK
  WHERE F.NomFournisseur in (SELECT TOP 5 F.NomFournisseur
  FROM [pharma_DW].[dbo].[FactAchat] V 
  INNER JOIN DimFournisseur F on V.FournisseurPK = F.FournisseurPK
  inner join DimArticle A on A.ArticlePK=V.ArticlePK
   Group BY F.NomFournisseur ORDER BY SUM(A.PrixAchatTTC*V.Qte) DESC ) 
  GROUP BY F.NomFournisseur, Month, Year ORDER BY Month,Year

"""

