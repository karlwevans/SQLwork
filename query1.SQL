CREATE MULTISET VOLATILE TABLE vt_****** AS(
SELECT 
table1.ID1
,table1.ID2
,table1.ID3
,table1.Pop
,table1.ID4
,table1.year
,table2.ID5
,table3.email
,table4.name
,table4.fullname
,table5.name2
,table5.name3
,table5.email2
,table6.postcode
,MAX(CASE WHEN table7.AddressCode = 5 THEN table7.postcode2 END) AS Residential_PC
,MAX(CASE WHEN table7.AddressCode = 15 THEN table7.postcode2 END) AS Business_PC
,CASE WHEN postcode IN (****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,
****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****) THEN 1 ELSE NULL END AS flag1 
, CASE WHEN Residential_PC IN (****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,
****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****) THEN 1 ELSE NULL END AS flag2  
,CASE WHEN Business_PC IN (****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,
****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****,****) THEN 1 ELSE NULL END AS flag3
,CASE WHEN postcode LIKE '4%'THEN 1 ELSE NULL END AS flag4
,CASE WHEN Residential_PC LIKE '*%'THEN 1 ELSE NULL END AS flag5
,CASE WHEN Business_PC LIKE '*%'THEN 1 ELSE NULL END AS flag6
,COALESCE(flag1,flag2,flag3, flag4, flag5, flag6) AS finalflag 
FROM database1.tableA AS table1

LEFT JOIN (SELECT ID1
			, email
            FROM database2.tableB 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY ID1 ORDER BY Seq_Num DESC, Crtd_DtTm) = 1)AS table3
ON table1.ID1 = table3.ID1

LEFT JOIN (SELECT 
        name
        , fullname
        , ID1
        FROM database2.TableC
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ID1 ORDER BY Seq_Num DESC, Crtd_DtTm) = 1) AS table4
ON table1.ID1 = table4.ID1

LEFT JOIN (SELECT 
        ID1
        , AddressCode
        , postcode2
        FROM database2.TableD 
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ID1, AddressCode ORDER BY Seq_Num DESC, Crtd_DtTm DESC)=1) AS table7
ON table1.ID1=table7.ID1

LEFT JOIN(SELECT
    u.ID1
    , postcode
    ,u.year
    ,ID4
    FROM database2.TableE AS f
    JOIN database3.TableF AS ad
        ON f.ID4=ad.ID4
    JOIN database1.TableA AS U
        ON U.ID1 = f.ID1
        AND U.ID2=f.ID2) AS table6
ON table1.ID1=table6.ID1

LEFT JOIN (SELECT 
        u.ID1
        ,dc.ID5
        FROM database1.tableA AS U
        JOIN database2.TableG AS DC
        ON u.ID1=DC.ID1) AS table2
ON table1.ID1=table2.ID1

LEFT JOIN (SELECT 
            a.ID4 
            ,MAX(b.name) AS name2
            , MAX(a.name) AS name3
            , MAX(a.email) AS email2
            FROM database3.TableF AS a
            JOIN database2.TableC AS b
            ON a.ID1=b.ID1
            GROUP BY 1) AS table5
ON table1.ID4=table5.ID4
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
) WITH data
ON COMMIT PRESERVE ROWS
;
