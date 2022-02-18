#MODELAMIENTO DIMENSIONAL - ETL

###Objetivo

Crear un proyecto de ETL utilizando SQL Server Integration Services

### Creación de la base de datos Northwind_Mart
#### Indicadores de análisis
- Análisis comparativos de monto facturado entre categorías por periodos de tiempo
- Analisis geográfico por países de clientes que solicitan ordenes por periodos de tiempo
- Analisis de productos con problemas de aceptación por rangos de tiempo.
- Comparativos entre meses relacionados a monto de facturación.
- Encontrar los periódos de tiempo con menos ingresos (facturación) para la empresa con respecto a las ódenes.

####Tabla de hecho
![](https://i.ibb.co/BC3VjkH/modelo-dimensional.jpg)
Fuente: https://es.calameo.com/read/0044613879ea64bfd0bf4


#### Estructura del modelo dimensional
![](https://i.ibb.co/w4xTDTy/Estructura-del-modelo-dimensinal.jpg)

#### Creacion de tablas 

#####Tabla producto
```
use Northwind;
go

SELECT *
FROM [dbo].[Categories];

--Agregar columna

alter table [dbo].[Categories]
add Grupo_Categoria varchar(20);

-- modificar columna para colocar grupos

update dbo.Categories set Grupo_Categoria = 'Grupo A'
where CategoryID = 1;

update dbo.Categories set Grupo_Categoria = 'Grupo A'
where CategoryID = 3;

update dbo.Categories set Grupo_Categoria = 'Grupo A'
where CategoryID = 5;

update dbo.Categories set Grupo_Categoria = 'Grupo A'
where CategoryID = 7;

update dbo.Categories set Grupo_Categoria = 'Grupo A'
where CategoryID = 8;

update dbo.Categories set Grupo_Categoria = 'Grupo B'
where CategoryID = 6;

update dbo.Categories set Grupo_Categoria = 'Grupo B'
where CategoryID = 2;

update dbo.Categories set Grupo_Categoria = 'Grupo B'
where CategoryID = 4;

--Vista tabla producto

SELECT P.ProductName AS Producto_Nombre, P.UnitPrice AS Producto_PUnitario, C.CategoryName AS Producto_CategoriaNombre,C.Grupo_Categoria As Producto_Categoria_Grupo
FROM [dbo].[Products] AS P
INNER JOIN [dbo].[Categories] AS C
ON P.CategoryID = C.CategoryID;
```

![](https://i.ibb.co/16qZzqN/Tabla-Producto.jpg)

![](https://i.ibb.co/Ny9qS4z/Tabla-Producto-Lista.jpg)


##### Tabla empleados
```
select *
from [dbo].[Employees];

select *
from [dbo].[Employees]
where Title = 'Sales Representative';


update [dbo].[Employees] set Region = 'UK'
where Country = 'UK';

update [dbo].[Employees] set ReportsTo ='1'
where ReportsTo is null;

SELECT CONCAT_WS(' ',E.LastName,E.FirstName) AS Empleado_NombreCompleto, E.[Address] AS Empleado_Direccion, E.City AS Empleado_Ciudad, E.Region AS Empleado_Region, E.Country AS Empleado_Pais, E.ReportsTo AS Empleado_SuperiorSkey
FROM [Northwind].[dbo].[Employees] AS E;
```
![](https://i.ibb.co/hL9fXSc/Tabla-Empleado.jpg)
![](https://i.ibb.co/JF0JHQW/Tabla-Empleado-Lista.jpg)

##### Tabla cliente
```
select *
from [dbo].[Customers];

update [dbo].[Customers] set Region =	'BE'	where City =	'Berlin'	;
update [dbo].[Customers] set Region =	'CMX'	where City =	'México D.F.'	;
update [dbo].[Customers] set Region =	'GB'	where City =	'London'	;
update [dbo].[Customers] set Region =	'BD'	where City =	'Luleå'	;
update [dbo].[Customers] set Region =	'RP'	where City =	'Mannheim'	;
update [dbo].[Customers] set Region =	'GES'	where City =	'Strasbourg'	;
update [dbo].[Customers] set Region =	'Maryland'	where City =	'Madrid'	;
update [dbo].[Customers] set Region =	'PAC'	where City =	'Marseille'	;
update [dbo].[Customers] set Region =	'AR-B'	where City =	'Buenos Aires'	;
update [dbo].[Customers] set Region =	'SER'	where City =	'Bern'	;
update [dbo].[Customers] set Region =	'NW'	where City =	'Aachen'	;
update [dbo].[Customers] set Region =	'PDL'	where City =	'Nantes'	;
update [dbo].[Customers] set Region =	'AT-6'	where City =	'Graz'	;
update [dbo].[Customers] set Region =	'Connecticut'	where City =	'Madrid'	;
update [dbo].[Customers] set Region =	'FHD'	where City =	'Lille'	;
update [dbo].[Customers] set Region =	'Z'	where City =	'Bräcke'	;
update [dbo].[Customers] set Region =	'NW'	where City =	'München'	;
update [dbo].[Customers] set Region =	'I-21'	where City =	'Torino'	;
update [dbo].[Customers] set Region =	'P-11'	where City =	'Lisboa'	;
update [dbo].[Customers] set Region =	'Connecticut'	where City =	'Barcelona'	;
update [dbo].[Customers] set Region =	'UN'	where City =	'Sevilla'	;
update [dbo].[Customers] set Region =	'HH'	where City =	'Brandenburg'	;
update [dbo].[Customers] set Region =	'PAC'	where City =	'Versailles'	;
update [dbo].[Customers] set Region =	'OCC'	where City =	'Toulouse'	;
update [dbo].[Customers] set Region =	'BB'	where City =	'Frankfurt a.M.'	;
update [dbo].[Customers] set Region =	'25'	where City =	'Bergamo'	;
update [dbo].[Customers] set Region =	'BRU'	where City =	'Bruxelles'	;
update [dbo].[Customers] set Region =	'SN'	where City =	'Leipzig'	;
update [dbo].[Customers] set Region =	'NW'	where City =	'Köln'	;
update [dbo].[Customers] set Region =	'FDI'	where City =	'Paris'	;
update [dbo].[Customers] set Region =	'AT-5'	where City =	'Salzburg'	;
update [dbo].[Customers] set Region =	'11'	where City =	'Lisboa'	;
update [dbo].[Customers] set Region =	'SN'	where City =	'Cunewalde'	;
update [dbo].[Customers] set Region =	'45'	where City =	'Reggio Emilia'	;
update [dbo].[Customers] set Region =	'GE'	where City =	'Genève'	;
update [dbo].[Customers] set Region =	'11'	where City =	'Stavern'	;
update [dbo].[Customers] set Region =	'KH'	where City =	'Kobenhavn'	;
update [dbo].[Customers] set Region =	'VLG'	where City =	'Charleroi'	;
update [dbo].[Customers] set Region =	'NW'	where City =	'Münster'	;
update [dbo].[Customers] set Region =	'SJ'	where City =	'Århus'	;
update [dbo].[Customers] set Region =	'ARA'	where City =	'Lyon'	;
update [dbo].[Customers] set Region =	'BRE'	where City =	'Reims'	;
update [dbo].[Customers] set Region =	'BW'	where City =	'Stuttgart'	;
update [dbo].[Customers] set Region =	'FI-OL'	where City =	'Oulu'	;
update [dbo].[Customers] set Region =	'FI-IS'	where City =	'Helsinki'	;
update [dbo].[Customers] set Region =	'14'	where City =	'Warszawa'	;


select C.ContactName as Cliente_Nombre, C.CompanyName as Cliente_Compania, C.[Address] as Cliente_Direccion, C.City as Cliente_Ciudad, C.Region as Cliente_Region, C.Country as Cliente_Pais
from [Northwind].[dbo].[Customers] as C
```
![](https://i.ibb.co/wsjT2c5/Tabla-Cliente.jpg)

![](https://i.ibb.co/bJp44Z3/Tabla-Cliente-Lista.jpg)

##### Tabla Tiempo
```
select 
CONVERT(date,o.OrderDate) as Tiempo_FechaActual,
CONVERT(int,DATEPART(YEAR,o.OrderDate)) as Tiempo_Anio,
CONVERT(int,DATEPART(QUARTER,o.OrderDate)) as Tiempo_Trimestre,
CONVERT(int,DATEPART(MONTH,o.OrderDate)) as Tiempo_Mes,
CONVERT(int,DATEPART(WEEK,o.OrderDate)) as Tiempo_Semana,
CONVERT(int,DATEPART(DY,o.OrderDate)) as Tiempo_DiaDeAnio,
CONVERT(int,DATEPART(WEEKDAY,o.OrderDate)) as Tiempo_DiaDeSemana,
case --- determinacion de sabados y domingos
when CONVERT(int,DATEPART(WEEKDAY,o.OrderDate)) = 6 then 1
when CONVERT(int,DATEPART(WEEKDAY,o.OrderDate)) = 7 then 1
else 0 
end  as Tiempo_EsFinSemana,
case --- determinacion de  dias festivos
when CONVERT(date,o.OrderDate) =  '1998-01-01'	then 1
when CONVERT(date,o.OrderDate) =  '1998-02-12'	then 1
when CONVERT(date,o.OrderDate) =  '1998-03-17'	then 1
when CONVERT(date,o.OrderDate) =  '1998-06-14'	then 1
when CONVERT(date,o.OrderDate) =  '1998-07-05'	then 1
when CONVERT(date,o.OrderDate) =  '1998-09-11'	then 1
when CONVERT(date,o.OrderDate) =  '1998-09-17'	then 1
when CONVERT(date,o.OrderDate) =  '1998-10-31'	then 1
when CONVERT(date,o.OrderDate) =  '1998-11-11'	then 1
when CONVERT(date,o.OrderDate) =  '1998-12-15'	then 1
when CONVERT(date,o.OrderDate) =  '1998-12-25'	then 1
when CONVERT(date,o.OrderDate) =  '1998-12-31'	then 1
when CONVERT(date,o.OrderDate) =  '1997-01-01'	then 1
when CONVERT(date,o.OrderDate) =  '1997-02-12'	then 1
when CONVERT(date,o.OrderDate) =  '1997-03-17'	then 1
when CONVERT(date,o.OrderDate) =  '1997-06-14'	then 1
when CONVERT(date,o.OrderDate) =  '1997-07-05'	then 1
when CONVERT(date,o.OrderDate) =  '1997-09-11'	then 1
when CONVERT(date,o.OrderDate) =  '1997-09-17'	then 1
when CONVERT(date,o.OrderDate) =  '1997-10-31'	then 1
when CONVERT(date,o.OrderDate) =  '1997-11-11'	then 1
when CONVERT(date,o.OrderDate) =  '1997-12-15'	then 1
when CONVERT(date,o.OrderDate) =  '1997-12-25'	then 1
when CONVERT(date,o.OrderDate) =  '1997-12-31'	then 1
when CONVERT(date,o.OrderDate) =  '1996-01-01'	then 1
when CONVERT(date,o.OrderDate) =  '1996-02-12'	then 1
when CONVERT(date,o.OrderDate) =  '1996-03-17'	then 1
when CONVERT(date,o.OrderDate) =  '1996-06-14'	then 1
when CONVERT(date,o.OrderDate) =  '1996-07-05'	then 1
when CONVERT(date,o.OrderDate) =  '1996-09-11'	then 1
when CONVERT(date,o.OrderDate) =  '1996-09-17'	then 1
when CONVERT(date,o.OrderDate) =  '1996-10-31'	then 1
when CONVERT(date,o.OrderDate) =  '1996-11-11'	then 1
when CONVERT(date,o.OrderDate) =  '1996-12-15'	then 1
when CONVERT(date,o.OrderDate) =  '1996-12-25'	then 1
when CONVERT(date,o.OrderDate) =  '1996-12-31'	then 1
else 0 
end  as Tiempo_EsFeriado,
case -- Determinar comentarios 
when CONVERT(date,o.OrderDate) =  '1998-01-01'	then 'New Years Day'
when CONVERT(date,o.OrderDate) =  '1998-02-12'	then 'Abraham Lincoln Day'
when CONVERT(date,o.OrderDate) =  '1998-03-17'	then 'St. Patricks Day'
when CONVERT(date,o.OrderDate) =  '1998-06-14'	then 'Flag Day'
when CONVERT(date,o.OrderDate) =  '1998-07-05'	then 'Independence Day'
when CONVERT(date,o.OrderDate) =  '1998-09-11'	then 'Patriot Day'
when CONVERT(date,o.OrderDate) =  '1998-09-17'	then 'Constitution Day'
when CONVERT(date,o.OrderDate) =  '1998-10-31'	then 'Halloween'
when CONVERT(date,o.OrderDate) =  '1998-11-11'	then 'Veterans Day'
when CONVERT(date,o.OrderDate) =  '1998-12-15'	then 'Bill of Rights Day'
when CONVERT(date,o.OrderDate) =  '1998-12-25'	then 'Christmas Day'
when CONVERT(date,o.OrderDate) =  '1998-12-31'	then 'New Years Eve'
when CONVERT(date,o.OrderDate) =  '1997-01-01'	then 'New Years Day'
when CONVERT(date,o.OrderDate) =  '1997-02-12'	then 'Abraham Lincoln Day'
when CONVERT(date,o.OrderDate) =  '1997-03-17'	then 'St. Patricks Day'
when CONVERT(date,o.OrderDate) =  '1997-06-14'	then 'Flag Day'
when CONVERT(date,o.OrderDate) =  '1997-07-05'	then 'Independence Day'
when CONVERT(date,o.OrderDate) =  '1997-09-11'	then 'Patriot Day'
when CONVERT(date,o.OrderDate) =  '1997-09-17'	then 'Constitution Day'
when CONVERT(date,o.OrderDate) =  '1997-10-31'	then 'Halloween'
when CONVERT(date,o.OrderDate) =  '1997-11-11'	then 'Veterans Day'
when CONVERT(date,o.OrderDate) =  '1997-12-15'	then 'Bill of Rights Day'
when CONVERT(date,o.OrderDate) =  '1997-12-25'	then 'Christmas Day'
when CONVERT(date,o.OrderDate) =  '1997-12-31'	then 'New Years Eve'
when CONVERT(date,o.OrderDate) =  '1996-01-01'	then 'New Years Day'
when CONVERT(date,o.OrderDate) =  '1996-02-12'	then 'Abraham Lincoln Day'
when CONVERT(date,o.OrderDate) =  '1996-03-17'	then 'St. Patricks Day'
when CONVERT(date,o.OrderDate) =  '1996-06-14'	then 'Flag Day'
when CONVERT(date,o.OrderDate) =  '1996-07-05'	then 'Independence Day'
when CONVERT(date,o.OrderDate) =  '1996-09-11'	then 'Patriot Day'
when CONVERT(date,o.OrderDate) =  '1996-09-17'	then 'Constitution Day'
when CONVERT(date,o.OrderDate) =  '1996-10-31'	then 'Halloween'
when CONVERT(date,o.OrderDate) =  '1996-11-11'	then 'Veterans Day'
when CONVERT(date,o.OrderDate) =  '1996-12-15'	then 'Bill of Rights Day'
when CONVERT(date,o.OrderDate) =  '1996-12-25'	then 'Christmas Day'
when CONVERT(date,o.OrderDate) =  '1996-12-31'	then 'New Years Eve'
else 'NULO'
end  as Tiempo_Comentarios,
CONVERT(int,DATEPART(WEEK,o.OrderDate)-DATEPART(WEEK,DATEADD(MM,DATEDIFF(MM,0,o.OrderDate),0))+1) as Tiempo_SemanaCalendario,
CONVERT(int,DATEPART(WEEK,o.OrderDate)-1) as Tiempo_SemanaSDelAnioLaborales,
case
when CONVERT(int,DATEPART(YEAR,o.OrderDate)%400) = 0 then 1 
when CONVERT(int,DATEPART(YEAR,o.OrderDate)%4) = 0 then 1
else 0 
end as Tiempo_AnioBisiesto,
DATENAME(DW,o.OrderDate)  as Tiempo_Descripcion_DiaDeSemana,
DATENAME(MONTH,o.OrderDate) as Tiempo_Descripcion_Mes,
case 
when DATENAME(QUARTER,o.OrderDate) =1 then 'primer trimestre'
when DATENAME(QUARTER,o.OrderDate) =2 then 'segundo trimestre'
when DATENAME(QUARTER,o.OrderDate) =3 then 'tercer trimestre'
when DATENAME(QUARTER,o.OrderDate) =4 then 'cuarto trimestre'
else 'N/A'
end as Tiempo_Descripcion_Trimestre,
case
when DATEPART(QUARTER,o.OrderDate) >= 3 then 'semestre dos'
else 'semestre uno'
end as Tiempo_Descripcion_Semestre
from [Northwind].[dbo].[Orders] as o;
```
![](https://i.ibb.co/qdYvdkR/Tabla-Tiempo.jpg)

![](https://i.ibb.co/ZmB6wyG/Tabla-Cliente-Lista.jpg)

##### Tabla Ventas
```
select 
DBN_MC.Cliente_Skey as Cliente_Skey,
DBN_MP.Producto_Skey as Producto_Skey,
DBN_ME.Empleado_Skey as Empleado_Skey,
DBN_MT.Tiempo_Skey as Tiempo_Skey,
DBNO.OrderID as Ventas_NOrden,
(DBNO.Quantity*DBNO.UnitPrice) as Ventas_Monto,
DBNO.Quantity as Ventas_unidades ,
DBNO.UnitPrice as Ventas_Punitario ,
(AVG(DBNO.UnitPrice * DBNO.Discount)) as Ventas_Descuento
from [Northwind].[dbo].[Products] DBNP
inner join [Northwind].[dbo].[Categories] DBNC
on DBNP.CategoryID = DBNC.CategoryID 
inner join [Northwind].[dbo].[Shippers] DBNPR
on DBNP.SupplierID = DBNP.SupplierID
inner join [Northwind].[dbo].[Order Details] DBNO
on DBNP.ProductID = DBNO.ProductID
inner join [Northwind].[dbo].[Orders] DBNOR
on DBNO.OrderID = DBNOR.OrderID
inner join [Northwind].[dbo].[Customers] DBNCL
on DBNOR.CustomerID = DBNCL.CustomerID
inner join [Northwind].[dbo].[Employees] DBNE
on DBNOR.EmployeeID = DBNE.EmployeeID
inner join [Northwind_FullData]. [dbo].[Dim_Producto] DBN_MP
on DBNP.ProductName = DBN_MP.Producto_Nombre
inner join [Northwind_FullData].[dbo].[Dim Cliente] DBN_MC
on DBNCL.ContactName =  DBN_MC.Cliente_Nombre
inner join [Northwind_FullData].[dbo].[Dim Empleado] DBN_ME
on CONCAT_WS(' ',DBNE.LastName,DBNE.FirstName) = DBN_ME.Empleado_NombreCompleto
inner join [Northwind_FullData].[dbo].[Dim Tiempo] DBN_MT
on (CONVERT(date,DBNOR.OrderDate)) =(CONVERT(date,DBN_MT.Tiempo_FechaActual))
group by DBN_MC.Cliente_Skey,
		 DBN_MP.Producto_Skey,
		 DBN_ME.Empleado_Skey,
		 DBN_MT.Tiempo_Skey,
		 DBNO.OrderID,
		(DBNO.Quantity*DBNO.UnitPrice),
		 DBNO.Quantity,
		 DBNO.UnitPrice;
```
![](https://i.ibb.co/wdYDs9t/Tabla-Ventas.jpg)

![](https://i.ibb.co/QcnfKvx/Tabla-Ventas-lista.jpg)

#### Proceso ETL 
Configuraciones requeridas para ejecutar instrucciones SQL y procedimientos almacenados mediante la conexion seleccionada

![](https://i.ibb.co/0Kzd2zw/Configuracion-requerida.jpg)

#####Flujo de control
![](https://i.ibb.co/v3XcjWq/Flujo-de-control.jpg)

#####Flujo de datos Tabla Tiempo
![](https://i.ibb.co/kK5SkDR/Flujo-de-datos-Tabla-tiempo.jpg)

##### Configuracion  "origen de OLE DB" tabla tiempo
![](https://i.ibb.co/0XDCMjB/Editor-de-origen-de-OLE-DB-Tabla-tiempo.jpg)

##### Configuración "Conversión de datos" tabla tiempo
![](https://i.ibb.co/pnJ07z9/Editor-de-transformacion-Conversi-n-de-datos.jpg)

##### Configuración "Destino de OLE DB" tabla tiempo
![](https://i.ibb.co/z6SMy6M/Editor-de-destino-de-OLE-DB.jpg)
![](https://i.ibb.co/FhM1Xbb/Configure-las-propiedades-para-insertar-datos-en-una-base-de-datos-relacional-mediante-un-proveedor.jpg)

#### Visualizacion de la informacion de acuerdo a los objetivos

https://app.powerbi.com/view?r=eyJrIjoiNjQ2YTdhMzAtZWRmNS00MjA3LTkwOWItZWYyNGQ1NWNmNTViIiwidCI6IjQ5NWZkMDQ2LWQ4NjQtNDY2Ny05ZWU4LTUwNjc5NzY0ZDgzNCIsImMiOjR9


