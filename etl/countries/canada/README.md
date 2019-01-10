# Canadian Sub-National Trade Data

Canadian International Merchandise Trade Database (CIMT) provide sub-national import/export/re-export data. These trade figures are customs-based data derived from customs documents and compiled by
Statistics Canada.

Product resolution : 6 digit HS classification  

Spatial resoultion : Province  

Frequency : Monthly

Starting Year : 1992

### Sample Data
CIMT database is divided into three groups export, import and re-export. All three have same file structure. Table below show a sample from export data.

 
 | HS.Code | UOM | Country.Code | State.Code | Geo | Value   | Quantity | Year | month |
|---------|-----|--------------|------------|-----|---------|----------|------|-------|
| 841381  | NMB | 154          | 1000       | 59  | 774     | 12       | 2018 | 2     |
| 853931  | NMB | 9            | 1041       | 24  | 781300  | 556203   | 2017 | 10    |
| 10600   | N/A | 9            | 1000       | 1   | 3806760 | 0        | 1988 | 7     |
| 10600   | N/A | 9            | 1000       | 1   | 1183017 | 0        | 1988 | 9     |
| 10600   | N/A | 9            | 1000       | 1   | 1185400 | 0        | 1988 | 12    |
| 10600   | N/A | 9            | 1000       | 1   | 702093  | 0        | 1989 | 1     |

### Variable Details
HS.Code : Harmonized System Code  
UOM : Unit of Measurement  
Country.Code : Country code for importing/exporting/reexporting country (999 refers to the totals for the
World)  
State.Code : 1000 for Non USA and 1001-1055 for USA states/other areas within USA  
Geo : 1 for Canada, other than 1 are Canadian province code  
Value : Trade value in Canadian Dollars  


## Download 
	1. CIMT  
	https://open.canada.ca/data/en/dataset?sort=metadata_modified+desc&q=CIMT&organization=statcan
	2. CIMT General Concepts
	https://www5.statcan.gc.ca/cimt-cicm/page-page?lang=eng&mode=concepts
	3. CIMT Export Classification
	https://www150.statcan.gc.ca/n1/en/catalogue/65-209-X
