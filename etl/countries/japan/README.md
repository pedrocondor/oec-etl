# Japanese Sub-National Trade Data

This trade dataset is made and published by the Ministry of Finance and the Customs.  

Source : Declarations submitted to the Customs  

Value (The unit is JPY1000 and rounded down)  
    1. Export: FOB base  
    2. Import: CIF base  

Country
    1. Export : Final Destination
    2. Import : Country of Origin  

Product Resolution : 9 Digit HS Classification

Spatial Resoultion : Coustoms

Frequency : Monthly

Starting Year : 2010

### Sample Data
Database is divided into two groups export, import. Both have same file structure. Table below show a sample from export data.  

| Exp or Imp | Year | Custom | HS        | Country | Unit1 | Unit2 | Quantity1-Year | Quantity2-Year | Value-Year |
|------------|------|--------|-----------|---------|-------|-------|----------------|----------------|------------|
| 1          | 2017 | 104    | 010121000 | 103     |       | NO    | 0              | 3              | 2100       |
| 1          | 2017 | 104    | 010121000 | 304     |       | NO    | 0              | 1              | 1138       |
| 1          | 2017 | 104    | 010121000 | 601     |       | NO    | 0              | 6              | 1041487    |
| 1          | 2017 | 104    | 010129000 | 103     |       | NO    | 0              | 6              | 40200      |
| 1          | 2017 | 300    | 010129000 | 105     |       | NO    | 0              | 24             | 20972      |
| 1          | 2017 | 104    | 010129000 | 108     |       | NO    | 0              | 5              | 107479     |
| 1          | 2017 | 104    | 010129000 | 112     |       | NO    | 0              | 11             | 48183      |

### Variable Details
Exp or Imp : 1 for Export and 2 for import  
Custom : Japan is divided into 9 Coustom regions which are subdivided into approximately 86 ports(this estimate is from distict values in 2017 export dataset).  


## Download 
http://www.customs.go.jp/toukei/info/index_e.htm

