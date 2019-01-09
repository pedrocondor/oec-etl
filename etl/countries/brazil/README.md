# Brazilian Sub-National Trade Data

Comex Stat Database by Ministry of Industry, Foreign Trade and Services provide Brazilian import/export data at sub-national level. Comex Stat Database contain two sub database.  

### Dataset 1 : State level data

Product resolution : 8 digit HS classification  

Spatial resoultion : Federative Units (States/Federal District)  

Frequency : Monthly

Starting Year : 1997

#### Sample Data
State Level dataset is divided into two groups export and import. Both have same file structure. Table below show a sample from export data.  

 
 | CO_ANO | CO_MES | CO_NCM   | CO_UNID | CO_PAIS | SG_UF_NCM | CO_VIA | CO_URF  | QT_ESTAT | KG_LIQUIDO | VL_FOB  |
|--------|--------|----------|---------|---------|-----------|--------|---------|----------|------------|---------|
| 1997   | 1      | 87089900 | 10      | 63      | SP        | 7      | 1010900 | 2269305  | 1737007    | 7087115 |
| 1997   | 1      | 37052000 | 10      | 589     | SP        | 4      | 817700  | 10       | 0          | 79      |
| 1997   | 1      | 70023200 | 10      | 741     | RS        | 4      | 817600  | 0        | 1          | 421     |
| 1997   | 2      | 85445100 | 10      | 493     | RE        | 4      | 817700  | 13961    | 977        | 23563   |
| 1997   | 2      | 9011110  | 21      | 399     | MG        | 1      | 817800  | 3207     | 3197340    | 8959457 |
| 1997   | 2      | 30061019 | 10      | 158     | SP        | 4      | 817600  | 0        | 525        | 106129  |

#### Variable Details
CO_AND : Year  
CO_MES : Month  
CO_NCM : HS Code  
CO_UNID : Statistical unit code  
CO_PAIS : Country code of destination/product origin  
SG_UF_NCM : Codes of the units of the federation (States/Federal District) of Brazil  
CO_VIA : transport code  
CO_URF : Federal Revenue Unit  
KG_LIQUIDO : Net Kilogram  
VL_FOB : FOB value in US dollar  


### Dataset 2 : Municipality level data

Product resolution : 4 digit HS classification  

Spatial resoultion : Municipality  

Frequency : Monthly

Starting Year : 1997

#### Sample Data
Municipality Level dataset is divided into two groups export and import. Both have same file structure. Table below show a sample from export data.  

 
|   | CO_ANO | CO_MES | SH4  | CO_PAIS | SG_UF_MUN | CO_MUN  | KG_LIQUIDO | VL_FOB  |
|---|--------|--------|------|---------|-----------|---------|------------|---------|
| 1 | 1997   | 2      | 8544 | 399     | SP        | 3419071 | 95         | 4403    |
| 2 | 1997   | 1      | 8436 | 325     | SP        | 3422604 | 85         | 494     |
| 3 | 1997   | 1      | 8409 | 249     | MG        | 3106705 | 3544251    | 4205965 |
| 4 | 1997   | 2      | 8479 | 586     | PR        | 4108304 | 2149       | 7506    |
| 5 | 1997   | 2      | 5603 | 845     | SP        | 3449904 | 6229       | 26524   |
| 6 | 1997   | 1      | 6204 | 586     | MS        | 5206606 | 2          | 65      |

#### Variable Details
CO_AND : Year  
CO_MES : Month  
SH4 : HS Code  
CO_PAIS : Country code of destination/product origin  
SG_UF_MUN : Codes of the units of the federation (States/Federal District) of Brazil  
CO_MUN : Municipality code  
KG_LIQUIDO : Net Kilogram  
VL_FOB : FOB value in US dollar  


## Download 
	1. Comex Stat Database 
	`http://www.mdic.gov.br/comercio-exterior/estatisticas-de-comercio-exterior/base-de-dados-do-comercio-exterior-brasileiro-arquivos-para-download`