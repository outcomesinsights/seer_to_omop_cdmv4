# SEER Medicare to OMOP CDMv4 ETL
A partial ETL of SEER Medicare data into OMOP CDMv4

### Original Purpose
Outcomes Insights ETLed a subset of SEER Medicare data in preparation for a demonstration of [Jigsaw](http://jigsawanalytics.com/) to [NCI](http://www.cancer.gov/) in November, 2014.  As members of the [CMS ETL Working Group](http://www.ohdsi.org/web/wiki/doku.php?id=projects:workgroups:etl-wg), we believed it might be helpful if we provided the OHDSI community with the SAS code we used to perform this incomplete ETL of SEER Medicare into OMOP CDMv4.

Please note the SAS code is for reference only and not intended to be reusable.  It is also not intended to serve as a basis for implementation of future ETLs.


### Scope of ETL
The ETL only generates CDMv4-compatible CSV files for the following tables:

- Person
- Condition Occurrence
- Procedure Occurence
- Visit Occurence
- Payer Plan Period
- Observation Period
- Death


### Further Information
Outcomes Insights created a [partial ETL specification](https://github.com/outcomesinsights/seer_to_omop_cdmv4/blob/master/ETL%20for%20SEER%20Medicare%20data%20v0.1.doc?raw=true) which contains additional information about this ETL implementation.
