**********************************************************************************************************************************************************;
*ETL process to pull SEER Medicare data for Jigsaw Demos.
*Outcomes Insights, Inc. is releasing this code for reference.  This is not intended to be run on any other system.;
*The code below includes Test code used to create the Provider and Drug Exposure table in CDM.  This is not documented in the Documentation File, nor
has the code been fully tested. The code has been commented out below to retain the logic behind these tables.;
*
*This code was written in SAS 9.4;
*
*Description of libnames: ped,med,out,nch,dme,hha,hsp are all pulling the raw SAS data from SEER Medicare files.;
*Base holds all claims data for all years for only patient ids found in the ped file you selected;
*Data holds cleaned base data (specific to this project). OMOP holds all OMOP tables after ETL.  Vocab holds the omop mappings;
*
*Author - Jennifer Duryea, Outcomes Insights, Inc., February 4, 2015;
**********************************************************************************************************************************************************;

libname ped "E:\Data\SEER 2000-2009 Medicare 1999-2010 (12 Cancers)\ped";
libname med "E:\Data\SEER 2000-2009 Medicare 1999-2010 (12 Cancers)\med";
libname out "E:\Data\SEER 2000-2009 Medicare 1999-2010 (12 Cancers)\out";
libname nch "E:\Data\SEER 2000-2009 Medicare 1999-2010 (12 Cancers)\nch";
libname dme "E:\Data\SEER 2000-2009 Medicare 1999-2010 (12 Cancers)\dme";
libname hha "E:\Data\SEER 2000-2009 Medicare 1999-2010 (12 Cancers)\hha";
libname hsp "E:\Data\SEER 2000-2009 Medicare 1999-2010 (12 Cancers)\hsp";
libname base "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\Base";
libname data "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\Data";
libname omop "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\OMOP";
libname vocab "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\OMOP\vocabs";

%let empty_val = .;
%let macroSource = %str(D:\Macros\SEER Medicare);
%include "&macroSource\Combine_All_Years_2013_06_18.sas";
ods graphics no;

proc format;
	value $ sex_fmt  "1" = "MALE"
				  "2" = "FEMALE";

	value $ race_fmt "1" = "White"
				   "2" = "Black or African American"
				   "4" = "Asian"
				   "6" = "Native American Native"
				   "5" = "Hispanic"
				   "3","0" = "Other or Unknown";

    picture icd9_dx low-high = "999.99";
run;

************************************************************************************************************************************************************************************************;
*Create raw medicare files from the 12 Cancer SEER data so that it only includes colorectal cancer patients;
*Use %CombineAllYears in-house macro to combine claims files by type across years 1999-2010;

data ped_ids;
	set ped.ped_all;
	keep patient_id;
run;

%let yearsList = %str(99 00 01 02 03 04 05 06 07 08 09 10);

%CombineAllYears(pedInclExclIDs=ped_ids,InLibname=med,OutLibname=base,yearsList=&yearsList ,monthVarName=1,dayVarName=1,yearVarName=1);
proc datasets;
	delete med:;
quit;
%CombineAllYears(pedInclExclIDs=ped_ids,InLibname=out,OutLibname=base,yearsList=&yearsList ,monthVarName=1,dayVarName=1,yearVarName=1);
proc datasets;
	delete out:;
quit;
%CombineAllYears(pedInclExclIDs=ped_ids,InLibname=nch,OutLibname=base,yearsList=&yearsList ,monthVarName=1,dayVarName=1,yearVarName=1);
proc datasets;
	delete nch:;
quit;
%CombineAllYears(pedInclExclIDs=ped_ids,InLibname=hsp,OutLibname=base,yearsList=&yearsList ,monthVarName=1,dayVarName=1,yearVarName=1);
proc datasets;
	delete hsp:;
quit;
%CombineAllYears(pedInclExclIDs=ped_ids,InLibname=dme,OutLibname=base,yearsList=&yearsList ,monthVarName=1,dayVarName=1,yearVarName=1);
proc datasets;
	delete dme:;
run;
%CombineAllYears(pedInclExclIDs=ped_ids,InLibname=hha,OutLibname=base,yearsList=&yearsList ,monthVarName=1,dayVarName=1,yearVarName=1);
proc datasets;
	delete hha:;
quit;


************************************************************************************************************************************************************************************************;
*Clean raw data files;
*Need to add new claim and visit dates to files since raw files split up year/month/days into separate columns. Drop claim_dt variable created during %CombineAllYears macro;
*MED: Drop claims with missing thru dates (applicable to some SNF claims);
*Out/HHA/HSP: drop claim dates = 0000-00-00 by removing revenue code=0001. If still 0000-00-00, then set dates to claim billing from/thru dates;
*Rename provider id variables to "provider" so all files have a "provider" id variable;
*Remove rejected claims from the NCH file;
data data.med;
	set base.med;
	clm_from_dt=catx('-',adm_y,adm_m,adm_d);

		length y $4;
		length m $2;
		length d $2;
		*Create combined claim dates to = visit dates in cdm from inpatient data.  If missing discharge date, set date to 0000-00-00 (YYYY-MM-DD format per Ryan);
		y=dis_y ;
		m=dis_m;
		d=dis_d;
		if dis_y = "" then y='0000';
		if dis_m = "" then m='00';
		if dis_d = "" then d='00';

	clm_thru_dt=catx('-',y,m,d);
	if clm_thru_dt ne "0000-00-00";

	drop y m d claim_dt;
run;

data data.out;
	set base.out;
	clm_from_dt=catx('-',cendy,cendm,cendd);
	clm_thru_dt=catx('-',cendy,cendm,cendd);
	if center ne "0001";
	if clm_from_dt = "0000-00-00" then clm_from_dt = catx("-",from_dty, from_dtm, from_dtd);
	if clm_thru_dt = "0000-00-00" then clm_thru_dt = catx("-",thru_dty, thru_dtm, thru_dtd);
	drop claim_dt;
run;

data data.nch;
	set base.nch;
	clm_from_dt=catx('-',frexpeny,frexpenm,frexpend);
	clm_thru_dt=catx('-',lsexpeny,lsexpenm,lsexpend);
	length provider $10;
	rename prf_npi=provider;
	if provider = " " then provider = perupin;
	if provider = " " and perupin = " " then provider = tax_num;
	if provider = " " and perupin = " " and tax_num = " " then provider = "nch";
	if pmtdnlcd ne "0";
	drop claim_dt;
run;

data data.dme;
	set base.dme;
	clm_from_dt=catx('-',frexpeny,frexpenm,frexpend);
	clm_thru_dt=catx('-',lsexpeny,lsexpenm,lsexpend);
	rename suplrnum=provider;
	drop claim_dt;
run;

data data.hha;
	set base.hha;
	clm_from_dt=catx('-',cendy,cendm,cendd);
	clm_thru_dt=catx('-',cendy,cendm,cendd);
	if center ne "0001";
	if clm_from_dt = "0000-00-00" then clm_from_dt = catx("-",from_dty, from_dtm, from_dtd);
	if clm_thru_dt = "0000-00-00" then clm_thru_dt = catx("-",thru_dty, thru_dtm, thru_dtd);
	drop claim_dt;
run;

data data.hsp;
	set base.hsp;
	clm_from_dt=catx('-',cendy,cendm,cendd);
	clm_thru_dt=catx('-',cendy,cendm,cendd);
	if center ne "0001";
	if clm_from_dt = "0000-00-00" then clm_from_dt = catx("-",from_dty, from_dtm, from_dtd);
	if clm_thru_dt = "0000-00-00" then clm_thru_dt = catx("-",thru_dty, thru_dtm, thru_dtd);
	drop claim_dt;
run;

************************************************************************************************************************************************************************************************;
*Create person and death table;
Data omop.person(keep=person_id	gender_concept_id	year_of_birth	month_of_birth	day_of_birth
				   race_concept_id	ethnicity_concept_id	location_id	provider_id	care_site_id
				   person_source_value	gender_source_value	race_source_value	ethnicity_source_value)
	 omop.death(keep=person_id death_date death_type_concept_id cause_of_death_concept_id cause_of_death_source_value);


	set ped.ped_all;

	*create new person id since source patient id is a non-integer (leading zeros). Merge tables at end to add person_id variable to all other tables;
	person_id = _n_;

	*OMOP Male conept_id = 8507 and female = 8532;
	if m_sex = 1 then
		gender_concept_id = 8507;
	else if m_sex = 2 then
		gender_concept_id = 8532;

	*Create year, month, and day of birth variables;
	year_of_birth = BIRTHYR;
	month_of_birth = BIRTHM;
	day_of_birth = 1;

	*OMOP white conept_id = 8527 black concept = 8516 asian=8515 native am=8657 other = 8522;
	if race = 1 then
		race_concept_id = 8527;
	else if race = 2 then
		race_concept_id = 8516;
	else if race = 4 then
		race_concept_id = 8515;
	else if race = 6 then
		race_concept_id = 8657;
	else
		race_concept_id = 8522;

	*race=5=hispanic. not sure how to incorporate that ;
	if race = 5 then do;
		ethnicity_concept_id = 38003563;
		ethnicity_source_value = "Hispanic";
	end;
	else do;
		ethnicity_concept_id = 38003564;
		ethnicity_source_value = "Not Hispanic";
	end;

	*Set these to empty, may fill these in later;
	location_id = &empty_val;
	provider_id = &empty_val;
	care_site_id = 0;

	*Fill in source values;
	person_source_value = patient_id;
	gender_source_value	= m_sex;
	race_source_value	= race;

	*Output to the omop.person data set;
	output omop.person;

	*Create omop.death data set with people who died;
	if med_dodm not in('','00') then do;
		dodmo = med_dodm ;
		dodyr = med_dody;
		dodd = med_dodd;
		if med_dodd in('','00') then dodd=1;
	end;
	else if ser_dodm not in('','00') then do;
		if ser_dodm = '14' then dodmo = '01'; else dodmo = ser_dodm ;
		if ser_dody = '2053' then dodyr = '2011'; else dodyr = ser_dody;
		dodd = '01';
	end;
	/*else if dod_flg=0 then do;
		dodyr='0000';
		dodmo='00';
		dodd='00';
	end;*/

	death_date = catx('-',dodyr,dodmo,dodd);

	if death_date ne " " then do;
		death_type_concept_id = 38003566;
		cause_of_death_concept_id = .;
		cause_of_death_source_value = "";
		output omop.death;
	end;

	format race_source_value race_fmt. gender_source_value sex_fmt. /*death_date YYMMDDd10. - removed since in correct format per Ryan*/;
run;

************************************************************************************************************************************************************************************************;
*Create Observation and Visit Occurrence table for each diagnosed cancer;
data observation_novisit (keep= observation_id person_source_value observation_concept_id observation_date observation_time value_as_number
						value_as_string value_as_concept_id unit_concept_id range_low range_high observation_type_concept_id
						associated_provider_id relevant_condition_concept_id observation_source_value
						units_source_value)
	visit_occurrence_seer_dups (keep= person_source_value provider visit_start_date visit_end_date place_of_service_concept_id
						care_site_id place_of_service_source_value);
	set ped.ped_all;

	*Create visit_occurrence_id after merging two visit_occurrence tables and creating ID from final set of rows;
	*Create variables for the observation table;
	person_source_value = patient_id;
	observation_concept_id = 0;
	observation_time = .;
	value_as_number = .;
	value_as_concept_id = .;
	unit_concept_id = .;
	range_low = .;
	range_high = .;
	observation_type_concept_id = 0;
	associated_provider_id = .;
	relevant_condition_concept_id = .;
	units_source_value = .;

	*Create variables for the visit_occurrence_seer table;
	care_site_id = 0;
	place_of_service_concept_id = 8844;
	place_of_service_source_value = "SEER Data";
	length provider $2;

	array seer {*} seq1-seq10 siter1-siter10 hist1-hist10 beh1-beh10 grade1-grade10 dxconf1-dxconf10 src1-src10 dajccstg1-dajccstg10
					aj3sr1-aj3sr10 hstst1-hstst10 sxprif1-sxprif10 rad1-rad10;
	array mo {*} modx1-modx10;
	*create new month variables, mapping 14 to 01 for unknown months, to calculate dates;
	array m {10} $2. m1-m10;
	array yr {*} yrdx1-yrdx10;

	do i=1 to 10;
		if mo(i) = "14" then m(i) = "01"; else m(i) = mo(i);
	end;

	do i=1 to dim(seer);
		if seer(i) ne "" then do;
		observation_id + 1;
		remainder = mod(i,10);

		*Code to put the variable name minus the "1-10" to put in value_as_string field per Ryan;
		vs = vname(seer(i));
		value_as_string = substr(vs,1,(anydigit(vs,1))-1);

		*Code to use the correct diagnosis mo/year = observation_date for one of the 1-10 cancers reporting on;
		if remainder = 0 then observation_date = catx('-',yr(10),m(10),"01");
			else observation_date = catx('-',yr(remainder),m(remainder),"01");

		observation_source_value = seer(i);
		output observation_novisit;

		visit_start_date = observation_date;
		visit_end_date = observation_date;
		if mod(i,10) = 0 then provider = 10; else provider = mod(i,10);
		output visit_occurrence_seer_dups;

		end;
	end;
run;

*de-dup visit_occurrence_seer table - created multiple rows for each seer variable when it should be just one date;
proc sort data=visit_occurrence_seer_dups out=visit_occurrence_seer nodupkey; by person_source_value visit_start_date provider; run;

************************************************************************************************************************************************************************************************;
*Create visit Occurrence table;

data visit_occurrence_dups (keep= visit_start_date	visit_end_date
								place_of_service_concept_id	care_site_id	place_of_service_source_value person_source_value provider);
	length provider $10;
	set data.nch(keep=patient_id clm_from_dt clm_thru_dt provider plcsrvc in=n)
		data.med(keep=patient_id clm_from_dt clm_thru_dt provider snfind in=i)
		data.out(keep=patient_id clm_from_dt clm_thru_dt provider typesrvc fac_type in=o)
		data.dme(keep=patient_id clm_from_dt clm_thru_dt provider plcsrvc in=e)
		data.hha(keep=patient_id clm_from_dt clm_thru_dt provider fac_type in=h)
		data.hsp(keep=patient_id clm_from_dt clm_thru_dt provider clm_type in=s);

	length place_of_service_source_value $66;

	*visit_occurrence_id + 1 => create visit_occurrence_id when combine visit_occurrence_seer with this de-duped dataset;

	person_source_value = patient_id;
	care_site_id = 0;

	if i=1 then do;
		if snfind = "N" then do;
				place_of_service_concept_id = 8863;
				place_of_service_source_value = "31 - SNF";
				end;
		else do;
				place_of_service_concept_id = 8717;
				place_of_service_source_value = "21 - Inpatient Hospital";
				end;
	end;

	*do not make a visit occurrence record when the revenue center = 0001;
	if (o=1 or h=1) and center ne "0001" then do;
		if fac_type = 7 and typesrvc = 2 then do;
			place_of_service_concept_id = 8949;
			place_of_service_source_value = "65 - ESRD Trt Facility";
			end;
		else if fac_type = 7 and typesrvc = 1 then do;
			place_of_service_concept_id = 8761;
			place_of_service_source_value = "72 - Rural Health Clinic";
			end;
		else if fac_type = 7 and typesrvc = 3 then do;
			place_of_service_concept_id = 8966;
			place_of_service_source_value = "50 - Federally Qualified Health Center";
			end;
		else if fac_type = 7 and typesrvc in (4, 5) then do;
			place_of_service_concept_id = 8947;
			place_of_service_source_value = "62 - Comp Outpt Rehab Fac";
			end;
		else if fac_type = 7 and typesrvc = 6 then do;
			place_of_service_concept_id = 8964;
			place_of_service_source_value = "53 - Community Mental HC";
			end;
		else if fac_type = 2 then do;
			place_of_service_concept_id = 8863;
			place_of_service_source_value = "31 - SNF";
			end;
		else if fac_type = 8 and typesrvc = 3 then do;
			place_of_service_concept_id = 8883;
			place_of_service_source_value = "24 - ASC";
			end;
		else if fac_type = 8 and typesrvc in (1, 2) then do;
			place_of_service_concept_id = 8546;
			place_of_service_source_value = "34 - Hospice";
			end;
		else if fac_type = 8 and typesrvc = 4 then do;
			place_of_service_concept_id = 8650;
			place_of_service_source_value = "25 - Birthing Center";
			end;
		else if fac_type = 8 and typesrvc in (5, 6, 7, 8, 9) then do;
			place_of_service_concept_id = 8716;
			place_of_service_source_value = "49 - Independent Clinic";
			end;
		else if fac_type in (1, 6) and typesrvc in (1, 5, 6, 7, 8, 9) then do;
			place_of_service_concept_id = 8717;
			place_of_service_source_value = "21 - Inpatient Hospital";
			end;
		else if fac_type in (1, 6) and typesrvc in (2, 3, 4) then do;
			place_of_service_concept_id = 8756;
			place_of_service_source_value = "22 - Outpatient";
			end;
		else if fac_type = 3 then do;
			place_of_service_concept_id = 8536;
			place_of_service_source_value = "12 - Home (HHA)";
			end;
		else if fac_type = 4 then do;
			place_of_service_concept_id = 8844;
			place_of_service_source_value = "99 Other POS - Religious Center";
			end;
		else if typesrvc = 2 then do;
			place_of_service_concept_id = 8892;
			place_of_service_source_value = "Other Inpt";
			end;
		else if typesrvc = 3 then do;
			place_of_service_concept_id = 8756;
			place_of_service_source_value = "Outpatient";
			end;
		else if typesrvc = 4 then do;
			place_of_service_concept_id = 8844;
			place_of_service_source_value = "Other Part B";
			end;
			/*Q - HOW TO MAP FACILITY TYPES? HOME HEALTH AGENCY (UNLIKE 'HOME')? CURRENTLY NO WAY TO DETERMINE FAC VS PROF CLAIMS*/
		else do;
			place_of_service_concept_id = 8844;
			place_of_service_source_value = "Other Unlisted Facility";
			end;
	end;

	if s=1 then do;
		if clm_type = "50" then do;
			place_of_service_concept_id = 8546;
			place_of_service_source_value = "Hospice";
			end;
		/*******Q - HOW TO INDICTE HOME HEALTH AGENCY (NOT POS = HOME)? HOW TO MAP FACILITY TYPES NOT POS TYPES?*/
		/*if fac_type = "3" then do;
			place_of_service_concept_id = ????;
			place_of_service_source_value = "Home Health Agency";
			end;*/
		else do;
			place_of_service_concept_id = 8844;
			place_of_service_source_value = "Other Unlisted Facility";
			end;
	end;

	if n=1 or e=1 then do;
		if plcsrvc = 11 then do;
			place_of_service_concept_id = 8940;
			place_of_service_source_value = "11 - Office";
			end;
		else if plcsrvc = 12 then do;
			place_of_service_concept_id = 8536;
			place_of_service_source_value = "12 - Home";
			end;
		else if plcsrvc = 21 then do;
			place_of_service_concept_id = 8717;
			place_of_service_source_value = "21 - Inpatient Hospital";
			end;
		else if plcsrvc = 22 then do;
			place_of_service_concept_id = 8756;
			place_of_service_source_value = "22 - Outpatient Hospital";
			end;
		else if plcsrvc = 23 then do;
			place_of_service_concept_id = 8870;
			place_of_service_source_value = "23 - ER";
			end;
		else if plcsrvc = 24 then do;
			place_of_service_concept_id = 8883;
			place_of_service_source_value = "24 - ASC";
			end;
		else if plcsrvc = 25 then do;
			place_of_service_concept_id = 8650;
			place_of_service_source_value = "25 - Birthing Center";
			end;
		else if plcsrvc = 26 then do;
			place_of_service_concept_id = 8905;
			place_of_service_source_value = "26 - Military Trtmt";
			end;
		else if plcsrvc = 31 then do;
			place_of_service_concept_id = 8663;
			place_of_service_source_value = "31 - SNF";
			end;
		else if plcsrvc = 32 then do;
			place_of_service_concept_id = 8676;
			place_of_service_source_value = "32 - Nursing Fac";
			end;
		else if plcsrvc = 33 then do;
			place_of_service_concept_id = 8827;
			place_of_service_source_value = "33 - Custodial Care";
			end;
		else if plcsrvc = 34 then do;
			place_of_service_concept_id = 8546;
			place_of_service_source_value = "34 - Hospice";
			end;
		else if plcsrvc = 35 then do;
			place_of_service_concept_id = 8882;
			place_of_service_source_value = "35 - Adult Living Fac";
			end;
		else if plcsrvc = 41 then do;
			place_of_service_concept_id = 8668;
			place_of_service_source_value = "41 - Ambulance Land";
			end;
		else if plcsrvc = 42 then do;
			place_of_service_concept_id = 8850;
			place_of_service_source_value = "42 - Ambulance waterair";
			end;
		else if plcsrvc = 49 then do;
			place_of_service_concept_id = 8716;
			place_of_service_source_value = "49 - Independent Clinic";
			end;
		else if plcsrvc = 50 then do;
			place_of_service_concept_id = 8966;
			place_of_service_source_value = "50 - Fed Qual HC";
			end;
		else if plcsrvc = 51 then do;
			place_of_service_concept_id = 8971;
			place_of_service_source_value = "51 - Inpt Psych Fac";
			end;
		else if plcsrvc = 52 then do;
			place_of_service_concept_id = 8913;
			place_of_service_source_value = "52 - Psych Fac Part Hosp";
			end;
		else if plcsrvc = 53 then do;
			place_of_service_concept_id = 8964;
			place_of_service_source_value = "53 - Comm Mental HC";
			end;
		else if plcsrvc = 54 then do;
			place_of_service_concept_id = 8951;
			place_of_service_source_value = "54 - Interm Mental HC";
			end;
		else if plcsrvc = 55 then do;
			place_of_service_concept_id = 8957;
			place_of_service_source_value = "55 - Res Subst Abuse Trt";
			end;
		else if plcsrvc = 56 then do;
			place_of_service_concept_id = 8650;
			place_of_service_source_value = "56 - Psych Res Trt";
			end;
		else if plcsrvc = 57 then do;
			place_of_service_concept_id = 8976;
			place_of_service_source_value = "57 - Non-Res Substance Abuse";
			end;
		else if plcsrvc = 60 then do;
			place_of_service_concept_id = 8858;
			place_of_service_source_value = "60 - Mass Immunization";
			end;
		else if plcsrvc = 61 then do;
			place_of_service_concept_id = 8920;
			place_of_service_source_value = "61 - Comp Inp Rehab";
			end;
		else if plcsrvc = 62 then do;
			place_of_service_concept_id = 8947;
			place_of_service_source_value = "62 - Comp Out Rehab";
			end;
		else if plcsrvc = 65 then do;
			place_of_service_concept_id = 8949;
			place_of_service_source_value = "65 - ESRD Trt Fac";
			end;
		else if plcsrvc = 71 then do;
			place_of_service_concept_id = 8977;
			place_of_service_source_value = "71 - Public Health Clin";
			end;
		else if plcsrvc = 72 then do;
			place_of_service_concept_id = 8761;
			place_of_service_source_value = "72 - Rural HC";
			end;
		else if plcsrvc = 81 then do;
			place_of_service_concept_id = 8809;
			place_of_service_source_value = "81 - Independent Lab";
			end;
		else if plcsrvc = 95 then do;
			place_of_service_concept_id = 8677;
			place_of_service_source_value = "95 - Outpatient NEC";
			end;
		else do;
			place_of_service_concept_id = 8844;
			place_of_service_source_value = "Other Unlisted Facility";
			end;
	end;

	visit_start_date = CLM_FROM_DT;
	visit_end_date = CLM_THRU_DT;

	output;

run;

proc sort data=visit_occurrence_dups out=visit_occurrence_clm nodupkey;
	by person_source_value provider visit_start_date visit_end_date;
run;

*Create visit_occurrence_id for all visit_occurrences from claim files and from seer files;
data visit_occurrence;
	set visit_occurrence_clm visit_occurrence_seer;
	visit_occurrence_id + 1;
run;

*Subset the visit_occurrence records that came from SEER data to merge the visit_occurrence_ids into the Observation table;
data visit_occurrence_s_id;
	set visit_occurrence;
	where place_of_service_source_value = "SEER Data";
run;

proc sort data=observation_novisit; by person_source_value observation_date;
proc sort data=visit_occurrence_s_id; by person_source_value visit_start_date;

data observation (keep= observation_id person_source_value observation_concept_id observation_date observation_time value_as_number
						value_as_string value_as_concept_id unit_concept_id range_low range_high observation_type_concept_id
						associated_provider_id visit_occurrence_id relevant_condition_concept_id observation_source_value
						units_source_value)
	errors;
	merge 	observation_novisit (in=a)
			visit_occurrence_s_id (in=b rename=(visit_start_date=observation_date));
	by person_source_value observation_date;
	if a=1 and b=0 then output errors;
	if a=1 and b=1 then output observation;
run;



************************************************************************************************************************************************************************************************;
*Create condition occurrence table;
proc sort data=visit_occurrence; by person_source_value visit_start_date visit_end_date provider; run;
proc sort data=data.med; by patient_id clm_from_dt clm_thru_dt provider; run;
proc sort data=data.out; by patient_id clm_from_dt clm_thru_dt provider; run;
proc sort data=data.nch; by patient_id clm_from_dt clm_thru_dt provider; run;
proc sort data=data.dme; by patient_id clm_from_dt clm_thru_dt provider; run;
proc sort data=data.hha; by patient_id clm_from_dt clm_thru_dt provider; run;
proc sort data=data.hsp; by patient_id clm_from_dt clm_thru_dt provider; run;

*****Only used facility header and outpatient header values for condition_occurrence_ids. Need better id for nch and claim dx vs line;
*Merge visit_occurrence_id to all lines in medicare files by patient_id visit_start_date visit_end_date and provider - renamed clm_from_dt & clm_thru_dt to visit_start/end_dt;
data condition_occurrence_dups (keep=condition_occurrence_id	person_source_value	condition_concept_id	condition_start_date	condition_end_date
								condition_type_concept_id	stop_reason	associated_provider_id	visit_occurrence_id	condition_source_value position vocabulary_id provider)
	errors;
	length provider $10;
	merge visit_occurrence(rename=(person_source_value=patient_id) in=visit)
		data.med (keep=patient_id dgn_cd: admdxcde clm_from_dt clm_thru_dt provider  in=inp rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.out (keep=patient_id dgn_cd: edgnsd: e1dgnscd clm_from_dt clm_thru_dt at_upin at_npi op_upin op_npi ot_upin ot_npi provider
					in=out rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.nch (keep=patient_id dgn_cd: pdgns_cd linediag clm_from_dt clm_thru_dt provider in=nch rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.dme (keep=patient_id dgn_cd: pdgns_cd linediag clm_from_dt clm_thru_dt provider in=dme rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.hha (keep=patient_id dgn_cd: clm_from_dt clm_thru_dt at_upin at_npi op_upin op_npi ot_upin ot_npi provider
					in=hha rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.hsp (keep=patient_id dgn_cd: clm_from_dt clm_thru_dt at_upin at_npi op_upin op_npi ot_upin ot_npi provider
					in=hsp rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date));

	by patient_id visit_start_date visit_end_date provider;

	retain condition_occurrence_id 0;

	length condition_source_value $50;
	length associated_provider_id $10;
	length dx_left $5;
	length dx_right $5;

	array inp_dx{*} dgn_cd: admdxcde;
	*do something with admdxcde - currently left in claim diagnosis as the last position;
	array out_dx{*} dgn_cd:;
	/*one array for out, hha and hsp claims since they have the same dx fields*/
	array oute_dx{*} e1dgnscd edgnsd:;
	array nch_dx{*} linediag pdgns_cd dgncd:;
	/*one array for nch and dme claims since they have the same dx fields*/
	/*currently put all diags together under "outpatient facility header". Need to make diff btw line and claim dx*/

	if inp=1 and visit=0 then output errors;
	if out=1 and visit=0 then output errors;
	if nch=1 and visit=0 then output errors;
	if dme=1 and visit=0 then output errors;
	if hha=1 and visit=0 then output errors;
	if hsp=1 and visit=0 then output errors;

	if visit=1;
	person_source_value = patient_id;

	if inp=1 then
	do i = 1 to dim(inp_dx);
		if inp_dx{i} ne "" then do;
			condition_occurrence_id+1;

			condition_concept_id = 1;
			condition_start_date = visit_start_date;
			condition_end_date = visit_end_date;

			*38000200 = Inpatient Header pos 1;
			condition_type_concept_id = min(38000200+i-1, 38000214);

			stop_reason = 0;

			associated_provider_id	= .;


			if 'E' = substr(inp_dx(i),1,1) then do;
				dx_left = substr(inp_dx(i),1,4);
				dx_right = substr(inp_dx(i),5,1);
			end;
			else do;
			    dx_left = substr(inp_dx(i),1,3);
				dx_right = substr(inp_dx(i),4,2);
			end;

			if lengthn(dx_right)>0 then
				condition_source_value = trim(dx_left) || "." || trim(dx_right);
			else
				condition_source_value = trim(dx_left);

			position = i;
			vocabulary_id = 2;
			output condition_occurrence_dups;
		end;
	end;

	*Remove lines that have revenue center = 0001 since not a real claim line so don't add dx from that line;
	if (out=1 or hha=1 or hsp=1) and center ne "0001" then
	do i = 1 to dim(out_dx);
		if out_dx{i} ne "" then do;
			condition_occurrence_id+1;

			condition_concept_id = 1;
			condition_start_date = visit_start_date;
			condition_end_date = visit_end_date;

			*38000215 = Outpatient Detail pos 1;
			condition_type_concept_id = min(38000215+i-1, 38000229);

			stop_reason = 0;

			if at_npi ne "" then associated_provider_id = at_npi;
			else if at_upin ne "" then associated_provider_id = at_upin;
			else if op_npi ne "" then associated_provider_id = op_npi;
			else if op_upin ne "" then associated_provider_id = op_upin;
			else if ot_npi ne "" then associated_provider_id = ot_npi;
			else if ot_upin ne "" then associated_provider_id = ot_upin;
			else if out=1 then associated_provider_id = "out";
			else if hha=1 then associated_provider_id = "hha";
			else if hsp=1 then associated_provider_id = "hsp";

			if 'E' = substr(out_dx(i),1,1) then do;
				dx_left = substr(out_dx(i),1,4);
				dx_right = substr(out_dx(i),5,1);
			end;
			else do;
			    dx_left = substr(out_dx(i),1,3);
				dx_right = substr(out_dx(i),4,2);
			end;

			if lengthn(dx_right)>0 then
				condition_source_value = trim(dx_left) || "." || trim(dx_right);
			else
				condition_source_value = trim(dx_left);

			position = i;
			vocabulary_id = 2;
			output condition_occurrence_dups;
		end;
	end;

	if out=1 and center ne "0001" then
	do i = 1 to dim(oute_dx);
		if oute_dx{i} ne "" then do;
			condition_occurrence_id+1;

			condition_concept_id = 1;
			condition_start_date = visit_start_date;
			condition_end_date = visit_end_date;

			*38000215 = Outpatient Detail pos 1;
			condition_type_concept_id = min(38000215+i-1, 38000229);

			stop_reason = 0;

			if at_npi ne "" then associated_provider_id = at_npi;
			else if at_upin ne "" then associated_provider_id = at_upin;
			else if op_npi ne "" then associated_provider_id = op_npi;
			else if op_upin ne "" then associated_provider_id = op_upin;
			else if ot_npi ne "" then associated_provider_id = ot_npi;
			else if ot_upin ne "" then associated_provider_id = ot_upin;
			else if out=1 then associated_provider_id = "out";
			else if hha=1 then associated_provider_id = "hha";
			else if hsp=1 then associated_provider_id = "hsp";

			if 'E' = substr(out_dx(i),1,1) then do;
				dx_left = substr(out_dx(i),1,4);
				dx_right = substr(out_dx(i),5,1);
			end;
			else do;
			    dx_left = substr(out_dx(i),1,3);
				dx_right = substr(out_dx(i),4,2);
			end;

			if lengthn(dx_right)>0 then
				condition_source_value = trim(dx_left) || "." || trim(dx_right);
			else
				condition_source_value = trim(dx_left);

			position = i;
			vocabulary_id = 2;
			output condition_occurrence_dups;
		end;
	end;

	if nch=1 or dme=1 then
	do i = 1 to dim(nch_dx);
		if nch_dx{i} ne "" then do;
			condition_occurrence_id+1;

			condition_concept_id = 1;
			condition_start_date = visit_start_date;
			condition_end_date = visit_end_date;

			*38000215 = Outpatient Detail pos 1;
			condition_type_concept_id = min(38000215+i-1, 38000229);

			stop_reason = 0;

			associated_provider_id	= provider;


			if 'E' = substr(nch_dx(i),1,1) then do;
				dx_left = substr(nch_dx(i),1,4);
				dx_right = substr(nch_dx(i),5,1);
			end;
			else do;
			    dx_left = substr(nch_dx(i),1,3);
				dx_right = substr(nch_dx(i),4,2);
			end;

			if lengthn(dx_right)>0 then
				condition_source_value = trim(dx_left) || "." || trim(dx_right);
			else
				condition_source_value = trim(dx_left);

			position = i;
			vocabulary_id = 2;
			output condition_occurrence_dups;
		end;
	end;

	drop dx_left dx_right;

run;

proc sort data=condition_occurrence_dups /*out=condition_occurrence_nodup nodupkey*/;
	by person_source_value provider condition_start_date condition_end_date condition_source_value condition_type_concept_id;
run;

proc sort data=condition_occurrence_dups out=condition_occurrence nodupkey;
	by person_source_value provider condition_start_date condition_end_date condition_source_value;
run;

************************************************************************************************************************************************************************************************;
*Create procedure occurrence table;
data procedure_occurrence(keep=procedure_occurrence_id	person_source_value	procedure_concept_id	procedure_date	provider
								procedure_type_concept_id	associated_provider_id	visit_occurrence_id	relevant_condition_concept_id procedure_source_value position vocabulary_id)
		errors;
	length provider $10;
	merge visit_occurrence(rename=(person_source_value=patient_id) in=visit )
		data.med (keep=patient_id srgcde: sg_dt: clm_from_dt clm_thru_dt provider  in=inp rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.out (keep=patient_id hcpcs center clm_from_dt clm_thru_dt provider in=out rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.nch (keep=patient_id hcpcs clm_from_dt clm_thru_dt provider in=nch rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.dme (keep=patient_id hcpcs clm_from_dt clm_thru_dt provider in=dme rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.hha (keep=patient_id hcpcs center clm_from_dt clm_thru_dt provider  in=hha rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date))
		data.hsp (keep=patient_id hcpcs center clm_from_dt clm_thru_dt provider  in=hsp rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date));

	by patient_id visit_start_date visit_end_date provider;

	length procedure_source_value $50;
	length procedure_date $10;
	length associated_provider_id $10;
	length icd9_pr_left $2;
	length icd9_pr_right $2;

	*Create YYYY-MM-DD format for surgery dates in the med file, use new variables num_sg_dt: in correct date format in rest of code;
	if inp=1 then do;
		array chard{*} sg_dt:;
		array num_sg_dt{25} $10. num_sg_dt1-num_sg_dt25;

		do k = 1 to dim(chard);
			if chard(k) ne "" then do;
			m=substr(chard(k),1,2);
			d=substr(chard(k),3,2);
			y=substr(chard(k),5,4);
			num_sg_dt(k)=catx('-',y,m,d);
			end;
		end;
	end;

	array inp_pr{*} srgcde:;
	array inp_dt{*} num_sg_dt:;



	if inp=1 and visit=0 then output errors;
	if out=1 and visit=0 then output errors;
	if nch=1 and visit=0 then output errors;
	if dme=1 and visit=0 then output errors;
	if hha=1 and visit=0 then output errors;
	if hsp=1 and visit=0 then output errors;

	if visit;
	person_source_value = patient_id;

	if inp=1 then
	do i = 1 to dim(inp_pr);
		if inp_pr{i} ne "" and anyalpha(inp_pr(i))=0 then do;
			procedure_occurrence_id+1;

			procedure_concept_id = 1;

			if inp_dt(i) = "" then procedure_date = visit_start_date;
				else procedure_date = inp_dt{i};

			*38000251 = Inpatient Detail pos 1;
			procedure_type_concept_id = min(38000183+i-1, 38000198);

			associated_provider_id	= "inp facility";

			relevant_condition_concept_id=0;

			if length(inp_pr(i)) = 4 then do;
				icd9_pr_left = substr(inp_pr(i),1,2);
				icd9_pr_right = substr(inp_pr(i),3,2);
				end;
			else do;
				icd9_pr_left = substr(inp_pr(i),1,2);
				icd9_pr_right = substr(inp_pr(i),3,1);
				end;

			procedure_source_value = trim(icd9_pr_left) || "." || trim(icd9_pr_right);
			position = i;
			vocabulary_id = 3;
			output procedure_occurrence;
		end;
	end;

	if nch=1 or dme=1 then do;
		if hcpcs ne "" then do;
			procedure_occurrence_id+1;

			procedure_concept_id = 1;

			procedure_date = visit_start_date;

			*38000269 = Outpatient Detail pos 1;
			procedure_type_concept_id = 38000215;

			associated_provider_id	= provider;

			relevant_condition_concept_id=0;

			procedure_source_value = hcpcs;
			position = i;

			*Check if the first character is a string then the vocab should be hcpcs, else cpt;
			if anyalpha(substr(hcpcs,1,1)) then vocabulary_id = 5;
			else vocabulary_id = 4;
			output procedure_occurrence;
		end;
	end;

	if out=1 or hha=1 or hsp=1 then do;
		if hcpcs ne "" then do;
			procedure_occurrence_id+1;

			procedure_concept_id = 1;

			procedure_date = visit_start_date;

			*38000269 = Outpatient Detail pos 1;
			procedure_type_concept_id = 38000215;

			if at_npi ne "" then associated_provider_id = at_npi;
			else if at_upin ne "" then associated_provider_id = at_upin;
			else if op_npi ne "" then associated_provider_id = op_npi;
			else if op_upin ne "" then associated_provider_id = op_upin;
			else if ot_npi ne "" then associated_provider_id = ot_npi;
			else if ot_upin ne "" then associated_provider_id = ot_upin;
			else if out=1 then associated_provider_id = "out";
			else if hha=1 then associated_provider_id = "hha";
			else if hsp=1 then associated_provider_id = "hsp";

			relevant_condition_concept_id = 0;

			procedure_source_value = hcpcs;
			position = i;

			*Check if the first character is a string then the vocab should be hcpcs, else cpt;
			if anyalpha(substr(hcpcs,1,1)) then vocabulary_id = 5;
			else vocabulary_id = 4;
			output procedure_occurrence;
		end;
	end;

	drop icd9_pr_left icd9_pr_right;

run;

************************************************************************************************************************************************************************************************;
*Create provider table. Use "associated_provider_id" from condition and procedure tables to link to the provider_id in sql below;
*Med file does not have performing provider information on file;
*NOTE: This table was not detailed in documentation file.  This is test code.  Commented out to prevent running code;

/*data provider_dups (keep= provider_id npi dea specialty_concept_id care_site_id provider_source_value specialty_source_value patient_id provider);
	length provider $10;
	set
		data.nch (keep=patient_id provider hcfaspec in=nch)
		data.dme (keep=patient_id provider hcfaspec in=dme);


	retain provider_id 0;

	length specialty_source_value $50;
	length provider_source_value $10;


	if nch=1 then do;
		provider_id + 1;
		provider_source_value = provider;
		specialty_source_value = hcfaspec;
		care_site_id = 0;
		npi = .;
		dea = .;
		if hcfaspec = "87" then do;
			specialty_source_value = "87 - All Other Suppliers";
			specialty_concept_id = 38004698;
		end;
		else if hcfaspec = "59" then do;
			specialty_source_value = "59 - Ambulance Service Provider";
			specialty_concept_id = 38004688;
		end;
		else if hcfaspec = "49" then do;
			specialty_source_value = "49 - Ambulance Service Center";
			specialty_concept_id = 38004679;
		end;
		else if hcfaspec = "32" then do;
			specialty_source_value = "32 - Anesthesiology Assistant";
			specialty_concept_id = 38004676;
		end;
		else if hcfaspec = "70" then do;
			specialty_source_value = "70 - Clinic or Group Practice";
			specialty_concept_id = 38004693;
		end;
		else if hcfaspec = "69" then do;
			specialty_source_value = "69 - Clinical Laboratory";
			specialty_concept_id = 38004692;
		end;
		else if hcfaspec = "47" then do;
			specialty_source_value = "47 - Independent Diagnostic Testing Facility";
			specialty_concept_id = 38004678;
		end;
		else if hcfaspec = "A9" then do;
			specialty_source_value = "A9 - Indian Health Service facility";
			specialty_concept_id = 38004702;
		end;
		else if hcfaspec = "55" then do;
			specialty_source_value = "55 - Individual Certified Orthotist";
			specialty_concept_id = 38004684;
		end;
		else if hcfaspec = "56" then do;
			specialty_source_value = "56 - Individual Certified Prosthetist";
			specialty_concept_id = 38004685;
		end;
		else if hcfaspec = "57" then do;
			specialty_source_value = "57 - Individual Certified Prosthetist-Orthotist";
			specialty_concept_id = 38004686;
		end;
		else if hcfaspec = "45" then do;
			specialty_source_value = "45 - Mammography Center";
			specialty_concept_id = 38004677;
		end;
		else if hcfaspec = "73" then do;
			specialty_source_value = "73 - Mass Immunizer Roster Biller";
			specialty_concept_id = 38004695;
		end;
		else if hcfaspec = "51" then do;
			specialty_source_value = "51 - Medical Supply Company with Orthotist";
			specialty_concept_id = 38004680;
		end;
		else if hcfaspec = "53" then do;
			specialty_source_value = "53 - Medical Supply Company with Orthotist-Prosthetist";
			specialty_concept_id = 38004682;
		end;
		else if hcfaspec = "58" then do;
			specialty_source_value = "58 - Medical Supply Company with Pharmacist";
			specialty_concept_id = 38004687;
		end;
		else if hcfaspec = "52" then do;
			specialty_source_value = "52 - Medical Supply Company with Prosthetist";
			specialty_concept_id = 38004681;
		end;
		else if hcfaspec = "96" then do;
			specialty_source_value = "96 - Optician";
			specialty_concept_id = 38004701;
		end;
		else if hcfaspec = "54" then do;
			specialty_source_value = "54 - Other Medical Supply Company";
			specialty_concept_id = 38004683;
		end;
		else if hcfaspec = "B1" then do;
			specialty_source_value = "B1 - Oxygen supplier";
			specialty_concept_id = 38004703;
		end;
		else if hcfaspec = "95" then do;
			specialty_source_value = "95 - Part B CAP Drug Vendor";
			specialty_concept_id = 38004700;
		end;
		else if hcfaspec = "30" then do;
			specialty_source_value = "30 - Physician/Diagnostic Radiology";
			specialty_concept_id = 38004675;
		end;
		else if hcfaspec = "63" then do;
			specialty_source_value = "63 - Portable X-Ray Supplier";
			specialty_concept_id = 38004691;
		end;
		else if hcfaspec = "60" then do;
			specialty_source_value = "60 - Public Health or Welfare Agency";
			specialty_concept_id = 38004689;
		end;
		else if hcfaspec = "74" then do;
			specialty_source_value = "74 - Radiation Therapy Center";
			specialty_concept_id = 38004696;
		end;
		else if hcfaspec = "71" then do;
			specialty_source_value = "71 - Registered Dietitian or Nutrition Professional";
			specialty_concept_id = 38004694;
		end;
		else if hcfaspec = "75" then do;
			specialty_source_value = "75 - Slide Preparation Facility";
			specialty_concept_id = 38004697;
		end;
		else if hcfaspec = "88" then do;
			specialty_source_value = "88 - Unknown Supplier/Provider Specialty";
			specialty_concept_id = 38004699;
		end;
		else if hcfaspec = "61" then do;
			specialty_source_value = "61 - Voluntary Health or Charitable Agency";
			specialty_concept_id = 38004690;
		end;
		else if hcfaspec = "79" then do;
			specialty_source_value = "79 - Addiction Medicine";
			specialty_concept_id = 38004498;
		end;
		else if hcfaspec = "03" then do;
			specialty_source_value = "03 - Allergy/Immunology";
			specialty_concept_id = 38004448;
		end;
		else if hcfaspec = "05" then do;
			specialty_source_value = "05 - Anesthesiology";
			specialty_concept_id = 38004450;
		end;
		else if hcfaspec = "64" then do;
			specialty_source_value = "64 - Audiology";
			specialty_concept_id = 38004489;
		end;
		else if hcfaspec = "78" then do;
			specialty_source_value = "78 - Cardiac Surgery";
			specialty_concept_id = 38004497;
		end;
		else if hcfaspec = "06" then do;
			specialty_source_value = "06 - Cardiology";
			specialty_concept_id = 38004451;
		end;
		else if hcfaspec = "89" then do;
			specialty_source_value = "89 - Certified Clinical Nurse Specialist";
			specialty_concept_id = 38004506;
		end;
		else if hcfaspec = "42" then do;
			specialty_source_value = "42 - Certified Nurse Midwife";
			specialty_concept_id = 38004482;
		end;
		else if hcfaspec = "43" then do;
			specialty_source_value = "43 - Certified Registered Nurse Anesthetist";
			specialty_concept_id = 38004483;
		end;
		else if hcfaspec = "35" then do;
			specialty_source_value = "35 - Chiropractic";
			specialty_concept_id = 38004475;
		end;
		else if hcfaspec = "68" then do;
			specialty_source_value = "68 - Clinical Psychology";
			specialty_concept_id = 38004493;
		end;
		else if hcfaspec = "28" then do;
			specialty_source_value = "28 - Colorectal Surgery";
			specialty_concept_id = 38004471;
		end;
		else if hcfaspec = "81" then do;
			specialty_source_value = "81 - Critical care (intensivist)";
			specialty_concept_id = 38004500;
		end;
		else if hcfaspec = "A7" then do;
			specialty_source_value = "A7 - Department Store";
			specialty_concept_id = 38004522;
		end;
		else if hcfaspec = "07" then do;
			specialty_source_value = "07 - Dermatology";
			specialty_concept_id = 38004452;
		end;
		else if hcfaspec = "93" then do;
			specialty_source_value = "93 - Emergency Medicine";
			specialty_concept_id = 38004510;
		end;
		else if hcfaspec = "46" then do;
			specialty_source_value = "46 - Endocrinology";
			specialty_concept_id = 38004485;
		end;
		else if hcfaspec = "08" then do;
			specialty_source_value = "08 - Family Practice";
			specialty_concept_id = 38004453;
		end;
		else if hcfaspec = "10" then do;
			specialty_source_value = "10 - Gastroenterology";
			specialty_concept_id = 38004455;
		end;
		else if hcfaspec = "01" then do;
			specialty_source_value = "01 - General Practice";
			specialty_concept_id = 38004446;
		end;
		else if hcfaspec = "02" then do;
			specialty_source_value = "02 - General Surgery";
			specialty_concept_id = 38004447;
		end;
		else if hcfaspec = "38" then do;
			specialty_source_value = "38 - Geriatric Medicine";
			specialty_concept_id = 38004478;
		end;
		else if hcfaspec = "27" then do;
			specialty_source_value = "27 - Geriatric Psychiatry";
			specialty_concept_id = 38004470;
		end;
		else if hcfaspec = "A8" then do;
			specialty_source_value = "A8 - Grocery Store";
			specialty_concept_id = 38004523;
		end;
		else if hcfaspec = "98" then do;
			specialty_source_value = "98 - Gynecology/Oncology";
			specialty_concept_id = 38004513;
		end;
		else if hcfaspec = "40" then do;
			specialty_source_value = "40 - Hand Surgery";
			specialty_concept_id = 38004480;
		end;
		else if hcfaspec = "82" then do;
			specialty_source_value = "82 - Hematology";
			specialty_concept_id = 38004501;
		end;
		else if hcfaspec = "83" then do;
			specialty_source_value = "83 - Hematology/Oncology";
			specialty_concept_id = 38004502;
		end;
		else if hcfaspec = "A4" then do;
			specialty_source_value = "A4 - Home Health Agency";
			specialty_concept_id = 38004519;
		end;
		else if hcfaspec = "17" then do;
			specialty_source_value = "17 - Hospice And Palliative Care";
			specialty_concept_id = 38004462;
		end;
		else if hcfaspec = "A0" then do;
			specialty_source_value = "A0 - Hospital";
			specialty_concept_id = 38004515;
		end;
		else if hcfaspec = "44" then do;
			specialty_source_value = "44 - Infectious Disease";
			specialty_concept_id = 38004484;
		end;
		else if hcfaspec = "A2" then do;
			specialty_source_value = "A2 - Intermediate Care Nursing Facility";
			specialty_concept_id = 38004517;
		end;
		else if hcfaspec = "11" then do;
			specialty_source_value = "11 - Internal Medicine";
			specialty_concept_id = 38004456;
		end;
		else if hcfaspec = "09" then do;
			specialty_source_value = "09 - Interventional Pain Management (IPM)";
			specialty_concept_id = 38004454;
		end;
		else if hcfaspec = "94" then do;
			specialty_source_value = "94 - Interventional Radiology";
			specialty_concept_id = 38004511;
		end;
		else if hcfaspec = "80" then do;
			specialty_source_value = "80 - Licensed Clinical Social Worker";
			specialty_concept_id = 38004499;
		end;
		else if hcfaspec = "85" then do;
			specialty_source_value = "85 - Maxillofacial Surgery";
			specialty_concept_id = 38004504;
		end;
		else if hcfaspec = "90" then do;
			specialty_source_value = "90 - Medical Oncology";
			specialty_concept_id = 38004507;
		end;
		else if hcfaspec = "B3" then do;
			specialty_source_value = "B3 - Medical Supply Company with Pedorthic Personnel";
			specialty_concept_id = 38004525;
		end;
		else if hcfaspec = "A6" then do;
			specialty_source_value = "A6 - Medical Supply Company with Respiratory Therapist";
			specialty_concept_id = 38004521;
		end;
		else if hcfaspec = "39" then do;
			specialty_source_value = "39 - Nephrology";
			specialty_concept_id = 38004479;
		end;
		else if hcfaspec = "13" then do;
			specialty_source_value = "13 - Neurology";
			specialty_concept_id = 38004458;
		end;
		else if hcfaspec = "86" then do;
			specialty_source_value = "86 - Neuropsychiatry";
			specialty_concept_id = 38004505;
		end;
		else if hcfaspec = "14" then do;
			specialty_source_value = "14 - Neurosurgery";
			specialty_concept_id = 38004459;
		end;
		else if hcfaspec = "36" then do;
			specialty_source_value = "36 - Nuclear Medicine";
			specialty_concept_id = 38004476;
		end;
		else if hcfaspec = "50" then do;
			specialty_source_value = "50 - Nurse Practitioner";
			specialty_concept_id = 38004487;
		end;
		else if hcfaspec = "16" then do;
			specialty_source_value = "16 - Obstetrics/Gynecology";
			specialty_concept_id = 38004461;
		end;
		else if hcfaspec = "67" then do;
			specialty_source_value = "67 - Occupational Therapy";
			specialty_concept_id = 38004492;
		end;
		else if hcfaspec = "B5" then do;
			specialty_source_value = "B5 - Ocularist";
			specialty_concept_id = 38004527;
		end;
		else if hcfaspec = "18" then do;
			specialty_source_value = "18 - Ophthalmology";
			specialty_concept_id = 38004463;
		end;
		else if hcfaspec = "41" then do;
			specialty_source_value = "41 - Optometry";
			specialty_concept_id = 38004481;
		end;
		else if hcfaspec = "19" then do;
			specialty_source_value = "19 - Oral Surgery";
			specialty_concept_id = 38004464;
		end;
		else if hcfaspec = "20" then do;
			specialty_source_value = "20 - Orthopedic Surgery";
			specialty_concept_id = 38004465;
		end;
		else if hcfaspec = "12" then do;
			specialty_source_value = "12 - Osteopathic Manipulative Therapy";
			specialty_concept_id = 38004457;
		end;
		else if hcfaspec = "A3" then do;
			specialty_source_value = "A3 - Other Nursing Facility";
			specialty_concept_id = 38004518;
		end;
		else if hcfaspec = "04" then do;
			specialty_source_value = "04 - Otolaryngology";
			specialty_concept_id = 38004449;
		end;
		else if hcfaspec = "72" then do;
			specialty_source_value = "72 - Pain Management";
			specialty_concept_id = 38004494;
		end;
		else if hcfaspec = "22" then do;
			specialty_source_value = "22 - Pathology";
			specialty_concept_id = 38004466;
		end;
		else if hcfaspec = "37" then do;
			specialty_source_value = "37 - Pediatric Medicine";
			specialty_concept_id = 38004477;
		end;
		else if hcfaspec = "B2" then do;
			specialty_source_value = "B2 - Pedorthic Personnel";
			specialty_concept_id = 38004524;
		end;
		else if hcfaspec = "76" then do;
			specialty_source_value = "76 - Peripheral Vascular Disease";
			specialty_concept_id = 38004495;
		end;
		else if hcfaspec = "A5" then do;
			specialty_source_value = "A5 - Pharmacy";
			specialty_concept_id = 38004520;
		end;
		else if hcfaspec = "25" then do;
			specialty_source_value = "25 - Physical Medicine And Rehabilitation";
			specialty_concept_id = 38004468;
		end;
		else if hcfaspec = "65" then do;
			specialty_source_value = "65 - Physical Therapy";
			specialty_concept_id = 38004490;
		end;
		else if hcfaspec = "97" then do;
			specialty_source_value = "97 - Physician Assistant";
			specialty_concept_id = 38004512;
		end;
		else if hcfaspec = "24" then do;
			specialty_source_value = "24 - Plastic And Reconstructive Surgery";
			specialty_concept_id = 38004467;
		end;
		else if hcfaspec = "48" then do;
			specialty_source_value = "48 - Podiatry";
			specialty_concept_id = 38004486;
		end;
		else if hcfaspec = "84" then do;
			specialty_source_value = "84 - Preventive Medicine";
			specialty_concept_id = 38004503;
		end;
		else if hcfaspec = "26" then do;
			specialty_source_value = "26 - Psychiatry";
			specialty_concept_id = 38004469;
		end;
		else if hcfaspec = "62" then do;
			specialty_source_value = "62 - Psychology";
			specialty_concept_id = 38004488;
		end;
		else if hcfaspec = "29" then do;
			specialty_source_value = "29 - Pulmonary Disease";
			specialty_concept_id = 38004472;
		end;
		else if hcfaspec = "92" then do;
			specialty_source_value = "92 - Radiation Oncology";
			specialty_concept_id = 38004509;
		end;
		else if hcfaspec = "B4" then do;
			specialty_source_value = "B4 - Rehabilitation Agency";
			specialty_concept_id = 38004526;
		end;
		else if hcfaspec = "66" then do;
			specialty_source_value = "66 - Rheumatology";
			specialty_concept_id = 38004491;
		end;
		else if hcfaspec = "A1" then do;
			specialty_source_value = "A1 - Skilled Nursing Facility";
			specialty_concept_id = 38004516;
		end;
		else if hcfaspec = "15" then do;
			specialty_source_value = "15 - Speech Language Pathology";
			specialty_concept_id = 38004460;
		end;
		else if hcfaspec = "91" then do;
			specialty_source_value = "91 - Surgical Oncology";
			specialty_concept_id = 38004508;
		end;
		else if hcfaspec = "33" then do;
			specialty_source_value = "33 - Thoracic Surgery";
			specialty_concept_id = 38004473;
		end;
		else if hcfaspec = "34" then do;
			specialty_source_value = "34 - Urology";
			specialty_concept_id = 38004474;
		end;
		else if hcfaspec = "77" then do;
			specialty_source_value = "77 - Vascular Surgery";
			specialty_concept_id = 38004496;
		end;
		else if hcfaspec = "99" then do;
			specialty_source_value = "99 - Unknown Physician Specialty";
			specialty_concept_id = 38004514;
		end;
		else do;
			specialty_source_value = "99 - Unknown Physician Specialty";
			specialty_concept_id = 38004514;
		end;
	end;

	if dme=1 then do;
		provider_id + 1;
		provider_source_value = provider;
		care_site_id = 0;
		npi = .;
		dea = .;

		if hcfaspec = "87" then do;
			specialty_source_value = "87 - All Other Suppliers";
			specialty_concept_id = 38004698;
		end;
		else if hcfaspec = "59" then do;
			specialty_source_value = "59 - Ambulance Service Provider";
			specialty_concept_id = 38004688;
		end;
		else if hcfaspec = "49" then do;
			specialty_source_value = "49 - Ambulance Service Center";
			specialty_concept_id = 38004679;
		end;
		else if hcfaspec = "32" then do;
			specialty_source_value = "32 - Anesthesiology Assistant";
			specialty_concept_id = 38004676;
		end;
		else if hcfaspec = "70" then do;
			specialty_source_value = "70 - Clinic or Group Practice";
			specialty_concept_id = 38004693;
		end;
		else if hcfaspec = "69" then do;
			specialty_source_value = "69 - Clinical Laboratory";
			specialty_concept_id = 38004692;
		end;
		else if hcfaspec = "47" then do;
			specialty_source_value = "47 - Independent Diagnostic Testing Facility";
			specialty_concept_id = 38004678;
		end;
		else if hcfaspec = "A9" then do;
			specialty_source_value = "A9 - Indian Health Service facility";
			specialty_concept_id = 38004702;
		end;
		else if hcfaspec = "55" then do;
			specialty_source_value = "55 - Individual Certified Orthotist";
			specialty_concept_id = 38004684;
		end;
		else if hcfaspec = "56" then do;
			specialty_source_value = "56 - Individual Certified Prosthetist";
			specialty_concept_id = 38004685;
		end;
		else if hcfaspec = "57" then do;
			specialty_source_value = "57 - Individual Certified Prosthetist-Orthotist";
			specialty_concept_id = 38004686;
		end;
		else if hcfaspec = "45" then do;
			specialty_source_value = "45 - Mammography Center";
			specialty_concept_id = 38004677;
		end;
		else if hcfaspec = "73" then do;
			specialty_source_value = "73 - Mass Immunizer Roster Biller";
			specialty_concept_id = 38004695;
		end;
		else if hcfaspec = "51" then do;
			specialty_source_value = "51 - Medical Supply Company with Orthotist";
			specialty_concept_id = 38004680;
		end;
		else if hcfaspec = "53" then do;
			specialty_source_value = "53 - Medical Supply Company with Orthotist-Prosthetist";
			specialty_concept_id = 38004682;
		end;
		else if hcfaspec = "58" then do;
			specialty_source_value = "58 - Medical Supply Company with Pharmacist";
			specialty_concept_id = 38004687;
		end;
		else if hcfaspec = "52" then do;
			specialty_source_value = "52 - Medical Supply Company with Prosthetist";
			specialty_concept_id = 38004681;
		end;
		else if hcfaspec = "96" then do;
			specialty_source_value = "96 - Optician";
			specialty_concept_id = 38004701;
		end;
		else if hcfaspec = "54" then do;
			specialty_source_value = "54 - Other Medical Supply Company";
			specialty_concept_id = 38004683;
		end;
		else if hcfaspec = "B1" then do;
			specialty_source_value = "B1 - Oxygen supplier";
			specialty_concept_id = 38004703;
		end;
		else if hcfaspec = "95" then do;
			specialty_source_value = "95 - Part B CAP Drug Vendor";
			specialty_concept_id = 38004700;
		end;
		else if hcfaspec = "30" then do;
			specialty_source_value = "30 - Physician/Diagnostic Radiology";
			specialty_concept_id = 38004675;
		end;
		else if hcfaspec = "63" then do;
			specialty_source_value = "63 - Portable X-Ray Supplier";
			specialty_concept_id = 38004691;
		end;
		else if hcfaspec = "60" then do;
			specialty_source_value = "60 - Public Health or Welfare Agency";
			specialty_concept_id = 38004689;
		end;
		else if hcfaspec = "74" then do;
			specialty_source_value = "74 - Radiation Therapy Center";
			specialty_concept_id = 38004696;
		end;
		else if hcfaspec = "71" then do;
			specialty_source_value = "71 - Registered Dietitian or Nutrition Professional";
			specialty_concept_id = 38004694;
		end;
		else if hcfaspec = "75" then do;
			specialty_source_value = "75 - Slide Preparation Facility";
			specialty_concept_id = 38004697;
		end;
		else if hcfaspec = "61" then do;
			specialty_source_value = "61 - Voluntary Health or Charitable Agency";
			specialty_concept_id = 38004690;
		end;
		else if hcfaspec = "79" then do;
			specialty_source_value = "79 - Addiction Medicine";
			specialty_concept_id = 38004498;
		end;
		else if hcfaspec = "03" then do;
			specialty_source_value = "03 - Allergy/Immunology";
			specialty_concept_id = 38004448;
		end;
		else if hcfaspec = "05" then do;
			specialty_source_value = "05 - Anesthesiology";
			specialty_concept_id = 38004450;
		end;
		else if hcfaspec = "64" then do;
			specialty_source_value = "64 - Audiology";
			specialty_concept_id = 38004489;
		end;
		else if hcfaspec = "78" then do;
			specialty_source_value = "78 - Cardiac Surgery";
			specialty_concept_id = 38004497;
		end;
		else if hcfaspec = "06" then do;
			specialty_source_value = "06 - Cardiology";
			specialty_concept_id = 38004451;
		end;
		else if hcfaspec = "89" then do;
			specialty_source_value = "89 - Certified Clinical Nurse Specialist";
			specialty_concept_id = 38004506;
		end;
		else if hcfaspec = "42" then do;
			specialty_source_value = "42 - Certified Nurse Midwife";
			specialty_concept_id = 38004482;
		end;
		else if hcfaspec = "43" then do;
			specialty_source_value = "43 - Certified Registered Nurse Anesthetist";
			specialty_concept_id = 38004483;
		end;
		else if hcfaspec = "35" then do;
			specialty_source_value = "35 - Chiropractic";
			specialty_concept_id = 38004475;
		end;
		else if hcfaspec = "68" then do;
			specialty_source_value = "68 - Clinical Psychology";
			specialty_concept_id = 38004493;
		end;
		else if hcfaspec = "28" then do;
			specialty_source_value = "28 - Colorectal Surgery";
			specialty_concept_id = 38004471;
		end;
		else if hcfaspec = "81" then do;
			specialty_source_value = "81 - Critical care (intensivist)";
			specialty_concept_id = 38004500;
		end;
		else if hcfaspec = "A7" then do;
			specialty_source_value = "A7 - Department Store";
			specialty_concept_id = 38004522;
		end;
		else if hcfaspec = "07" then do;
			specialty_source_value = "07 - Dermatology";
			specialty_concept_id = 38004452;
		end;
		else if hcfaspec = "93" then do;
			specialty_source_value = "93 - Emergency Medicine";
			specialty_concept_id = 38004510;
		end;
		else if hcfaspec = "46" then do;
			specialty_source_value = "46 - Endocrinology";
			specialty_concept_id = 38004485;
		end;
		else if hcfaspec = "08" then do;
			specialty_source_value = "08 - Family Practice";
			specialty_concept_id = 38004453;
		end;
		else if hcfaspec = "10" then do;
			specialty_source_value = "10 - Gastroenterology";
			specialty_concept_id = 38004455;
		end;
		else if hcfaspec = "01" then do;
			specialty_source_value = "01 - General Practice";
			specialty_concept_id = 38004446;
		end;
		else if hcfaspec = "02" then do;
			specialty_source_value = "02 - General Surgery";
			specialty_concept_id = 38004447;
		end;
		else if hcfaspec = "38" then do;
			specialty_source_value = "38 - Geriatric Medicine";
			specialty_concept_id = 38004478;
		end;
		else if hcfaspec = "27" then do;
			specialty_source_value = "27 - Geriatric Psychiatry";
			specialty_concept_id = 38004470;
		end;
		else if hcfaspec = "A8" then do;
			specialty_source_value = "A8 - Grocery Store";
			specialty_concept_id = 38004523;
		end;
		else if hcfaspec = "98" then do;
			specialty_source_value = "98 - Gynecology/Oncology";
			specialty_concept_id = 38004513;
		end;
		else if hcfaspec = "40" then do;
			specialty_source_value = "40 - Hand Surgery";
			specialty_concept_id = 38004480;
		end;
		else if hcfaspec = "82" then do;
			specialty_source_value = "82 - Hematology";
			specialty_concept_id = 38004501;
		end;
		else if hcfaspec = "83" then do;
			specialty_source_value = "83 - Hematology/Oncology";
			specialty_concept_id = 38004502;
		end;
		else if hcfaspec = "A4" then do;
			specialty_source_value = "A4 - Home Health Agency";
			specialty_concept_id = 38004519;
		end;
		else if hcfaspec = "17" then do;
			specialty_source_value = "17 - Hospice And Palliative Care";
			specialty_concept_id = 38004462;
		end;
		else if hcfaspec = "A0" then do;
			specialty_source_value = "A0 - Hospital";
			specialty_concept_id = 38004515;
		end;
		else if hcfaspec = "44" then do;
			specialty_source_value = "44 - Infectious Disease";
			specialty_concept_id = 38004484;
		end;
		else if hcfaspec = "A2" then do;
			specialty_source_value = "A2 - Intermediate Care Nursing Facility";
			specialty_concept_id = 38004517;
		end;
		else if hcfaspec = "11" then do;
			specialty_source_value = "11 - Internal Medicine";
			specialty_concept_id = 38004456;
		end;
		else if hcfaspec = "09" then do;
			specialty_source_value = "09 - Interventional Pain Management (IPM)";
			specialty_concept_id = 38004454;
		end;
		else if hcfaspec = "94" then do;
			specialty_source_value = "94 - Interventional Radiology";
			specialty_concept_id = 38004511;
		end;
		else if hcfaspec = "80" then do;
			specialty_source_value = "80 - Licensed Clinical Social Worker";
			specialty_concept_id = 38004499;
		end;
		else if hcfaspec = "85" then do;
			specialty_source_value = "85 - Maxillofacial Surgery";
			specialty_concept_id = 38004504;
		end;
		else if hcfaspec = "90" then do;
			specialty_source_value = "90 - Medical Oncology";
			specialty_concept_id = 38004507;
		end;
		else if hcfaspec = "B3" then do;
			specialty_source_value = "B3 - Medical Supply Company with Pedorthic Personnel";
			specialty_concept_id = 38004525;
		end;
		else if hcfaspec = "A6" then do;
			specialty_source_value = "A6 - Medical Supply Company with Respiratory Therapist";
			specialty_concept_id = 38004521;
		end;
		else if hcfaspec = "39" then do;
			specialty_source_value = "39 - Nephrology";
			specialty_concept_id = 38004479;
		end;
		else if hcfaspec = "13" then do;
			specialty_source_value = "13 - Neurology";
			specialty_concept_id = 38004458;
		end;
		else if hcfaspec = "86" then do;
			specialty_source_value = "86 - Neuropsychiatry";
			specialty_concept_id = 38004505;
		end;
		else if hcfaspec = "14" then do;
			specialty_source_value = "14 - Neurosurgery";
			specialty_concept_id = 38004459;
		end;
		else if hcfaspec = "36" then do;
			specialty_source_value = "36 - Nuclear Medicine";
			specialty_concept_id = 38004476;
		end;
		else if hcfaspec = "50" then do;
			specialty_source_value = "50 - Nurse Practitioner";
			specialty_concept_id = 38004487;
		end;
		else if hcfaspec = "16" then do;
			specialty_source_value = "16 - Obstetrics/Gynecology";
			specialty_concept_id = 38004461;
		end;
		else if hcfaspec = "67" then do;
			specialty_source_value = "67 - Occupational Therapy";
			specialty_concept_id = 38004492;
		end;
		else if hcfaspec = "B5" then do;
			specialty_source_value = "B5 - Ocularist";
			specialty_concept_id = 38004527;
		end;
		else if hcfaspec = "18" then do;
			specialty_source_value = "18 - Ophthalmology";
			specialty_concept_id = 38004463;
		end;
		else if hcfaspec = "41" then do;
			specialty_source_value = "41 - Optometry";
			specialty_concept_id = 38004481;
		end;
		else if hcfaspec = "19" then do;
			specialty_source_value = "19 - Oral Surgery";
			specialty_concept_id = 38004464;
		end;
		else if hcfaspec = "20" then do;
			specialty_source_value = "20 - Orthopedic Surgery";
			specialty_concept_id = 38004465;
		end;
		else if hcfaspec = "12" then do;
			specialty_source_value = "12 - Osteopathic Manipulative Therapy";
			specialty_concept_id = 38004457;
		end;
		else if hcfaspec = "A3" then do;
			specialty_source_value = "A3 - Other Nursing Facility";
			specialty_concept_id = 38004518;
		end;
		else if hcfaspec = "04" then do;
			specialty_source_value = "04 - Otolaryngology";
			specialty_concept_id = 38004449;
		end;
		else if hcfaspec = "72" then do;
			specialty_source_value = "72 - Pain Management";
			specialty_concept_id = 38004494;
		end;
		else if hcfaspec = "22" then do;
			specialty_source_value = "22 - Pathology";
			specialty_concept_id = 38004466;
		end;
		else if hcfaspec = "37" then do;
			specialty_source_value = "37 - Pediatric Medicine";
			specialty_concept_id = 38004477;
		end;
		else if hcfaspec = "B2" then do;
			specialty_source_value = "B2 - Pedorthic Personnel";
			specialty_concept_id = 38004524;
		end;
		else if hcfaspec = "76" then do;
			specialty_source_value = "76 - Peripheral Vascular Disease";
			specialty_concept_id = 38004495;
		end;
		else if hcfaspec = "A5" then do;
			specialty_source_value = "A5 - Pharmacy";
			specialty_concept_id = 38004520;
		end;
		else if hcfaspec = "25" then do;
			specialty_source_value = "25 - Physical Medicine And Rehabilitation";
			specialty_concept_id = 38004468;
		end;
		else if hcfaspec = "65" then do;
			specialty_source_value = "65 - Physical Therapy";
			specialty_concept_id = 38004490;
		end;
		else if hcfaspec = "97" then do;
			specialty_source_value = "97 - Physician Assistant";
			specialty_concept_id = 38004512;
		end;
		else if hcfaspec = "24" then do;
			specialty_source_value = "24 - Plastic And Reconstructive Surgery";
			specialty_concept_id = 38004467;
		end;
		else if hcfaspec = "48" then do;
			specialty_source_value = "48 - Podiatry";
			specialty_concept_id = 38004486;
		end;
		else if hcfaspec = "84" then do;
			specialty_source_value = "84 - Preventive Medicine";
			specialty_concept_id = 38004503;
		end;
		else if hcfaspec = "26" then do;
			specialty_source_value = "26 - Psychiatry";
			specialty_concept_id = 38004469;
		end;
		else if hcfaspec = "62" then do;
			specialty_source_value = "62 - Psychology";
			specialty_concept_id = 38004488;
		end;
		else if hcfaspec = "29" then do;
			specialty_source_value = "29 - Pulmonary Disease";
			specialty_concept_id = 38004472;
		end;
		else if hcfaspec = "92" then do;
			specialty_source_value = "92 - Radiation Oncology";
			specialty_concept_id = 38004509;
		end;
		else if hcfaspec = "B4" then do;
			specialty_source_value = "B4 - Rehabilitation Agency";
			specialty_concept_id = 38004526;
		end;
		else if hcfaspec = "66" then do;
			specialty_source_value = "66 - Rheumatology";
			specialty_concept_id = 38004491;
		end;
		else if hcfaspec = "A1" then do;
			specialty_source_value = "A1 - Skilled Nursing Facility";
			specialty_concept_id = 38004516;
		end;
		else if hcfaspec = "15" then do;
			specialty_source_value = "15 - Speech Language Pathology";
			specialty_concept_id = 38004460;
		end;
		else if hcfaspec = "91" then do;
			specialty_source_value = "91 - Surgical Oncology";
			specialty_concept_id = 38004508;
		end;
		else if hcfaspec = "33" then do;
			specialty_source_value = "33 - Thoracic Surgery";
			specialty_concept_id = 38004473;
		end;
		else if hcfaspec = "34" then do;
			specialty_source_value = "34 - Urology";
			specialty_concept_id = 38004474;
		end;
		else if hcfaspec = "77" then do;
			specialty_source_value = "77 - Vascular Surgery";
			specialty_concept_id = 38004496;
		end;
		else if hcfaspec = "99" then do;
			specialty_source_value = "99 - Unknown Physician Specialty";
			specialty_concept_id = 38004514;
		end;
		else if hcfaspec = "88" then do;
			specialty_source_value = "88 - Unknown Supplier/Provider Specialty";
			specialty_concept_id = 38004699;
		end;
		else do;
			specialty_source_value = "88 - Unknown Supplier/Provider Specialty";
			specialty_concept_id = 38004699;
		end;
	end;

	/*if out=1 or hha=1 or hsp=1 then do;
		provider_id + 1;
		npi = .;
		dea = .;
		*Assign provider source value to attending, operating or other physician, preference to NPI over UPIN;
		if at_npi ne "" then provider_source_value = at_npi;
		else if at_upin ne "" then provider_source_value = at_upin;
		else if op_npi ne "" then provider_source_value = op_npi;
		else if op_upin ne "" then provider_source_value = op_upin;
		else if ot_npi ne "" then provider_source_value = ot_npi;
		else if ot_upin ne "" then provider_source_value = ot_upin;
		else if out=1 then provider_source_value = "out";
		else if hha=1 then provider_source_value = "hha";
		else if hsp=1 then provider_source_value = "hsp";

		if provider_source_value = "0000000000" then do;
			if at_upin ne "" then provider_source_value = at_upin;
			else if op_upin ne "" then provider_source_value = op_upin;
			else if ot_upin ne "" then provider_source_value = ot_upin;
		end;

		*Assign specialty source value to the file that the claim came from;
		if out=1 then specialty_source_value = "out";
		else if hha=1 then specialty_source_value = "hha";
		else if hsp=1 then specialty_source_value = "hsp";
		care_site_id = .;
		*Assign specialty concept id to unknown since files do not hold specialties;
		specialty_concept_id = 38004699;
	end;*/

/*run;

*Count how many records each provider has by specialty. Sort by freq and choose the most freq specialty for that provider;
proc sort data=provider_dups; by provider_source_value specialty_concept_id; run;
data provider_dups2;
	set provider_dups;
	by provider_source_value specialty_concept_id;
	retain specialty 0;
	if first.specialty_concept_id then specialty = 1;
	specialty + 1;
	if last.specialty_concept_id;
run;
proc sort data=provider_dups2 ; by provider_source_value specialty; run;

data provider (keep=provider_id npi dea specialty_concept_id care_site_id provider_source_value specialty_source_value patient_id provider);
	set provider_dups2;
	by provider_source_value specialty;
	if last.provider_source_value;
run;

data omop.provider (keep=provider_id npi dea specialty_concept_id care_site_id provider_source_value specialty_source_value);
	set provider;
run;*/
********************************************************************************************************************************************************************************************;
*End Test Code;
************************************************************************************************************************************************************************************************;

********************************************************************************************************************************************************************************************;
*Create payer plan table. Make two plan occurrences - one for part A & B eligibility and one for HMO use. Both entries in payer plan table.;

%macro createdate (mon);
	if mod(_n_,12) = 0 then do; monum=12; yrnum=int(_n_/12)-1; end;
      else do; monum=mod(_n_,12) ; yrnum=int(_n_/12); end;
%mend;

data partab (keep= payer_plan_period_start_date payer_plan_period_end_date payer_source_value plan_source_value family_source_value
				person_source_value);
	set ped.ped_all;

	person_source_value = patient_id;
	family_source_value = .;
	payer_source_value = .;

	length plan_source_value $45;

	retain payer_plan_period_id 0;

	array m{*} mon1-mon252;

	payer_plan_period_start_date= mdy(1,1,1991);

	do _n_=2 to dim(m);
		if m(_n_-1) ne m(_n_) or _n_=dim(m) then do;
			%createdate(_n_);
			if _n_=dim(m) then payer_plan_period_end_date = mdy(monum,31,yrnum+1991);
				else payer_plan_period_end_date = mdy(monum,1,yrnum+1991)-1;

			if m(_n_-1) = "0" then plan_source_value = "0 - Not Entitled";
			if m(_n_-1) = "1" then plan_source_value = "1 - Part A only";
			if m(_n_-1) = "2" then plan_source_value = "2 - Part B only";
			if m(_n_-1) = "3" then plan_source_value = "3 - Part A and B";

			output;
			payer_plan_period_start_date = mdy(monum,1,yrnum+1991);
		end;
	end;

	format payer_plan_period_end_date payer_plan_period_start_date yymmdd10.;
run;

data hmo (keep= payer_plan_period_start_date payer_plan_period_end_date payer_source_value plan_source_value family_source_value
				person_source_value);
	set ped.ped_all;

	person_source_value = patient_id;
	family_source_value = .;
	payer_source_value = .;

	length plan_source_value $68;

	retain payer_plan_period_id 0;

	array h{*} gho1-gho252;

	payer_plan_period_start_date= mdy(1,1,1991);

	do _n_=2 to dim(h);
		if h(_n_-1) ne h(_n_) or _n_=dim(h) then do;
			%createdate(_n_);
			if _n_=dim(h) then payer_plan_period_end_date = mdy(monum,31,yrnum+1991);
				else payer_plan_period_end_date = mdy(monum,1,yrnum+1991)-1;

			if h(_n_-1) = "0" then plan_source_value = "0 - Not a member of HMO";
			if h(_n_-1) = "1" then plan_source_value = "1 - Non-Lock-in, CMS process provider claims";
			if h(_n_-1) = "2" then plan_source_value = "2 - Non-Lock-in, GHO process in-plan A/B clms";
			if h(_n_-1) = "4" then plan_source_value = "4 - Chronic care disease mngmt org-FFS plan";
			if h(_n_-1) = "A" then plan_source_value = "A - Lock-in, CMS process provider claims";
			if h(_n_-1) = "B" then plan_source_value = "B - Lock-in, GHO process in-plan A/B clms";
			if h(_n_-1) = "C" then plan_source_value = "C - Lock-in, GHO process all Part A/B clms";

			output;
			payer_plan_period_start_date = mdy(monum,1,yrnum+1991);
		end;
	end;

	format payer_plan_period_end_date payer_plan_period_start_date yymmdd10.;
run;


proc sort data=partab; by person_source_value payer_plan_period_start_date payer_plan_period_end_date; run;
proc sort data=hmo; by person_source_value payer_plan_period_start_date payer_plan_period_end_date; run;
data payer_plan_period;
	set partab hmo;
	by person_source_value payer_plan_period_start_date payer_plan_period_end_date;
	payer_plan_period_id + 1;
run;

************************************************************************************************************************************************************************************************;
*Create Observation_period table. Defined as the time where patients have both Part A & B coverage and does not have HMO coverage;
*This is common observation periods used for research purposes in observational data;

data observation_period (keep= observation_period_id person_source_value observation_period_start_date observation_period_end_date o1-o252);
	set ped.ped_all;

	person_source_value = patient_id;
	retain observation_period_id 0;

	array m{*} mon1-mon252;
	array h{*} gho1-gho252;
	array o{252} o1-o252;

	do _n_=1 to dim(m);
		if m(_n_) = "3" and h(_n_) = "0" then o(_n_) = 1;
			else o(_n_) = 0;
	end;

	observation_period_start_date= mdy(1,1,1991);

	do _n_=2 to 252;
		if o(_n_-1) ne o(_n_) or _n_=dim(o) then do;
			%createdate(_n_);
			if _n_=dim(o) then observation_period_end_date = mdy(monum,31,yrnum+1991);
				else observation_period_end_date = mdy(monum,1,yrnum+1991)-1;

			if o(_n_-1) = 1 then do;
				observation_period_id + 1;
				output;
			end;

			observation_period_start_date = mdy(monum,1,yrnum+1991);
		end;
	end;

	format observation_period_start_date observation_period_end_date yymmdd10.;
run;

************************************************************************************************************************************************************************************************;
*Create drug exposure table. Import NDC's from DME file;
*NOTE: This table was not detailed in documentation file.  This is test code.  Commented out to prevent running code;

/*data drug_exposure (keep= drug_exposure_id person_source_value drug_concept_id drug_exposure_start_date drug_exposure_end_date drug_type_concept_id
						stop_reason refills quantity days_supply sig prescribing_provider_id visit_occurrence_id relevant_condition_concept_id
						drug_source_value);
		merge 	visit_occurrence(rename=(person_source_value=patient_id) in=visit )
				data.dme (keep=patient_id ndc_cd clm_from_dt clm_thru_dt provider ord_npi ord_upin in=dme rename=(clm_from_dt=visit_start_date clm_thru_dt=visit_end_date));

		by patient_id visit_start_date visit_end_date provider;

		if visit=1;
		person_source_value = patient_id;

		retain drug_exposure_id 0;

		if ndc_cd ne "" then do;
			drug_exposure_id + 1;
			drug_exposure_start_date = visit_start_date;
			drug_exposure_end_date = visit_end_date;
			drug_concept_id = 1; /*merge snomed id in sql statement below*/
			/*assign drug_type_concept_id = physician administered drug (id'd as procedure)*/
			/*drug_type_concept_id = 38000179;
			stop_reason = .;
			refills = .;
			quantity = .;
			days_supply = .;
			sig = .;
			if ord_npi ne "" then prescribing_provider_id = ord_npi;
				else prescribing_provider_id = ord_upin;
			relevant_condition_concept_id = .;
			drug_source_value = ndc_cd;
			output;
		end;
run;

proc sort data=drug_exposure nodupkey; by drug_exposure_id; run;*/
************************************************************************************************************************************************************************************************;
*End Test Code;
************************************************************************************************************************************************************************************************;



************************************************************************************************************************************************************************************************;
*Import OMOP table for hcpcs/icd9 mapping to concept_ids;


filename in "D:\Users\marc\Jigasaw\scm.csv";

 data OMOP.SOURCE_TO_CONCEPT_MAP_ICD9_HCPCS    ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile IN delimiter = ';' MISSOVER DSD lrecl=32767 firstobs=2 ;
        /* informat invalid_reason $255. ;
         informat mapping_type $3. ;
         informat PRIMARY_MAP $8. ;
         informat source_code $255. ;
         informat source_code_description best32. ;
         informat source_vocabulary_id best32. ;
         informat target_concept_id best32. ;
         informat target_vocabulary_id $12. ;*/
         informat valid_end_date YYMMDD10. ;
		 informat valid_start_date YYMMDD10. ;
         format invalid_reason $14. ;
         format mapping_type $50. ;
         format PRIMARY_MAP $8. ;
         format source_code $169. ;
         format source_code_description $255. ;
         format source_vocabulary_id best12. ;
         format target_concept_id best12. ;
         format target_vocabulary_id best12. ;
         format valid_end_date $12. ;
      input
                  invalid_reason $
                  mapping_type $
                  PRIMARY_MAP $
                  source_code $
                  source_code_description
                  source_vocabulary_id
                  target_concept_id
                  target_vocabulary_id $
                  valid_end_date $
				  valid_start_date $
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */


run;

*OMOP-ify conditions = use source_to_concept maps for icd9, hcpcs, etc codes for condition and procedure occurrence tables.;

proc sql;
	create table condition_occurrence2 as
	select co.*, case
					when c.target_concept_id = . then 0
					else c.target_concept_id
					end as condition_concept_id, c.valid_start_date
	from condition_occurrence(drop=condition_concept_id ) as co
	left join omop.source_to_concept_map_icd9_hcpcs(keep=source_code source_vocabulary_id PRIMARY_MAP target_concept_id valid_start_date valid_end_date target_vocabulary_id invalid_reason where=(invalid_reason = "" and PRIMARY_MAP='Y')) as c
	on co.condition_source_value = c.source_code and c.source_vocabulary_id = co.vocabulary_id and ( c.target_vocabulary_id = 1 or c.target_vocabulary_id = 0 )
	order by co.condition_occurrence_id, c.valid_start_date
	;
quit;


proc sql;
	create table procedure_occurrence2 as
	select co.*, case when c.target_concept_id = . then 0 else c.target_concept_id end as procedure_concept_id
	from procedure_occurrence(drop=procedure_concept_id ) as co
	left join omop.source_to_concept_map_icd9_hcpcs(keep=source_code source_vocabulary_id PRIMARY_MAP target_concept_id valid_start_date valid_end_date target_vocabulary_id invalid_reason where=(invalid_reason = "" and PRIMARY_MAP='Y')) as c
	on co.procedure_source_value = c.source_code and c.source_vocabulary_id = co.vocabulary_id and ( c.target_vocabulary_id = co.vocabulary_id or c.target_vocabulary_id = 0 )
	order by co.procedure_occurrence_id
	;

quit;

/*Commented out code - Test code used for Drug Exposure Table. Not in Documentation file.
proc sql;
	create table drug_exposure2 as
	select d.*, case when c.target_concept_id = . then 0 else c.target_concept_id end as drug_concept_id
	from drug_exposure (drop=drug_concept_id) as d
	left join vocab.jen (keep= source_code source_vocabulary_id primary_map target_concept_id target_vocabulary_id invalid_reason valid_start_date valid_end_date) as c
	on d.drug_source_value = c.source_code and c.source_vocabulary_id = 9 and ( c.target_vocabulary_id = 8 or c.target_vocabulary_id = 0 )
				and (input(trim(d.drug_exposure_start_date),YYMMDD10.) ge c.valid_start_date and input(trim(d.drug_exposure_end_date),YYMMDD10.) le c.valid_end_date)
	order by d.drug_exposure_id
	;
quit;*/


*Use person table to get person_id for all tables and Drop records that map to RXNORM;
proc sql;
	create table procedure_occurrence_visitids as
	select po.procedure_occurrence_id, p.person_id, po.procedure_concept_id, po.procedure_date, po.procedure_type_concept_id,
			po.associated_provider_id, po.visit_occurrence_id, po.relevant_condition_concept_id, po.procedure_source_value

	from procedure_occurrence2 as po
	inner join omop.person(keep=person_id person_source_value) as p
	on po.person_source_value = p.person_source_value;

quit;

proc sql;
	create table condition_occurrence_visitids as
	select co.condition_occurrence_id, p.person_id, co.condition_concept_id, co.condition_start_date, co.condition_end_date,
		   co.condition_type_concept_id, co.stop_reason, co.associated_provider_id, co.visit_occurrence_id, co.condition_source_value

	from condition_occurrence2 as co
	inner join omop.person(keep=person_id person_source_value) as p
	on co.person_source_value = p.person_source_value;
quit;

proc sql;
	create table omop.visit_occurrence as
	select vo.visit_occurrence_id, p.person_id, vo.visit_start_date, vo.visit_end_date, vo.place_of_service_concept_id, vo.care_site_id, vo.place_of_service_source_value
	from visit_occurrence as vo
	inner join omop.person(keep=person_id person_source_value) as p
	on vo.person_source_value = p.person_source_value;
quit;

proc sql;
	create table omop.observation as
	select ob.observation_id, p.person_id, ob.observation_concept_id, ob.observation_date, ob.observation_time, ob.value_as_number, ob.value_as_string, ob.value_as_concept_id,
			ob.unit_concept_id, ob.range_low, ob.range_high, ob.observation_type_concept_id, ob.associated_provider_id, ob.visit_occurrence_id, ob.relevant_condition_concept_id,
			ob.observation_source_value, ob.units_source_value
	from observation as ob
	inner join omop.person (keep=person_id person_source_value) as p
	on ob.person_source_value = p.person_source_value;
quit;

proc sql;
	create table omop.observation_period as
	select o.observation_period_id, p.person_id, o.observation_period_start_date, o.observation_period_end_date
	from observation_period as o
	inner join omop.person (keep=person_id person_source_value) as p
	on o.person_source_value = p.person_source_value;
quit;

proc sql;
	create table omop.payer_plan_period as
	select pp.payer_plan_period_id, p.person_id, pp.payer_plan_period_start_date, pp.payer_plan_period_end_date, pp.payer_source_value, pp.plan_source_value, pp.family_source_value
	from payer_plan_period as pp
	inner join omop.person (keep=person_id person_source_value) as p
	on pp.person_source_value = p.person_source_value;
quit;

/*Commented out code - Test code used for Drug Exposure Table. Not in Documentation file.
proc sql;
	create table drug_exposure_visitids as
	select d.drug_exposure_id, p.person_id, d.drug_concept_id, d.drug_exposure_start_date, d.drug_exposure_end_date, d.drug_type_concept_id, d.stop_reason, d.refills,
			d.quantity, d.days_supply, d.sig, d.prescribing_provider_id, d.visit_occurrence_id, d.relevant_condition_concept_id, d.drug_source_value
	from drug_exposure2 as d
	inner join omop.person (keep=person_id person_source_value) as p
	on d.person_source_value = p.person_source_value;
quit;*/

/*Commented out code - Test code used for Provider Table.  Not in Documentation File.
*Use provider table to get all provider ids in the procedure occurrence, condition occurrence, drug exposure tables;
proc sql;
	create table omop.procedure_occurrence as
	select po.procedure_occurrence_id, po.person_id, po.procedure_concept_id, po.procedure_date, po.procedure_type_concept_id,
			p.provider_id as associated_provider_id, po.visit_occurrence_id, po.relevant_condition_concept_id, po.procedure_source_value
	from procedure_occurrence_visitids as po
	left join provider (keep=provider_id provider_source_value) as p
	on po.associated_provider_id = p.provider_source_value;
quit;

proc sql;
	create table omop.condition_occurrence as
	select co.condition_occurrence_id, co.person_id, co.condition_concept_id, co.condition_start_date, co.condition_end_date,
		   co.condition_type_concept_id, co.stop_reason, p.provider_id as associated_provider_id, co.visit_occurrence_id, co.condition_source_value
	from condition_occurrence_visitids as co
	left join provider (keep=provider_id provider_source_value) as p
	on co.associated_provider_id = p.provider_source_value;
quit;

proc sql;
	create table omop.drug_exposure as
	select d.drug_exposure_id, d.person_id, d.drug_concept_id, d.drug_exposure_start_date, d.drug_exposure_end_date, d.drug_type_concept_id, d.stop_reason, d.refills,
			d.quantity, d.days_supply, d.sig, p.provider_id as prescribing_provider_id, d.visit_occurrence_id, d.relevant_condition_concept_id, d.drug_source_value
	from drug_exposure_visitids as d
	left join provider (keep=provider_id provider_source_value) as p
	on d.prescribing_provider_id = p.provider_source_value;
quit;*/



*Create CSV files from OMOP tables;
*added proc export code auto-generated from using sas wizard when exporting tables one by one in the explorer tab;
PROC EXPORT DATA= OMOP.Condition_occurrence
            OUTFILE= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\condition_occurrence.csv"
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;
PROC EXPORT DATA= OMOP.Death
            OUTFILE= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\death.csv"
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;
/*PROC EXPORT DATA= OMOP.Drug_exposure
            OUTFILE= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\drug_exposure.csv"
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;*/
PROC EXPORT DATA= OMOP.Observation
            OUTFILE= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\observation.csv"
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;
PROC EXPORT DATA= OMOP.Payer_plan_period
            OUTFILE= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\payer_plan_period.csv"
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;
PROC EXPORT DATA= OMOP.Person
            OUTFILE= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\person.csv"
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;
PROC EXPORT DATA= OMOP.Procedure_occurrence
            OUTFILE= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\procedure_occurrence.csv"
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;
/*PROC EXPORT DATA= OMOP.Provider
            OUTFILE= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\provider.csv"
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;*/
PROC EXPORT DATA= OMOP.Visit_occurrence
            OUTFILE= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\visit_occurrence.csv"
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;
proc export data= omop.observation_period
			outfile= "D:\Users\jen\Jigsaw\SEER Medicare ETL NCI\ALL\CSV Files\observation_period.csv"
			dbms=CSV replace;
		putnames=YES;
run;


