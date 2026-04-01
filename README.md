# GI-Bill-Decision-Tool Project still in work
A self-started portfolio project investigating where the best value exists for utilizing the GI bill.
This project was born during my transition out of the military.  When thinking about utilizing the GI bill benefits, one decision point is the cost of living in the area.  I set out to determine where the cost of living is most mismatched to the housing allowance received by the participating service member.    

* BAH research:  How is BAH calculated?  BAH is Basic Allowance for Housing.  A veteran utilizing their GI bill benefit will receive BAH at the E-5, with dependents rate.  This rate is determined based on the zip code where the veteran is attending school.  I reviewed the below resources to understand how BAH is calculated
    * https://www.travel.dod.mil/Allowances/Basic-Allowance-for-Housing/BAH-Data-Collection/
    * https://militarypay.defense.gov/pay/allowances/bah.aspx 
    * BAH primer available at https://www.travel.dod.mil/Allowances/Basic-Allowance-for-Housing/ 

* Yellow Ribbon Schools: To create a starting point for analysis, the VA website has a search tool in order to locate participating universities.  A Yellow Ribbon school is one which partners with
the VA to cover tuition expenses that exceed maximum post-911 GI bill rate.  This often covers the full cost for veterans at private or out-of-state schools.  This also represents a potential flaw
as the tool may miss some in-state or public institutions.  Users should recognize that this tool is oriented to utilization of the post-911, not Montgomery GI-bill.  In this section, I will
discuss overall project methodology.  Later, I will cover how I approached ethical and responsible data collection for the various sources within the project.  At this stage, I produced a CSV with
all Yellow Ribbon Schools and associated information.

* Lat-long to Zip Code: The VA site did not house zip codes, so an intermediate step to produce the zip codes from lat-long was required to proceed to determining the BAH rate for a school (location).  Downloaded US Census ZCTA files hosted at https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html.  Force 5 digit strings for zip codes with leading zeroes.

* Add BAH: Defense Travel Management Office (DTMO) hosts a tool for Basic Allowance for Housing Rate Lookup at https://www.travel.dod.mil/Allowances/Basic-Allowance-for-Housing/BAH-Rate-Lookup/.  Additionally, they offer download of an ASCII file containing BAH rates for all locations and all pay grades for the specified year.  At this step, I built a script to read the CSV with yellow ribbon schools, force zip codes to a 5-digit string (escape leading zero issues), map zip to MHA (military housing allowance), add a column for BAH for E-5 with dependents, and write an updated CSV.

*Zip codes to FIPS: HUD User Office of Policy Development and Research (PD&R) data uses Census Bureau geographies in datasets, so an intermediate step was required.  Used zip to county crosswalk provided https://www.huduser.gov/apps/public/uspscrosswalk/home 
Design decision made at this step since one zip can map to multiple counties, elected to use the row with highest TOT_RATIO assuming this is the highest share of addresses associated with this county (dominant county for that zip).

* Add Fair Market Rents:  Explored Living Wage Calculator hosted at https://livingwage.mit.edu/.  This site expressly prohibits scraping the data for more than 10 sites.  This led me to look to the source data for housing specifically after reading the documentation at https://livingwage.mit.edu/resources/living_wage_technical_documentation.pdf, accessed on 20 March, 2026.
HUD’s Office of Policy Development and Research (PD&R) 
This product uses the HUD User Data API but is not endorsed or certified by HUD User.
Limitations may include: FMR values are estimates provided by HUD and may not reflect real-time market conditions, missing ZIP codes or geolocation data may result in incomplete records, derived cost calculations are estimates and not official HUD outputs  
