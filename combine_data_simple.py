### Imports ###
from pathlib import Path

import pandas as pd


### Basic setup ###

data_dir = Path("data")
vdh_path = data_dir / "vdh-pud-overdose-deaths-by-year-and-geography.csv"
output_path = data_dir / "vdh_overdose_employment_population_full.csv"
#------------------------------


### Gather LAUC employment spreadsheets ###

#this finds every LAUC spreadsheet in the data folder
lauc_paths = sorted(data_dir.glob("laucnty*.xlsx"))
#this list will store the cleaned tables from each year
employment_tables = []

#this loop repeats the same cleaning steps for each spreadsheet
for path in lauc_paths:
    #read_excel pulls the sheet into a pandas table
    lauc = pd.read_excel(path, header=1)
    #this keeps only Virginia rows so other states do not slip in
    lauc = lauc[lauc["State FIPS Code"] == 51].copy()
    #astype and zfill make the FIPS codes look like 2+3 digit strings
    lauc["state_fips"] = lauc["State FIPS Code"].astype(int).astype(str).str.zfill(2)
    lauc["county_fips"] = lauc["County FIPS Code"].astype(int).astype(str).str.zfill(3)
    #this combines state and county into a 5 digit key
    lauc["fips"] = lauc["state_fips"] + lauc["county_fips"]
    #astype(int) gives us a plain year column for joining
    lauc["year"] = lauc["Year"].astype(int)
    #this strips the ", VA" ending so the names match other files
    lauc["locality_name"] = (
        lauc["County Name/State Abbreviation"].astype(str).str.replace(", VA", "", regex=False)
    )
    #this marks rows that mention city so we can track independents
    lauc["locality_type"] = lauc["locality_name"].str.contains("city", case=False, na=False)
    lauc["locality_type"] = lauc["locality_type"].map({True: "city", False: "county"})
    #this selects the columns we care about and renames them
    employment_tables.append(
        lauc[
            [
                "fips",
                "year",
                "locality_name",
                "locality_type",
                "Labor Force",
                "Employed",
                "Unemployed",
                "Unemployment Rate (%)",
            ]
        ].rename(
            columns={
                "Labor Force": "labor_force",
                "Employed": "employed",
                "Unemployed": "unemployed",
                "Unemployment Rate (%)": "unemployment_rate",
            }
        )
    )

#concat stacks all tables into one
employment = pd.concat(employment_tables, ignore_index=True)
#------------------------------


### Gather Weldon-Cooper population tables ###

#this list collects each population table before stacking
population_tables = []

#first I pulled the 2010-2020 file
pop_2010_path = data_dir / "VA-Intercensal-Estimates_2010-2020_UVA-CooperCenter_Updated-2023-01 (1).xlsx"
pop_2010_years = list(range(2010, 2021))
#read_excel pulls the sheet into a pandas table
raw_2010 = pd.read_excel(pop_2010_path, sheet_name="2010-2020 Estimates", skiprows=6)
#this trims away footers so we keep the year columns only
raw_2010 = raw_2010.iloc[:, : 3 + len(pop_2010_years)]
raw_2010.columns = ["county_fips", "locality_name", "census_value"] + pop_2010_years
#to_numeric lets us drop the footnotes with stars or words
raw_2010["county_fips"] = pd.to_numeric(raw_2010["county_fips"], errors="coerce")
raw_2010 = raw_2010[raw_2010["county_fips"].notna()].copy()
#astype and zfill make the three digit county codes
raw_2010["county_fips"] = raw_2010["county_fips"].astype(int).astype(str).str.zfill(3)
#this prefixes 51 because every row is Virginia
raw_2010["fips"] = "51" + raw_2010["county_fips"]
#melt turns wide year columns into tidy year rows
melt_2010 = raw_2010.melt(
    id_vars="fips", value_vars=pop_2010_years, var_name="year", value_name="population"
)
#to_numeric forces values to numbers
melt_2010["population"] = pd.to_numeric(melt_2010["population"], errors="coerce")
melt_2010["priority"] = 0
population_tables.append(melt_2010)

#later I added the 2020-2024 workbook so newer years are included
pop_2020_path = data_dir / "VA-Intercensal-Estimates_2020-2024_UVA-CooperCenter (1).xlsx"
pop_2020_years = list(range(2020, 2025))
raw_2020 = pd.read_excel(pop_2020_path, sheet_name="Table", skiprows=6)
raw_2020 = raw_2020.iloc[:, : 3 + len(pop_2020_years)]
raw_2020.columns = ["county_fips", "locality_name", "census_value"] + pop_2020_years
raw_2020["county_fips"] = pd.to_numeric(raw_2020["county_fips"], errors="coerce")
raw_2020 = raw_2020[raw_2020["county_fips"].notna()].copy()
raw_2020["county_fips"] = raw_2020["county_fips"].astype(int).astype(str).str.zfill(3)
raw_2020["fips"] = "51" + raw_2020["county_fips"]
melt_2020 = raw_2020.melt(
    id_vars="fips", value_vars=pop_2020_years, var_name="year", value_name="population"
)
melt_2020["population"] = pd.to_numeric(melt_2020["population"], errors="coerce")
melt_2020["priority"] = 1
population_tables.append(melt_2020)

#concat stacks all tables into one
population = pd.concat(population_tables, ignore_index=True)
#sort_values orders rows by the columns we give it
population.sort_values(["fips", "year", "priority"], inplace=True)
#drop_duplicates keeps the latest value for each locality/year combo
population = population.drop_duplicates(subset=["fips", "year"], keep="last")
population.drop(columns="priority", inplace=True)
#------------------------------


### Load VDH overdose CSV ###

#read_csv loads the health department file
vdh = pd.read_csv(vdh_path)
#this keeps only locality rows so we can match to counties and cities
vdh = vdh[vdh["Overdose Death Geography Level"] == "Locality"].copy()
#extract grabs the four digit year from text like 2024^
vdh["year"] = vdh["Overdose Death Year"].astype(str).str.extract(r"(\d{4})").astype(int)
#this drops 2025 rows because we do not have LAUC or population for that year yet
vdh = vdh[vdh["year"] <= 2024]
#str.zfill pads the code so every FIPS is five digits
vdh["fips"] = vdh["Overdose Death FIPS"].astype(str).str.replace(".0", "", regex=False).str.zfill(5)
#to_numeric turns the counts into numbers for analysis later
vdh["overdose_count"] = pd.to_numeric(vdh["Overdose Death Count"], errors="coerce")
vdh["overdose_rate"] = pd.to_numeric(
    vdh["Overdose Death Rate per 100,000 Residents"], errors="coerce"
)
#rename shortens the column names so they are easier to read
vdh = vdh.rename(
    columns={
        "Data Extract Date": "data_extract_date",
        "Overdose Death Drug Class": "drug_class",
        "Overdose Death Geography Name": "vdh_geography_name",
        "Overdose Death Health District": "health_district",
    }
)
#------------------------------


### Join all sources together ###

#merge joins tables by matching columns
merged = vdh.merge(employment, how="left", on=["fips", "year"])
merged = merged.merge(population, how="left", on=["fips", "year"])
#sort_values orders rows by the columns we give it
merged.sort_values(["drug_class", "year", "fips"], inplace=True)
#to_csv writes the final table to disk
merged.to_csv(output_path, index=False)
#------------------------------

missing_emp = merged["labor_force"].isna().sum()
missing_pop = merged["population"].isna().sum()

print(f"Saved merged dataset to {output_path}")
print(f"Rows missing employment data: {missing_emp}")
print(f"Rows missing population data: {missing_pop}")
