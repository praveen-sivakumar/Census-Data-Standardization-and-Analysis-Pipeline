# --------------------------------------------Import Statements-----------------------------------------------------
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from docx import Document
import matplotlib.pyplot as plt
import pymongo
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import Integer, VARCHAR

# ------------------------------------------Page Configuration Setup-----------------------------------------------
st.set_page_config(
    page_title="Census",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items={"About": "Application developed by Praveen Sivakumar"},
)

# -------------------------------------------Creating Navigation bar-----------------------------------------------
selected = option_menu(
    menu_title=None,
    options=["HOME", "CENSUS", "ANALYSIS", "ABOUT"],
    icons=["house-fill", "globe-central-south-asia", "graph-up", "info-circle"],
    default_index=0,
    orientation="horizontal",
    styles={
        "container": {"background-color": "#000000"},
        "icon": {"color": "white", "font-size": "25px"},
        "nav-link": {"text-align": "centre", "--hover-color": "red", "color": "white"},
        "nav-link-selected": {"background-color": "red"},
    },
)

# ------------------------------------------------Setting Title----------------------------------------------------
st.title(":red[Census Data Standardization and Analysis Pipeline]")

# ---------------------------------------------------HomePage------------------------------------------------------
if selected == "HOME":
    st.header("Home Page")

# ---------------------------------------------------Census--------------------------------------------------------
if selected == "CENSUS":

    # -----------------------------------------Read from Dataset------------------------------------------
    census_file_path = "D:\Praveen\Projects\Census\Dataset\census_2011.xlsx"
    df = pd.read_excel(census_file_path)

    telangana_districts_file_path = "D:\Praveen\Projects\Census\Dataset\Telangana.docx"
    doc = Document(telangana_districts_file_path)

    ladakh_districts = ["Leh(Ladakh)", "Kargil"]

    # ---------------------------------------------Raw Data-----------------------------------------------
    st.subheader(":red[_Raw data_] from the Dataset")
    st.dataframe(df)

    # --------------------------------------------After Renaming the Columns------------------------------
    rename_dict = {
        "District code" : "District_Code",
        "State name": "State/UT",
        "District name": "District",
        "Male_Literate": "Literate_Male",
        "Female_Literate": "Literate_Female",
        "Rural_Households": "Households_Rural",
        "Urban_Households": "Households_Urban",
        "Age_Group_0_29": "Young_and_Adult",
        "Age_Group_30_49": "Middle_Aged",
        "Age_Group_50": "Senior_Citizen",
        "Age not stated": "Age_Not_Stated",
        "Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car" : "Households_with_TV_Comp_Laptop_Tlph_mbl_and_Scooter_Car",
        "Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households" : "Typ_of_ltrn_fclty_Nyt_soil_disposed_in_open_drain_Hsehlds",
        "Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households" : "Typ_of_ltrn_fclty_Flsh_pour_flsh_ltrn_cnctd_to_othr_sys_Hsehlds",
        "Not_having_bathing_facility_within_the_premises_Total_Households" : "Nt_hvng_bthng_fclty_within_the_premises_Total_Households",
        "Not_having_latrine_facility_within_the_premises_Alternative_source_Open_Households" : "Nt_hvng_ltrn_fclty_within_the_premises_Altrntve_src_Open_Hsehlds",
        "Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households" : "Main_src_of_drnkng_water_Handpump_Tubewell_Borewell_Hsehlds",
        "Main_source_of_drinking_water_Other_sources_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households" : "MsrcDrnkwtr_Othr_Sprng_Rvr_Cnl_Tnk_Pnd_Lake_Othrsrc_HH",
        "Location_of_drinking_water_source_Near_the_premises_Households" : "Lctn_of_drinking_water_source_Near_the_premises_Hsehlds",
        "Location_of_drinking_water_source_Within_the_premises_Households" : "Lctn_of_drinking_water_source_Within_the_premises_Hsehlds"
    }

    renamed_df = df.rename(columns=rename_dict)

    st.subheader("Dataset after :red[_Renaming the columns_]")
    st.dataframe(renamed_df)

    # --------------------------------------------After Renaming the State/UT column's values------------------------------
    def custom_case_conversion(state):
        words = state.split()
        words = [word.lower() if word == "AND" else word.title() for word in words]
        return " ".join(words)

    renamed_df["State/UT"] = renamed_df["State/UT"].apply(custom_case_conversion)

    st.subheader("Dataset after :red[_Renaming the State/UT column's values_]")
    st.dataframe(renamed_df)

    # ------------------------------------------------New State/UT formation-------------------------------------------------
    telangana_districts = []
    for paragraph in doc.paragraphs:
        telangana_districts.append(paragraph.text)

    def change_state(district, telangana_districts, ladakh_districts, state):
        if district in telangana_districts:
            return "Telangana"
        elif district in ladakh_districts:
            return "Ladakh"
        else:
            return state

    renamed_df["State/UT"] = renamed_df.apply(
        lambda x: change_state(
            x["District"], telangana_districts, ladakh_districts, x["State/UT"]
        ),
        axis=1,
    )

    st.subheader("Dataset after :red[_New State/UT formation_]")
    st.dataframe(renamed_df)
    unProcessed_data_df = renamed_df.copy()
    if "unProcessed_data" not in st.session_state:
        st.session_state["unProcessed_data"] = unProcessed_data_df

    # ------------------------------------------------Processing the Missing data-------------------------------------------------
    def process_data(df_temp):

        # Filling Population
        df_temp.loc[df_temp['Population'].isnull(), 'Population'] = df_temp['Male'].fillna(0) + df_temp['Female'].fillna(0)
        df_temp.loc[df_temp['Male'].isnull(), 'Male'] = df_temp['Population'].fillna(0) - df_temp['Female'].fillna(0)
        df_temp.loc[df_temp['Female'].isnull(), 'Female'] = df_temp['Population'].fillna(0) - df_temp['Male'].fillna(0)

        # Filling Literacy
        df_temp.loc[df_temp['Literate'].isnull(), 'Literate'] = df_temp['Literate_Male'].fillna(0) + df_temp['Literate_Female'].fillna(0)
        df_temp.loc[df_temp['Literate_Male'].isnull(), 'Literate_Male'] = df_temp['Literate'].fillna(0) - df_temp['Literate_Female'].fillna(0)
        df_temp.loc[df_temp['Literate_Female'].isnull(), 'Literate_Female'] = df_temp['Literate'].fillna(0) - df_temp['Literate_Male'].fillna(0)

        # Filling SC
        df_temp.loc[df_temp['SC'].isnull(), 'SC'] = df_temp['Male_SC'].fillna(0) + df_temp['Female_SC'].fillna(0)
        df_temp.loc[df_temp['Male_SC'].isnull(), 'Male_SC'] = df_temp['SC'].fillna(0) - df_temp['Female_SC'].fillna(0)
        df_temp.loc[df_temp['Female_SC'].isnull(), 'Female_SC'] = df_temp['SC'].fillna(0) - df_temp['Male_SC'].fillna(0)

        # Filling ST
        df_temp.loc[df_temp['ST'].isnull(), 'ST'] = df_temp['Male_ST'].fillna(0) + df_temp['Female_ST'].fillna(0)
        df_temp.loc[df_temp['Male_ST'].isnull(), 'Male_ST'] = df_temp['ST'].fillna(0) - df_temp['Female_ST'].fillna(0)
        df_temp.loc[df_temp['Female_ST'].isnull(), 'Female_ST'] = df_temp['ST'].fillna(0) - df_temp['Male_ST'].fillna(0)

        # Filling Workers
        df_temp.loc[df_temp['Workers'].isnull(), 'Workers'] = df_temp['Male_Workers'].fillna(0) + df_temp['Female_Workers'].fillna(0)
        df_temp.loc[df_temp['Male_Workers'].isnull(), 'Male_Workers'] = df_temp['Workers'].fillna(0) - df_temp['Female_Workers'].fillna(0)
        df_temp.loc[df_temp['Female_Workers'].isnull(), 'Female_Workers'] = df_temp['Workers'].fillna(0) - df_temp['Male_Workers'].fillna(0)

        # Filling Workers Sub Divisions
        df_temp.loc[df_temp['Main_Workers'].isnull(), 'Main_Workers'] = df_temp['Population'].fillna(0) - (df_temp['Marginal_Workers'].fillna(0) + df_temp['Non_Workers'].fillna(0) + df_temp['Cultivator_Workers'].fillna(0) + df_temp['Agricultural_Workers'].fillna(0) + df_temp['Household_Workers'].fillna(0) + df_temp['Other_Workers'].fillna(0))
        df_temp.loc[df_temp['Marginal_Workers'].isnull(), 'Marginal_Workers'] = df_temp['Population'].fillna(0) - (df_temp['Main_Workers'].fillna(0) + df_temp['Non_Workers'].fillna(0) + df_temp['Cultivator_Workers'].fillna(0) + df_temp['Agricultural_Workers'].fillna(0) + df_temp['Household_Workers'].fillna(0) + df_temp['Other_Workers'].fillna(0))
        df_temp.loc[df_temp['Non_Workers'].isnull(), 'Non_Workers'] = df_temp['Population'].fillna(0) - (df_temp['Marginal_Workers'].fillna(0) + df_temp['Main_Workers'].fillna(0) + df_temp['Cultivator_Workers'].fillna(0) + df_temp['Agricultural_Workers'].fillna(0) + df_temp['Household_Workers'].fillna(0) + df_temp['Other_Workers'].fillna(0))
        df_temp.loc[df_temp['Cultivator_Workers'].isnull(), 'Cultivator_Workers'] = df_temp['Population'].fillna(0) - (df_temp['Marginal_Workers'].fillna(0) + df_temp['Non_Workers'].fillna(0) + df_temp['Main_Workers'].fillna(0) + df_temp['Agricultural_Workers'].fillna(0) + df_temp['Household_Workers'].fillna(0) + df_temp['Other_Workers'].fillna(0))
        df_temp.loc[df_temp['Agricultural_Workers'].isnull(), 'Agricultural_Workers'] = df_temp['Population'].fillna(0) - (df_temp['Marginal_Workers'].fillna(0) + df_temp['Non_Workers'].fillna(0) + df_temp['Cultivator_Workers'].fillna(0) + df_temp['Main_Workers'].fillna(0) + df_temp['Household_Workers'].fillna(0) + df_temp['Other_Workers'].fillna(0))
        df_temp.loc[df_temp['Household_Workers'].isnull(), 'Household_Workers'] = df_temp['Population'].fillna(0) - (df_temp['Marginal_Workers'].fillna(0) + df_temp['Non_Workers'].fillna(0) + df_temp['Cultivator_Workers'].fillna(0) + df_temp['Agricultural_Workers'].fillna(0) + df_temp['Main_Workers'].fillna(0) + df_temp['Other_Workers'].fillna(0))
        df_temp.loc[df_temp['Other_Workers'].isnull(), 'Other_Workers'] = df_temp['Population'].fillna(0) - (df_temp['Marginal_Workers'].fillna(0) + df_temp['Non_Workers'].fillna(0) + df_temp['Cultivator_Workers'].fillna(0) + df_temp['Agricultural_Workers'].fillna(0) + df_temp['Household_Workers'].fillna(0) + df_temp['Main_Workers'].fillna(0))

        # Filling Religion
        df_temp.loc[df_temp['Hindus'].isnull(), 'Hindus'] = df_temp['Population'].fillna(0) - (df_temp['Muslims'].fillna(0) + df_temp['Christians'].fillna(0) + df_temp['Sikhs'].fillna(0) + df_temp['Buddhists'].fillna(0) + df_temp['Jains'].fillna(0) + df_temp['Others_Religions'].fillna(0) + df_temp['Religion_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Muslims'].isnull(), 'Muslims'] = df_temp['Population'].fillna(0) - (df_temp['Hindus'].fillna(0) + df_temp['Christians'].fillna(0) + df_temp['Sikhs'].fillna(0) + df_temp['Buddhists'].fillna(0) + df_temp['Jains'].fillna(0) + df_temp['Others_Religions'].fillna(0) + df_temp['Religion_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Christians'].isnull(), 'Christians'] = df_temp['Population'].fillna(0) - (df_temp['Muslims'].fillna(0) + df_temp['Hindus'].fillna(0) + df_temp['Sikhs'].fillna(0) + df_temp['Buddhists'].fillna(0) + df_temp['Jains'].fillna(0) + df_temp['Others_Religions'].fillna(0) + df_temp['Religion_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Sikhs'].isnull(), 'Sikhs'] = df_temp['Population'].fillna(0) - (df_temp['Muslims'].fillna(0) + df_temp['Christians'].fillna(0) + df_temp['Hindus'].fillna(0) + df_temp['Buddhists'].fillna(0) + df_temp['Jains'].fillna(0) + df_temp['Others_Religions'].fillna(0) + df_temp['Religion_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Buddhists'].isnull(), 'Buddhists'] = df_temp['Population'].fillna(0) - (df_temp['Muslims'].fillna(0) + df_temp['Christians'].fillna(0) + df_temp['Sikhs'].fillna(0) + df_temp['Hindus'].fillna(0) + df_temp['Jains'].fillna(0) + df_temp['Others_Religions'].fillna(0) + df_temp['Religion_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Jains'].isnull(), 'Jains'] = df_temp['Population'].fillna(0) - (df_temp['Muslims'].fillna(0) + df_temp['Christians'].fillna(0) + df_temp['Sikhs'].fillna(0) + df_temp['Buddhists'].fillna(0) + df_temp['Hindus'].fillna(0) + df_temp['Others_Religions'].fillna(0) + df_temp['Religion_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Others_Religions'].isnull(), 'Others_Religions'] = df_temp['Population'].fillna(0) - (df_temp['Muslims'].fillna(0) + df_temp['Christians'].fillna(0) + df_temp['Sikhs'].fillna(0) + df_temp['Buddhists'].fillna(0) + df_temp['Jains'].fillna(0) + df_temp['Hindus'].fillna(0) + df_temp['Religion_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Religion_Not_Stated'].isnull(), 'Religion_Not_Stated'] = df_temp['Population'].fillna(0) - (df_temp['Muslims'].fillna(0) + df_temp['Christians'].fillna(0) + df_temp['Sikhs'].fillna(0) + df_temp['Buddhists'].fillna(0) + df_temp['Jains'].fillna(0) + df_temp['Others_Religions'].fillna(0) + df_temp['Hindus'].fillna(0))

        # Filling Households
        df_temp.loc[df_temp['Households'].isnull(), 'Households'] = df_temp['Households_Rural'].fillna(0) + df_temp['Households_Urban'].fillna(0)
        df_temp.loc[df_temp['Households_Rural'].isnull(), 'Households_Rural'] = df_temp['Households'].fillna(0) - df_temp['Households_Urban'].fillna(0)
        df_temp.loc[df_temp['Households_Urban'].isnull(), 'Households_Urban'] = df_temp['Households'].fillna(0) - df_temp['Households_Rural'].fillna(0)

        # Filling Education
        df_temp.loc[df_temp['Total_Education'].isnull(), 'Total_Education'] = df_temp['Literate_Education'].fillna(0) + df_temp['Illiterate_Education'].fillna(0)
        df_temp.loc[df_temp['Literate_Education'].isnull(), 'Literate_Education'] = df_temp['Total_Education'].fillna(0) - df_temp['Illiterate_Education'].fillna(0)
        df_temp.loc[df_temp['Illiterate_Education'].isnull(), 'Illiterate_Education'] = df_temp['Total_Education'].fillna(0) - df_temp['Literate_Education'].fillna(0)

        # Filling Education Sub Divisions
        df_temp.loc[df_temp['Below_Primary_Education'].isnull(), 'Below_Primary_Education'] = df_temp['Population'].fillna(0) - (df_temp['Primary_Education'].fillna(0) + df_temp['Middle_Education'].fillna(0) + df_temp['Secondary_Education'].fillna(0) + df_temp['Higher_Education'].fillna(0) + df_temp['Graduate_Education'].fillna(0) + df_temp['Other_Education'].fillna(0))
        df_temp.loc[df_temp['Primary_Education'].isnull(), 'Primary_Education'] = df_temp['Population'].fillna(0) - (df_temp['Below_Primary_Education'].fillna(0) + df_temp['Middle_Education'].fillna(0) + df_temp['Secondary_Education'].fillna(0) + df_temp['Higher_Education'].fillna(0) + df_temp['Graduate_Education'].fillna(0) + df_temp['Other_Education'].fillna(0))
        df_temp.loc[df_temp['Middle_Education'].isnull(), 'Middle_Education'] = df_temp['Population'].fillna(0) - (df_temp['Primary_Education'].fillna(0) + df_temp['Below_Primary_Education'].fillna(0) + df_temp['Secondary_Education'].fillna(0) + df_temp['Higher_Education'].fillna(0) + df_temp['Graduate_Education'].fillna(0) + df_temp['Other_Education'].fillna(0))
        df_temp.loc[df_temp['Secondary_Education'].isnull(), 'Secondary_Education'] = df_temp['Population'].fillna(0) - (df_temp['Primary_Education'].fillna(0) + df_temp['Middle_Education'].fillna(0) + df_temp['Below_Primary_Education'].fillna(0) + df_temp['Higher_Education'].fillna(0) + df_temp['Graduate_Education'].fillna(0) + df_temp['Other_Education'].fillna(0))
        df_temp.loc[df_temp['Higher_Education'].isnull(), 'Higher_Education'] = df_temp['Population'].fillna(0) - (df_temp['Primary_Education'].fillna(0) + df_temp['Middle_Education'].fillna(0) + df_temp['Secondary_Education'].fillna(0) + df_temp['Below_Primary_Education'].fillna(0) + df_temp['Graduate_Education'].fillna(0) + df_temp['Other_Education'].fillna(0))
        df_temp.loc[df_temp['Graduate_Education'].isnull(), 'Graduate_Education'] = df_temp['Population'].fillna(0) - (df_temp['Primary_Education'].fillna(0) + df_temp['Middle_Education'].fillna(0) + df_temp['Secondary_Education'].fillna(0) + df_temp['Higher_Education'].fillna(0) + df_temp['Below_Primary_Education'].fillna(0) + df_temp['Other_Education'].fillna(0))
        df_temp.loc[df_temp['Other_Education'].isnull(), 'Other_Education'] = df_temp['Population'].fillna(0) - (df_temp['Primary_Education'].fillna(0) + df_temp['Middle_Education'].fillna(0) + df_temp['Secondary_Education'].fillna(0) + df_temp['Higher_Education'].fillna(0) + df_temp['Graduate_Education'].fillna(0) + df_temp['Below_Primary_Education'].fillna(0))

        # Filling Age
        df_temp.loc[df_temp['Young_and_Adult'].isnull(), 'Young_and_Adult'] = df_temp['Population'].fillna(0) - (df_temp['Middle_Aged'].fillna(0) + df_temp['Senior_Citizen'].fillna(0) + df_temp['Age_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Middle_Aged'].isnull(), 'Middle_Aged'] = df_temp['Population'].fillna(0) - (df_temp['Young_and_Adult'].fillna(0) + df_temp['Senior_Citizen'].fillna(0) + df_temp['Age_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Senior_Citizen'].isnull(), 'Senior_Citizen'] = df_temp['Population'].fillna(0) - (df_temp['Middle_Aged'].fillna(0) + df_temp['Young_and_Adult'].fillna(0) + df_temp['Age_Not_Stated'].fillna(0))
        df_temp.loc[df_temp['Age_Not_Stated'].isnull(), 'Age_Not_Stated'] = df_temp['Population'].fillna(0) - (df_temp['Middle_Aged'].fillna(0) + df_temp['Senior_Citizen'].fillna(0) + df_temp['Young_and_Adult'].fillna(0))

        # Filling Power Parity
        df_temp.loc[df_temp['Total_Power_Parity'].isnull(), 'Total_Power_Parity'] = df_temp['Power_Parity_Less_than_Rs_45000'].fillna(0) + df_temp['Power_Parity_Rs_45000_90000'].fillna(0) + df_temp['Power_Parity_Rs_90000_150000'].fillna(0) + df_temp['Power_Parity_Rs_150000_240000'].fillna(0) + df_temp['Power_Parity_Rs_240000_330000'].fillna(0) + df_temp['Power_Parity_Rs_330000_425000'].fillna(0) + df_temp['Power_Parity_Rs_425000_545000'].fillna(0) + df_temp['Power_Parity_Above_Rs_545000'].fillna(0)
        df_temp.loc[df_temp['Power_Parity_Less_than_Rs_45000'].isnull(), 'Power_Parity_Less_than_Rs_45000'] = df_temp['Total_Power_Parity'].fillna(0) - (df_temp['Power_Parity_Rs_45000_90000'].fillna(0) + df_temp['Power_Parity_Rs_90000_150000'].fillna(0) + df_temp['Power_Parity_Rs_150000_240000'].fillna(0) + df_temp['Power_Parity_Rs_240000_330000'].fillna(0) + df_temp['Power_Parity_Rs_330000_425000'].fillna(0) + df_temp['Power_Parity_Rs_425000_545000'].fillna(0) + df_temp['Power_Parity_Above_Rs_545000'].fillna(0))
        df_temp.loc[df_temp['Power_Parity_Rs_45000_90000'].isnull(), 'Power_Parity_Rs_45000_90000'] = df_temp['Total_Power_Parity'].fillna(0) - (df_temp['Power_Parity_Less_than_Rs_45000'].fillna(0) + df_temp['Power_Parity_Rs_90000_150000'].fillna(0) + df_temp['Power_Parity_Rs_150000_240000'].fillna(0) + df_temp['Power_Parity_Rs_240000_330000'].fillna(0) + df_temp['Power_Parity_Rs_330000_425000'].fillna(0) + df_temp['Power_Parity_Rs_425000_545000'].fillna(0) + df_temp['Power_Parity_Above_Rs_545000'].fillna(0))
        df_temp.loc[df_temp['Power_Parity_Rs_90000_150000'].isnull(), 'Power_Parity_Rs_90000_150000'] = df_temp['Total_Power_Parity'].fillna(0) - (df_temp['Power_Parity_Rs_45000_90000'].fillna(0) + df_temp['Power_Parity_Less_than_Rs_45000'].fillna(0) + df_temp['Power_Parity_Rs_150000_240000'].fillna(0) + df_temp['Power_Parity_Rs_240000_330000'].fillna(0) + df_temp['Power_Parity_Rs_330000_425000'].fillna(0) + df_temp['Power_Parity_Rs_425000_545000'].fillna(0) + df_temp['Power_Parity_Above_Rs_545000'].fillna(0))
        df_temp.loc[df_temp['Power_Parity_Rs_150000_240000'].isnull(), 'Power_Parity_Rs_150000_240000'] = df_temp['Total_Power_Parity'].fillna(0) - (df_temp['Power_Parity_Rs_45000_90000'].fillna(0) + df_temp['Power_Parity_Rs_90000_150000'].fillna(0) + df_temp['Power_Parity_Less_than_Rs_45000'].fillna(0) + df_temp['Power_Parity_Rs_240000_330000'].fillna(0) + df_temp['Power_Parity_Rs_330000_425000'].fillna(0) + df_temp['Power_Parity_Rs_425000_545000'].fillna(0) + df_temp['Power_Parity_Above_Rs_545000'].fillna(0))
        df_temp.loc[df_temp['Power_Parity_Rs_240000_330000'].isnull(), 'Power_Parity_Rs_240000_330000'] = df_temp['Total_Power_Parity'].fillna(0) - (df_temp['Power_Parity_Rs_45000_90000'].fillna(0) + df_temp['Power_Parity_Rs_90000_150000'].fillna(0) + df_temp['Power_Parity_Rs_150000_240000'].fillna(0) + df_temp['Power_Parity_Less_than_Rs_45000'].fillna(0) + df_temp['Power_Parity_Rs_330000_425000'].fillna(0) + df_temp['Power_Parity_Rs_425000_545000'].fillna(0) + df_temp['Power_Parity_Above_Rs_545000'].fillna(0))
        df_temp.loc[df_temp['Power_Parity_Rs_330000_425000'].isnull(), 'Power_Parity_Rs_330000_425000'] = df_temp['Total_Power_Parity'].fillna(0) - (df_temp['Power_Parity_Rs_45000_90000'].fillna(0) + df_temp['Power_Parity_Rs_90000_150000'].fillna(0) + df_temp['Power_Parity_Rs_150000_240000'].fillna(0) + df_temp['Power_Parity_Rs_240000_330000'].fillna(0) + df_temp['Power_Parity_Less_than_Rs_45000'].fillna(0) + df_temp['Power_Parity_Rs_425000_545000'].fillna(0) + df_temp['Power_Parity_Above_Rs_545000'].fillna(0))
        df_temp.loc[df_temp['Power_Parity_Rs_425000_545000'].isnull(), 'Power_Parity_Rs_425000_545000'] = df_temp['Total_Power_Parity'].fillna(0) - (df_temp['Power_Parity_Rs_45000_90000'].fillna(0) + df_temp['Power_Parity_Rs_90000_150000'].fillna(0) + df_temp['Power_Parity_Rs_150000_240000'].fillna(0) + df_temp['Power_Parity_Rs_240000_330000'].fillna(0) + df_temp['Power_Parity_Rs_330000_425000'].fillna(0) + df_temp['Power_Parity_Less_than_Rs_45000'].fillna(0) + df_temp['Power_Parity_Above_Rs_545000'].fillna(0))
        df_temp.loc[df_temp['Power_Parity_Above_Rs_545000'].isnull(), 'Power_Parity_Above_Rs_545000'] = df_temp['Total_Power_Parity'].fillna(0) - (df_temp['Power_Parity_Rs_45000_90000'].fillna(0) + df_temp['Power_Parity_Rs_90000_150000'].fillna(0) + df_temp['Power_Parity_Rs_150000_240000'].fillna(0) + df_temp['Power_Parity_Rs_240000_330000'].fillna(0) + df_temp['Power_Parity_Rs_330000_425000'].fillna(0) + df_temp['Power_Parity_Rs_425000_545000'].fillna(0) + df_temp['Power_Parity_Less_than_Rs_45000'].fillna(0))
        df_temp.loc[df_temp['Power_Parity_Rs_45000_150000'].isnull(), 'Power_Parity_Rs_45000_150000'] = df_temp['Power_Parity_Less_than_Rs_45000'].fillna(0) + df_temp['Power_Parity_Rs_45000_90000'].fillna(0) + df_temp['Power_Parity_Rs_90000_150000'].fillna(0)
        df_temp.loc[df_temp['Power_Parity_Rs_150000_330000'].isnull(), 'Power_Parity_Rs_150000_330000'] = df_temp['Power_Parity_Rs_150000_240000'].fillna(0) + df_temp['Power_Parity_Rs_240000_330000'].fillna(0)
        df_temp.loc[df_temp['Power_Parity_Rs_330000_545000'].isnull(), 'Power_Parity_Rs_330000_545000'] = df_temp['Power_Parity_Rs_330000_425000'].fillna(0) + df_temp['Power_Parity_Rs_425000_545000'].fillna(0)


        return df_temp


    cleaned_df = process_data(renamed_df)
    processed_data_df = cleaned_df.fillna(df.mode().iloc[0])
    processed_data_df = processed_data_df.fillna(0)

    st.subheader("Dataset after :red[_Processing the Missing Data_]")
    st.dataframe(processed_data_df)

    if "processed_data" not in st.session_state:
        st.session_state["processed_data"] = processed_data_df

    #----------------------------------------------Migrate to MongoDB---------------------------------------------
    # # Connect to the MongoDB server
    # client = pymongo.MongoClient("mongodb+srv://praveensivakumar:root@cluster0.jxaowcs.mongodb.net/")
    # db = client['Census']
    # collection = db['Census_Data']

    # # Convert DataFrame to dictionary
    # data_dict = processed_data_df.to_dict(orient='records')
    # collection.insert_many(data_dict)

    # # Read from Mongodb
    # data_from_mongodb = list(collection.find())

    # # Convert data to DataFrame
    # df_mongodb = pd.DataFrame(data_from_mongodb)

    #Function call to convert the data types
    # def sqlcol(df_mongodb):    
    
    #     type_df = {}
    #     for i,j in zip(df_mongodb.columns, df_mongodb.dtypes):
    #         if "object" in str(j):
    #             type_df.update({i: VARCHAR(length=50)})

    #         if "int" in str(j):
    #             type_df.update({i: Integer})

    #     return type_df

    # op_dtype = sqlcol(df_mongodb) 

    # MySQL connection parameters
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = '3306'  # Default MySQL port
    database_name = 'census'

    # MySQL connection string
    connection_string = 'mysql+pymysql://root:root@localhost:3306/census'

    # Create engine
    engine = create_engine(connection_string)

    # tab = inspect(engine)
    # if tab.has_table("census") == False:
    #     df_mongodb.to_sql("census", con = engine, if_exists='replace', index =False,dtype = op_dtype) # pushing data from mongo dataframe to postgree table census , rows will be replaced if the table already exists
    #     with engine.connect() as conn:
    #         conn.execute(text('ALTER TABLE census ADD PRIMARY KEY(District_Code);'))

# ---------------------------------------------------Questions-----------------------------------------------------
if selected == "ANALYSIS":
    # --------------------------------------------Percentage of data missing before Data Processing----------------
    with st.expander(
        "Percentage of data missing for each column :red[_before Data Processing_]"
    ):
        if "unProcessed_data" not in st.session_state:
            st.write("No Data Available")
        else:
            unProcessed_data = st.session_state["unProcessed_data"]
            col1, col2 = st.columns(2)
            with col1:
                missing_data_df = pd.DataFrame(
                    {
                        "Missing_Count": unProcessed_data.isnull().sum(),
                        "Missing_Percentage": unProcessed_data.isnull().mean() * 100,
                    }
                )
                st.dataframe(missing_data_df)
            with col2:
                column_to_plot = st.selectbox(
                    "Select Un-Processed Column to Plot",
                    options=missing_data_df.columns,
                )
                st.bar_chart(missing_data_df[column_to_plot])

    # --------------------------------------------Percentage of data missing after Data Processing----------------
    with st.expander(
        "Percentage of data missing for each column :red[_after Data Processing_]"
    ):
        if "processed_data" not in st.session_state:
            st.write("No Data Available")
        else:
            processed_data = st.session_state["processed_data"]
            col1, col2 = st.columns(2)
            with col1:
                processed_data_df = pd.DataFrame(
                    {
                        "Missing_Count": processed_data.isnull().sum(),
                        "Missing_Percentage": processed_data.isnull().mean() * 100,
                    }
                )
                st.dataframe(processed_data_df)
            with col2:
                processed_Columns = st.selectbox(
                    "Select Processed Column to Plot", options=processed_data_df.columns
                )
                st.bar_chart(processed_data_df[column_to_plot])

# ---------------------------------------------------About---------------------------------------------------------
if selected == "ABOUT":
    st.header("About Page")
