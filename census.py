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
import plotly.express as px

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
    st.header("Home")

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
        "District code": "District_Code",
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
        "Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car": "Households_with_TV_Comp_Laptop_Tlph_mbl_and_Scooter_Car",
        "Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households": "Typ_of_ltrn_fclty_Nyt_soil_disposed_in_open_drain_Hsehlds",
        "Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households": "Typ_of_ltrn_fclty_Flsh_pour_flsh_ltrn_cnctd_to_othr_sys_Hsehlds",
        "Not_having_bathing_facility_within_the_premises_Total_Households": "Nt_hvng_bthng_fclty_within_the_premises_Total_Households",
        "Not_having_latrine_facility_within_the_premises_Alternative_source_Open_Households": "Nt_hvng_ltrn_fclty_within_the_premises_Altrntve_src_Open_Hsehlds",
        "Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households": "Main_src_of_drnkng_water_Handpump_Tubewell_Borewell_Hsehlds",
        "Main_source_of_drinking_water_Other_sources_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households": "MsrcDrnkwtr_Othr_Sprng_Rvr_Cnl_Tnk_Pnd_Lake_Othrsrc_HH",
        "Location_of_drinking_water_source_Near_the_premises_Households": "Lctn_of_drinking_water_source_Near_the_premises_Hsehlds",
        "Location_of_drinking_water_source_Within_the_premises_Households": "Lctn_of_drinking_water_source_Within_the_premises_Hsehlds",
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
        df_temp.loc[df_temp["Population"].isnull(), "Population"] = df_temp[
            "Male"
        ].fillna(0) + df_temp["Female"].fillna(0)
        df_temp.loc[df_temp["Male"].isnull(), "Male"] = df_temp["Population"].fillna(
            0
        ) - df_temp["Female"].fillna(0)
        df_temp.loc[df_temp["Female"].isnull(), "Female"] = df_temp[
            "Population"
        ].fillna(0) - df_temp["Male"].fillna(0)

        # Filling Literacy
        df_temp.loc[df_temp["Literate"].isnull(), "Literate"] = df_temp[
            "Literate_Male"
        ].fillna(0) + df_temp["Literate_Female"].fillna(0)
        df_temp.loc[df_temp["Literate_Male"].isnull(), "Literate_Male"] = df_temp[
            "Literate"
        ].fillna(0) - df_temp["Literate_Female"].fillna(0)
        df_temp.loc[df_temp["Literate_Female"].isnull(), "Literate_Female"] = df_temp[
            "Literate"
        ].fillna(0) - df_temp["Literate_Male"].fillna(0)

        # Filling SC
        df_temp.loc[df_temp["SC"].isnull(), "SC"] = df_temp["Male_SC"].fillna(
            0
        ) + df_temp["Female_SC"].fillna(0)
        df_temp.loc[df_temp["Male_SC"].isnull(), "Male_SC"] = df_temp["SC"].fillna(
            0
        ) - df_temp["Female_SC"].fillna(0)
        df_temp.loc[df_temp["Female_SC"].isnull(), "Female_SC"] = df_temp["SC"].fillna(
            0
        ) - df_temp["Male_SC"].fillna(0)

        # Filling ST
        df_temp.loc[df_temp["ST"].isnull(), "ST"] = df_temp["Male_ST"].fillna(
            0
        ) + df_temp["Female_ST"].fillna(0)
        df_temp.loc[df_temp["Male_ST"].isnull(), "Male_ST"] = df_temp["ST"].fillna(
            0
        ) - df_temp["Female_ST"].fillna(0)
        df_temp.loc[df_temp["Female_ST"].isnull(), "Female_ST"] = df_temp["ST"].fillna(
            0
        ) - df_temp["Male_ST"].fillna(0)

        # Filling Workers
        df_temp.loc[df_temp["Workers"].isnull(), "Workers"] = df_temp[
            "Male_Workers"
        ].fillna(0) + df_temp["Female_Workers"].fillna(0)
        df_temp.loc[df_temp["Male_Workers"].isnull(), "Male_Workers"] = df_temp[
            "Workers"
        ].fillna(0) - df_temp["Female_Workers"].fillna(0)
        df_temp.loc[df_temp["Female_Workers"].isnull(), "Female_Workers"] = df_temp[
            "Workers"
        ].fillna(0) - df_temp["Male_Workers"].fillna(0)

        # Filling Workers Sub Divisions
        df_temp.loc[df_temp["Main_Workers"].isnull(), "Main_Workers"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Marginal_Workers"].fillna(0)
            + df_temp["Non_Workers"].fillna(0)
            + df_temp["Cultivator_Workers"].fillna(0)
            + df_temp["Agricultural_Workers"].fillna(0)
            + df_temp["Household_Workers"].fillna(0)
            + df_temp["Other_Workers"].fillna(0)
        )
        df_temp.loc[df_temp["Marginal_Workers"].isnull(), "Marginal_Workers"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Main_Workers"].fillna(0)
            + df_temp["Non_Workers"].fillna(0)
            + df_temp["Cultivator_Workers"].fillna(0)
            + df_temp["Agricultural_Workers"].fillna(0)
            + df_temp["Household_Workers"].fillna(0)
            + df_temp["Other_Workers"].fillna(0)
        )
        df_temp.loc[df_temp["Non_Workers"].isnull(), "Non_Workers"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Marginal_Workers"].fillna(0)
            + df_temp["Main_Workers"].fillna(0)
            + df_temp["Cultivator_Workers"].fillna(0)
            + df_temp["Agricultural_Workers"].fillna(0)
            + df_temp["Household_Workers"].fillna(0)
            + df_temp["Other_Workers"].fillna(0)
        )
        df_temp.loc[
            df_temp["Cultivator_Workers"].isnull(), "Cultivator_Workers"
        ] = df_temp["Population"].fillna(0) - (
            df_temp["Marginal_Workers"].fillna(0)
            + df_temp["Non_Workers"].fillna(0)
            + df_temp["Main_Workers"].fillna(0)
            + df_temp["Agricultural_Workers"].fillna(0)
            + df_temp["Household_Workers"].fillna(0)
            + df_temp["Other_Workers"].fillna(0)
        )
        df_temp.loc[
            df_temp["Agricultural_Workers"].isnull(), "Agricultural_Workers"
        ] = df_temp["Population"].fillna(0) - (
            df_temp["Marginal_Workers"].fillna(0)
            + df_temp["Non_Workers"].fillna(0)
            + df_temp["Cultivator_Workers"].fillna(0)
            + df_temp["Main_Workers"].fillna(0)
            + df_temp["Household_Workers"].fillna(0)
            + df_temp["Other_Workers"].fillna(0)
        )
        df_temp.loc[
            df_temp["Household_Workers"].isnull(), "Household_Workers"
        ] = df_temp["Population"].fillna(0) - (
            df_temp["Marginal_Workers"].fillna(0)
            + df_temp["Non_Workers"].fillna(0)
            + df_temp["Cultivator_Workers"].fillna(0)
            + df_temp["Agricultural_Workers"].fillna(0)
            + df_temp["Main_Workers"].fillna(0)
            + df_temp["Other_Workers"].fillna(0)
        )
        df_temp.loc[df_temp["Other_Workers"].isnull(), "Other_Workers"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Marginal_Workers"].fillna(0)
            + df_temp["Non_Workers"].fillna(0)
            + df_temp["Cultivator_Workers"].fillna(0)
            + df_temp["Agricultural_Workers"].fillna(0)
            + df_temp["Household_Workers"].fillna(0)
            + df_temp["Main_Workers"].fillna(0)
        )

        # Filling Religion
        df_temp.loc[df_temp["Hindus"].isnull(), "Hindus"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Muslims"].fillna(0)
            + df_temp["Christians"].fillna(0)
            + df_temp["Sikhs"].fillna(0)
            + df_temp["Buddhists"].fillna(0)
            + df_temp["Jains"].fillna(0)
            + df_temp["Others_Religions"].fillna(0)
            + df_temp["Religion_Not_Stated"].fillna(0)
        )
        df_temp.loc[df_temp["Muslims"].isnull(), "Muslims"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Hindus"].fillna(0)
            + df_temp["Christians"].fillna(0)
            + df_temp["Sikhs"].fillna(0)
            + df_temp["Buddhists"].fillna(0)
            + df_temp["Jains"].fillna(0)
            + df_temp["Others_Religions"].fillna(0)
            + df_temp["Religion_Not_Stated"].fillna(0)
        )
        df_temp.loc[df_temp["Christians"].isnull(), "Christians"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Muslims"].fillna(0)
            + df_temp["Hindus"].fillna(0)
            + df_temp["Sikhs"].fillna(0)
            + df_temp["Buddhists"].fillna(0)
            + df_temp["Jains"].fillna(0)
            + df_temp["Others_Religions"].fillna(0)
            + df_temp["Religion_Not_Stated"].fillna(0)
        )
        df_temp.loc[df_temp["Sikhs"].isnull(), "Sikhs"] = df_temp["Population"].fillna(
            0
        ) - (
            df_temp["Muslims"].fillna(0)
            + df_temp["Christians"].fillna(0)
            + df_temp["Hindus"].fillna(0)
            + df_temp["Buddhists"].fillna(0)
            + df_temp["Jains"].fillna(0)
            + df_temp["Others_Religions"].fillna(0)
            + df_temp["Religion_Not_Stated"].fillna(0)
        )
        df_temp.loc[df_temp["Buddhists"].isnull(), "Buddhists"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Muslims"].fillna(0)
            + df_temp["Christians"].fillna(0)
            + df_temp["Sikhs"].fillna(0)
            + df_temp["Hindus"].fillna(0)
            + df_temp["Jains"].fillna(0)
            + df_temp["Others_Religions"].fillna(0)
            + df_temp["Religion_Not_Stated"].fillna(0)
        )
        df_temp.loc[df_temp["Jains"].isnull(), "Jains"] = df_temp["Population"].fillna(
            0
        ) - (
            df_temp["Muslims"].fillna(0)
            + df_temp["Christians"].fillna(0)
            + df_temp["Sikhs"].fillna(0)
            + df_temp["Buddhists"].fillna(0)
            + df_temp["Hindus"].fillna(0)
            + df_temp["Others_Religions"].fillna(0)
            + df_temp["Religion_Not_Stated"].fillna(0)
        )
        df_temp.loc[df_temp["Others_Religions"].isnull(), "Others_Religions"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Muslims"].fillna(0)
            + df_temp["Christians"].fillna(0)
            + df_temp["Sikhs"].fillna(0)
            + df_temp["Buddhists"].fillna(0)
            + df_temp["Jains"].fillna(0)
            + df_temp["Hindus"].fillna(0)
            + df_temp["Religion_Not_Stated"].fillna(0)
        )
        df_temp.loc[
            df_temp["Religion_Not_Stated"].isnull(), "Religion_Not_Stated"
        ] = df_temp["Population"].fillna(0) - (
            df_temp["Muslims"].fillna(0)
            + df_temp["Christians"].fillna(0)
            + df_temp["Sikhs"].fillna(0)
            + df_temp["Buddhists"].fillna(0)
            + df_temp["Jains"].fillna(0)
            + df_temp["Others_Religions"].fillna(0)
            + df_temp["Hindus"].fillna(0)
        )

        # Filling Households
        df_temp.loc[df_temp["Households"].isnull(), "Households"] = df_temp[
            "Households_Rural"
        ].fillna(0) + df_temp["Households_Urban"].fillna(0)
        df_temp.loc[df_temp["Households_Rural"].isnull(), "Households_Rural"] = df_temp[
            "Households"
        ].fillna(0) - df_temp["Households_Urban"].fillna(0)
        df_temp.loc[df_temp["Households_Urban"].isnull(), "Households_Urban"] = df_temp[
            "Households"
        ].fillna(0) - df_temp["Households_Rural"].fillna(0)

        # Filling Education
        df_temp.loc[df_temp["Total_Education"].isnull(), "Total_Education"] = df_temp[
            "Literate_Education"
        ].fillna(0) + df_temp["Illiterate_Education"].fillna(0)
        df_temp.loc[df_temp["Literate_Education"].isnull(), "Literate_Education"] = (
            df_temp["Total_Education"].fillna(0)
            - df_temp["Illiterate_Education"].fillna(0)
        )
        df_temp.loc[
            df_temp["Illiterate_Education"].isnull(), "Illiterate_Education"
        ] = df_temp["Total_Education"].fillna(0) - df_temp["Literate_Education"].fillna(
            0
        )

        # Filling Education Sub Divisions
        df_temp.loc[
            df_temp["Below_Primary_Education"].isnull(), "Below_Primary_Education"
        ] = df_temp["Population"].fillna(0) - (
            df_temp["Primary_Education"].fillna(0)
            + df_temp["Middle_Education"].fillna(0)
            + df_temp["Secondary_Education"].fillna(0)
            + df_temp["Higher_Education"].fillna(0)
            + df_temp["Graduate_Education"].fillna(0)
            + df_temp["Other_Education"].fillna(0)
        )
        df_temp.loc[
            df_temp["Primary_Education"].isnull(), "Primary_Education"
        ] = df_temp["Population"].fillna(0) - (
            df_temp["Below_Primary_Education"].fillna(0)
            + df_temp["Middle_Education"].fillna(0)
            + df_temp["Secondary_Education"].fillna(0)
            + df_temp["Higher_Education"].fillna(0)
            + df_temp["Graduate_Education"].fillna(0)
            + df_temp["Other_Education"].fillna(0)
        )
        df_temp.loc[df_temp["Middle_Education"].isnull(), "Middle_Education"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Primary_Education"].fillna(0)
            + df_temp["Below_Primary_Education"].fillna(0)
            + df_temp["Secondary_Education"].fillna(0)
            + df_temp["Higher_Education"].fillna(0)
            + df_temp["Graduate_Education"].fillna(0)
            + df_temp["Other_Education"].fillna(0)
        )
        df_temp.loc[
            df_temp["Secondary_Education"].isnull(), "Secondary_Education"
        ] = df_temp["Population"].fillna(0) - (
            df_temp["Primary_Education"].fillna(0)
            + df_temp["Middle_Education"].fillna(0)
            + df_temp["Below_Primary_Education"].fillna(0)
            + df_temp["Higher_Education"].fillna(0)
            + df_temp["Graduate_Education"].fillna(0)
            + df_temp["Other_Education"].fillna(0)
        )
        df_temp.loc[df_temp["Higher_Education"].isnull(), "Higher_Education"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Primary_Education"].fillna(0)
            + df_temp["Middle_Education"].fillna(0)
            + df_temp["Secondary_Education"].fillna(0)
            + df_temp["Below_Primary_Education"].fillna(0)
            + df_temp["Graduate_Education"].fillna(0)
            + df_temp["Other_Education"].fillna(0)
        )
        df_temp.loc[
            df_temp["Graduate_Education"].isnull(), "Graduate_Education"
        ] = df_temp["Population"].fillna(0) - (
            df_temp["Primary_Education"].fillna(0)
            + df_temp["Middle_Education"].fillna(0)
            + df_temp["Secondary_Education"].fillna(0)
            + df_temp["Higher_Education"].fillna(0)
            + df_temp["Below_Primary_Education"].fillna(0)
            + df_temp["Other_Education"].fillna(0)
        )
        df_temp.loc[df_temp["Other_Education"].isnull(), "Other_Education"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Primary_Education"].fillna(0)
            + df_temp["Middle_Education"].fillna(0)
            + df_temp["Secondary_Education"].fillna(0)
            + df_temp["Higher_Education"].fillna(0)
            + df_temp["Graduate_Education"].fillna(0)
            + df_temp["Below_Primary_Education"].fillna(0)
        )

        # Filling Age
        df_temp.loc[df_temp["Young_and_Adult"].isnull(), "Young_and_Adult"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Middle_Aged"].fillna(0)
            + df_temp["Senior_Citizen"].fillna(0)
            + df_temp["Age_Not_Stated"].fillna(0)
        )
        df_temp.loc[df_temp["Middle_Aged"].isnull(), "Middle_Aged"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Young_and_Adult"].fillna(0)
            + df_temp["Senior_Citizen"].fillna(0)
            + df_temp["Age_Not_Stated"].fillna(0)
        )
        df_temp.loc[df_temp["Senior_Citizen"].isnull(), "Senior_Citizen"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Middle_Aged"].fillna(0)
            + df_temp["Young_and_Adult"].fillna(0)
            + df_temp["Age_Not_Stated"].fillna(0)
        )
        df_temp.loc[df_temp["Age_Not_Stated"].isnull(), "Age_Not_Stated"] = df_temp[
            "Population"
        ].fillna(0) - (
            df_temp["Middle_Aged"].fillna(0)
            + df_temp["Senior_Citizen"].fillna(0)
            + df_temp["Young_and_Adult"].fillna(0)
        )

        # Filling Power Parity
        df_temp.loc[df_temp["Total_Power_Parity"].isnull(), "Total_Power_Parity"] = (
            df_temp["Power_Parity_Less_than_Rs_45000"].fillna(0)
            + df_temp["Power_Parity_Rs_45000_90000"].fillna(0)
            + df_temp["Power_Parity_Rs_90000_150000"].fillna(0)
            + df_temp["Power_Parity_Rs_150000_240000"].fillna(0)
            + df_temp["Power_Parity_Rs_240000_330000"].fillna(0)
            + df_temp["Power_Parity_Rs_330000_425000"].fillna(0)
            + df_temp["Power_Parity_Rs_425000_545000"].fillna(0)
            + df_temp["Power_Parity_Above_Rs_545000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Less_than_Rs_45000"].isnull(),
            "Power_Parity_Less_than_Rs_45000",
        ] = df_temp["Total_Power_Parity"].fillna(0) - (
            df_temp["Power_Parity_Rs_45000_90000"].fillna(0)
            + df_temp["Power_Parity_Rs_90000_150000"].fillna(0)
            + df_temp["Power_Parity_Rs_150000_240000"].fillna(0)
            + df_temp["Power_Parity_Rs_240000_330000"].fillna(0)
            + df_temp["Power_Parity_Rs_330000_425000"].fillna(0)
            + df_temp["Power_Parity_Rs_425000_545000"].fillna(0)
            + df_temp["Power_Parity_Above_Rs_545000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Rs_45000_90000"].isnull(),
            "Power_Parity_Rs_45000_90000",
        ] = df_temp["Total_Power_Parity"].fillna(0) - (
            df_temp["Power_Parity_Less_than_Rs_45000"].fillna(0)
            + df_temp["Power_Parity_Rs_90000_150000"].fillna(0)
            + df_temp["Power_Parity_Rs_150000_240000"].fillna(0)
            + df_temp["Power_Parity_Rs_240000_330000"].fillna(0)
            + df_temp["Power_Parity_Rs_330000_425000"].fillna(0)
            + df_temp["Power_Parity_Rs_425000_545000"].fillna(0)
            + df_temp["Power_Parity_Above_Rs_545000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Rs_90000_150000"].isnull(),
            "Power_Parity_Rs_90000_150000",
        ] = df_temp["Total_Power_Parity"].fillna(0) - (
            df_temp["Power_Parity_Rs_45000_90000"].fillna(0)
            + df_temp["Power_Parity_Less_than_Rs_45000"].fillna(0)
            + df_temp["Power_Parity_Rs_150000_240000"].fillna(0)
            + df_temp["Power_Parity_Rs_240000_330000"].fillna(0)
            + df_temp["Power_Parity_Rs_330000_425000"].fillna(0)
            + df_temp["Power_Parity_Rs_425000_545000"].fillna(0)
            + df_temp["Power_Parity_Above_Rs_545000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Rs_150000_240000"].isnull(),
            "Power_Parity_Rs_150000_240000",
        ] = df_temp["Total_Power_Parity"].fillna(0) - (
            df_temp["Power_Parity_Rs_45000_90000"].fillna(0)
            + df_temp["Power_Parity_Rs_90000_150000"].fillna(0)
            + df_temp["Power_Parity_Less_than_Rs_45000"].fillna(0)
            + df_temp["Power_Parity_Rs_240000_330000"].fillna(0)
            + df_temp["Power_Parity_Rs_330000_425000"].fillna(0)
            + df_temp["Power_Parity_Rs_425000_545000"].fillna(0)
            + df_temp["Power_Parity_Above_Rs_545000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Rs_240000_330000"].isnull(),
            "Power_Parity_Rs_240000_330000",
        ] = df_temp["Total_Power_Parity"].fillna(0) - (
            df_temp["Power_Parity_Rs_45000_90000"].fillna(0)
            + df_temp["Power_Parity_Rs_90000_150000"].fillna(0)
            + df_temp["Power_Parity_Rs_150000_240000"].fillna(0)
            + df_temp["Power_Parity_Less_than_Rs_45000"].fillna(0)
            + df_temp["Power_Parity_Rs_330000_425000"].fillna(0)
            + df_temp["Power_Parity_Rs_425000_545000"].fillna(0)
            + df_temp["Power_Parity_Above_Rs_545000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Rs_330000_425000"].isnull(),
            "Power_Parity_Rs_330000_425000",
        ] = df_temp["Total_Power_Parity"].fillna(0) - (
            df_temp["Power_Parity_Rs_45000_90000"].fillna(0)
            + df_temp["Power_Parity_Rs_90000_150000"].fillna(0)
            + df_temp["Power_Parity_Rs_150000_240000"].fillna(0)
            + df_temp["Power_Parity_Rs_240000_330000"].fillna(0)
            + df_temp["Power_Parity_Less_than_Rs_45000"].fillna(0)
            + df_temp["Power_Parity_Rs_425000_545000"].fillna(0)
            + df_temp["Power_Parity_Above_Rs_545000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Rs_425000_545000"].isnull(),
            "Power_Parity_Rs_425000_545000",
        ] = df_temp["Total_Power_Parity"].fillna(0) - (
            df_temp["Power_Parity_Rs_45000_90000"].fillna(0)
            + df_temp["Power_Parity_Rs_90000_150000"].fillna(0)
            + df_temp["Power_Parity_Rs_150000_240000"].fillna(0)
            + df_temp["Power_Parity_Rs_240000_330000"].fillna(0)
            + df_temp["Power_Parity_Rs_330000_425000"].fillna(0)
            + df_temp["Power_Parity_Less_than_Rs_45000"].fillna(0)
            + df_temp["Power_Parity_Above_Rs_545000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Above_Rs_545000"].isnull(),
            "Power_Parity_Above_Rs_545000",
        ] = df_temp["Total_Power_Parity"].fillna(0) - (
            df_temp["Power_Parity_Rs_45000_90000"].fillna(0)
            + df_temp["Power_Parity_Rs_90000_150000"].fillna(0)
            + df_temp["Power_Parity_Rs_150000_240000"].fillna(0)
            + df_temp["Power_Parity_Rs_240000_330000"].fillna(0)
            + df_temp["Power_Parity_Rs_330000_425000"].fillna(0)
            + df_temp["Power_Parity_Rs_425000_545000"].fillna(0)
            + df_temp["Power_Parity_Less_than_Rs_45000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Rs_45000_150000"].isnull(),
            "Power_Parity_Rs_45000_150000",
        ] = (
            df_temp["Power_Parity_Less_than_Rs_45000"].fillna(0)
            + df_temp["Power_Parity_Rs_45000_90000"].fillna(0)
            + df_temp["Power_Parity_Rs_90000_150000"].fillna(0)
        )
        df_temp.loc[
            df_temp["Power_Parity_Rs_150000_330000"].isnull(),
            "Power_Parity_Rs_150000_330000",
        ] = df_temp["Power_Parity_Rs_150000_240000"].fillna(0) + df_temp[
            "Power_Parity_Rs_240000_330000"
        ].fillna(
            0
        )
        df_temp.loc[
            df_temp["Power_Parity_Rs_330000_545000"].isnull(),
            "Power_Parity_Rs_330000_545000",
        ] = df_temp["Power_Parity_Rs_330000_425000"].fillna(0) + df_temp[
            "Power_Parity_Rs_425000_545000"
        ].fillna(
            0
        )

        return df_temp

    cleaned_df = process_data(renamed_df)
    processed_data_df = cleaned_df.fillna(df.mode().iloc[0])
    processed_data_df = processed_data_df.fillna(0)

    st.subheader("Dataset after :red[_Processing the Missing Data_]")
    st.dataframe(processed_data_df)

    if "processed_data" not in st.session_state:
        st.session_state["processed_data"] = processed_data_df

    # ----------------------------------------------Migrate to MongoDB---------------------------------------------
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

    # Function call to convert the data types
    # def sqlcol(df_mongodb):

    #     type_df = {}
    #     for i,j in zip(df_mongodb.columns, df_mongodb.dtypes):
    #         if "object" in str(j):
    #             type_df.update({i: VARCHAR(length=50)})

    #         if "int" in str(j):
    #             type_df.update({i: Integer})

    #     return type_df

    # op_dtype = sqlcol(df_mongodb)

    # # MySQL connection parameters
    # user = "root"
    # password = "root"
    # host = "localhost"
    # port = "3306"  # Default MySQL port
    # database_name = "census"

    # # MySQL connection string
    # connection_string = "mysql+pymysql://root:root@localhost:3306/census"

    # # Create engine
    # engine = create_engine(connection_string)

    # tab = inspect(engine)
    # if tab.has_table("census") == False:
    #     df_mongodb.to_sql("census", con = engine, if_exists='replace', index =False,dtype = op_dtype)
    #     with engine.connect() as conn:
    #         conn.execute(text('ALTER TABLE census ADD PRIMARY KEY(District_Code);'))

# ---------------------------------------------------Analysis-----------------------------------------------------
if selected == "ANALYSIS":

    # Connect to the database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="census"
    )
    # Create a cursor object to execute queries
    mycursor = mydb.cursor()

    # Method to execute Query
    def execute_query(query):
        mycursor.execute(query)
        results = mycursor.fetchall()
        columns = [i[0] for i in mycursor.description]
        df = pd.DataFrame(results, columns=columns)
        return df

    # Method to plot Chart
    def plot(df,x,y,title):
        fig = px.bar(
                df,
                title=title,
                x=x,
                y=y,
                color=y,
                labels={y: y},
                color_continuous_scale=px.colors.sequential.Agsunset
            )
        st.plotly_chart(fig,use_container_width=True)
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

    #--------------------------------------------Total Population of each district--------------------------------
    with st.expander(
        ":red[_Total Population_] of each District"
    ):
        query = "select District, sum(Population) as Population from census group by District;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df, 'District', 'Population', 'Total Population of each District')

    with st.expander(":red[_Total literate males and females_] of each District"):
        query = "select District, sum(Literate_Male) as Literate_Male, sum(Literate_Female) as Literate_Female from census group by District;"
        result_df = execute_query(query)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df, 'District', 'Literate_Male', 'Total Literate Male of each District')
        with col3:
            plot(result_df, 'District', 'Literate_Female', 'Total Literate Female of each District')

    with st.expander(":red[_Percentage of workers_] in each District"):
        query = "select District,(Male_Workers/Workers)*100 as Male_Workers,(Female_Workers/Workers)*100 as Female_Workers from census;"
        result_df = execute_query(query)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df, 'District', 'Male_Workers', 'Percentage of Male Workers in each state')
        with col3:
            plot(result_df, 'District', 'Female_Workers', 'Percentage of Female Workers in each state')

    with st.expander(":red[_Household having access to LPG or PNG_] as cooking fuel in each District"):
        query = "select District, LPG_or_PNG_Households from census;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df,'District', 'LPG_or_PNG_Households', 'Household having access to LPG or PNG as co0king fuel in each District')

        
    with st.expander(":red[_Religious Composition_] of each District"):
        query = "select District,Hindus,Muslims,Christians,Sikhs,Buddhists,Jains,Others_Religions,Religion_Not_Stated from census;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_district = st.selectbox("Select an option:", result_df['District'])
            district_data = result_df[result_df['District'] == selected_district].set_index('District')
            district_data_transposed = district_data.T
            st.bar_chart(district_data_transposed, use_container_width=True)

    with st.expander(":red[_Households having Internet access_] in each District"):
        query = "select District, Households_with_Internet from census;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df, 'District', 'Households_with_Internet', 'Households having Internet access in each District')

    with st.expander(":red[_Educational attainment distribtion_] in each District"):
        query = "select District,Below_Primary_Education,Primary_Education,Middle_Education,Secondary_Education,Higher_Education,Graduate_Education,Other_Education from census;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_district = st.selectbox("Select a district:", result_df['District'])
            district_data = result_df[result_df['District'] == selected_district].set_index('District')
            district_data_transposed = district_data.T
            st.bar_chart(district_data_transposed, use_container_width=True)

    with st.expander("Housholds having :red[_accesss to mode of transportation_] in each District"):
        query = "select District, Households_with_Bicycle, Households_with_Car_Jeep_Van, Households_with_Scooter_Motorcycle_Moped from census;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_district = st.selectbox("Select district:", result_df['District'])
            district_data = result_df[result_df['District'] == selected_district].set_index('District')
            district_data_transposed = district_data.T
            st.bar_chart(district_data_transposed, use_container_width=True)

    with st.expander(":red[_Condition of occupied census houses_] in each District"):
        query = "select District, Condition_of_occupied_census_houses_Dilapidated_Households as Dilapidated, Households_with_separate_kitchen_Cooking_inside_house as Seperate_Kitchen, Having_bathing_facility_Total_Households as Bathing_Facility, Having_latrine_facility_within_the_premises_Total_Households as Laterine_within_premisis, Nt_hvng_ltrn_fclty_within_the_premises_Altrntve_src_Open_Hsehlds as Laterine_outside_premisis_Alternate_sources from census;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_district = st.selectbox("Select corresponding district:", result_df['District'])
            district_data = result_df[result_df['District'] == selected_district].set_index('District')
            district_data_transposed = district_data.T
            st.bar_chart(district_data_transposed, use_container_width=True)

    with st.expander(":red[_Household size distribution_] in each District"):
        query  = "select District, Household_size_1_person_Households as 1_Person, Household_size_2_persons_Households as 2_Persons,  Household_size_3_persons_Households as 3_Persons, Household_size_4_persons_Households as 4_Persons, Household_size_5_persons_Households as 5_Persons, Household_size_6_8_persons_Households as 6to8_Persons, Household_size_9_persons_and_above_Households as 9_and_more_Persons from census;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_district = st.selectbox("Select districts:", result_df['District'])
            district_data = result_df[result_df['District'] == selected_district].set_index('District')
            district_data_transposed = district_data.T
            st.bar_chart(district_data_transposed, use_container_width=True)

    with st.expander(":red[_Total number of households_] in each state"):
        query = "select `State/UT`, sum(Households)as Total_Households from census group by `State/UT`;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df, 'State/UT', 'Total_Households', 'Total number of households in each state')

    with st.expander("Household having :red[_laterine facility within permises-] of each state"):
        query = "select `State/UT`, sum(Having_latrine_facility_within_the_premises_Total_Households) as On_Premisis_Laterine from census group by `State/UT`"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df, 'State/UT', 'On_Premisis_Laterine', 'Household having laterine facility within permises of each state')

    with st.expander(":red[_Average household size_] in each state"):
        query = 'select `State/UT`, Avg(Household_size_1_person_Households) as "1-Person", Avg(Household_size_2_persons_Households) as "2-Persons", Avg(Household_size_3_persons_Households) as "3-Persons", Avg(Household_size_4_persons_Households) as "4-Persons", Avg(Household_size_5_persons_Households) as "5-Persons", Avg(Household_size_6_8_persons_Households) as "6to8-Persons", Avg(Household_size_9_persons_and_above_Households) as "9 and more Persons" from census group by `State/UT`;'
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_state = st.selectbox("Select a state:", result_df['State/UT'])
            state_data = result_df[result_df['State/UT'] == selected_state].set_index('State/UT')
            state_data_transposed = state_data.T
            st.bar_chart(state_data_transposed, use_container_width=True)

    with st.expander(":red[_Households owned vs rented_] in each state"):
        query = 'select `State/UT`, sum(Ownership_Owned_Households) as "Owned",sum(Ownership_Rented_Households) as "Rented" from census group by `State/UT`;'
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_state = st.selectbox("Select state:", result_df['State/UT'])
            state_data = result_df[result_df['State/UT'] == selected_state].set_index('State/UT')
            state_data_transposed = state_data.T
            st.bar_chart(state_data_transposed, use_container_width=True)

    with st.expander("Distribution of :red[_different types of laterine facilities_] in each state"):
        query = 'select `State/UT`, sum(Type_of_latrine_facility_Pit_latrine_Households) as "Pit Laterine",sum(type_of_latrine_facility_Other_latrine_Households) as "Other Laterine",sum(Typ_of_ltrn_fclty_Nyt_soil_disposed_in_open_drain_Hsehlds) as "Night Soil",sum(Typ_of_ltrn_fclty_Flsh_pour_flsh_ltrn_cnctd_to_othr_sys_Hsehlds) as "Flush" from census group by `State/UT`;'
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_state = st.selectbox("Select the state:", result_df['State/UT'])
            state_data = result_df[result_df['State/UT'] == selected_state].set_index('State/UT')
            state_data_transposed = state_data.T
            st.bar_chart(state_data_transposed, use_container_width=True)

    with st.expander("Households having access to :red[_drinking water sources within the premises_] of each state"):
        query = 'select `State/UT`, sum(Lctn_of_drinking_water_source_Within_the_premises_Hsehlds) as "Households" from census group by `State/UT`;'
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df, 'State/UT', 'Households', 'Households having drinking water sources within the premises in each State')

    with st.expander(":red[_Average household income distribution_] in each state based on the power parity categories"):
        query = 'select `State/UT`, avg(Power_Parity_Less_than_Rs_45000) as "income < 45000", avg(Power_Parity_Rs_45000_150000) as "income between 45000 to 150000", avg(Power_Parity_Rs_150000_330000) as "income between 150000 to 330000", avg(Power_Parity_Rs_330000_545000) as "income between 330000 to 545000", avg(Power_Parity_Above_Rs_545000) as "income > 545000" from census group by `State/UT`;'
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_state = st.selectbox("Select corresponding state:", result_df['State/UT'])
            state_data = result_df[result_df['State/UT'] == selected_state].set_index('State/UT')
            state_data_transposed = state_data.T
            st.bar_chart(state_data_transposed, use_container_width=True)
    
    with st.expander("Percentage of :red[_married couples with different household sizes_] in each state"):
        query = 'select `State/UT`, (sum(Married_couples_1_Households)/sum(Married_couples_None_Households +Married_couples_1_Households+Married_couples_2_Households+Married_couples_3_or_more_Households))*100 as "1"  ,(sum(Married_couples_2_Households)/sum(Married_couples_None_Households +Married_couples_1_Households+Married_couples_2_Households+Married_couples_3_or_more_Households))*100 as"2" ,(sum(Married_couples_3_or_more_Households)/sum(Married_couples_None_Households+Married_couples_1_Households+Married_couples_2_Households+Married_couples_3_or_more_Households))*100 as "3" from census group by `State/UT`;'
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            selected_state = st.selectbox("Select states:", result_df['State/UT'])
            state_data = result_df[result_df['State/UT'] == selected_state].set_index('State/UT')
            state_data_transposed = state_data.T
            st.bar_chart(state_data_transposed, use_container_width=True)

    with st.expander(":red[_Households falling below the poverty line_] in each state based on the power parity categories"):
        query = 'select `State/UT`,sum(Power_Parity_Less_than_Rs_45000) as "Households"  from census group by `State/UT`;'
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df, 'State/UT', 'Households', 'Households falling below the poverty line')

    with st.expander(":red[_Overall literacy rate_] in each state"):
        query = "select `State/UT`, (sum(Literate_Education)/sum(Total_Education))*100 as Literacy_Rate from census group by `State/UT`;"
        result_df = execute_query(query)
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(result_df)
        with col2:
            plot(result_df, 'State/UT', 'Literacy_Rate', 'Literacy rate in each state')
# ---------------------------------------------------About---------------------------------------------------------
if selected == "ABOUT":
    st.header("About")
    st.markdown(
        """Census Data Standardization and Analysis Pipeline is a user friendly Streamlit application that extracts the data from the provided dataset for Census, process the data and stores it in a MongoDb database. Then it migrates the data from MongoDb to a RDBMS like MySQl. It also enables the users to analysis the data, provides insights on the data through the grahps, charts and data frames."""
    )

    st.markdown("**:red[Technologies]** : Python, SQL , MongoDB, Streamlit")

    st.markdown("**:red[Domain]** : Ministry of Home Affairs")

    st.markdown(
        "**:red[Github Link]** : https://github.com/praveen-sivakumar/Census-Data-Standardization-and-Analysis-Pipeline"
    )

    st.subheader("Project done by **:red[_Praveen Sivakumar_]**")
