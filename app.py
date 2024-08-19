import streamlit as st
import pandas as pd
import os
import pydeck as pdk
import geopandas as gpd
from streamlit_dynamic_filters import DynamicFilters

def archaic():

    layers = [
        pdk.Layer(
            "GeoJsonLayer",
            data=data_gpd,
            get_fill_color=[0, 0, 0],
        ),
        pdk.Layer(
            "TextLayer",
            data=data_gpd,
            get_position="geometry.coordinates",
            get_size=16,
            get_color=[255, 255, 255],
            get_text="title",
        ),
    ]

    layers = [
        pdk.Layer(
            "ScatterplotLayer",
            data = data_by_coord_df,
            pickable=True,
            opacity=0.8,
            stroked=True,
            filled=True,
            radius_min_pixels=5,
            radius_max_pixels=100,
            line_width_min_pixels=1,
            get_position="[lon, lat]",
            get_radius=100,
            get_fill_color=[255, 140, 0],
            get_line_color=[0, 0, 0],
        )
    ]


    st.pydeck_chart(pdk.Deck(
        layers=[layers],
        tooltip={"html": "<b>{geocoded_address}:</b><br>{titles}"},
        map_style='light',
        map_provider = 'mapbox'
    ))

def main():
    style = "<style> body * {text-align: center;}</style>"
    st.markdown(style, unsafe_allow_html=True)

    st.markdown("<h2> Traveling By Book </h2>",unsafe_allow_html=True)

     
    multi = '''

    <p>
    Whether you're visiting somewhere for the first time, the hundredth time, or virtually, there's no better guide than a book.
    Find notable books set in cities or countries you would like to explore.

    Happy reading! :)
    </p>
    '''

    st.markdown(multi, unsafe_allow_html=True)
    
    data_file = 'data/csv/book_db.csv'
    data_df = pd.read_csv(data_file)
    data_df = data_df.dropna(subset=['lat','lon'])[['title','author','lat','lon','address','geocoded_address','geocoded_country']]
    data_gpd = gpd.GeoDataFrame(data_df, geometry=gpd.points_from_xy(data_df.lon, data_df.lat), crs="EPSG:4326")

    # unnecessary it turns out; we'll do live filtering and updating right in the app
    data_by_coord_file = 'data/csv/books_per_coord_db.csv'
    data_by_coord_df = pd.read_csv(data_by_coord_file)
    data_by_coord_df = data_by_coord_df.dropna(subset=['lat','lon'])[['titles','lat','lon','geocoded_address']]
    data_by_coord_gpd = gpd.GeoDataFrame(data_by_coord_df, geometry=gpd.points_from_xy(data_by_coord_df.lon, data_by_coord_df.lat), crs="EPSG:4326")


    # final quality checks

    data_df['title'] = data_df['title'].astype(str) # coerce titles to str
    data_df['author'] = data_df['author'].astype(str) # coerce titles to str
    data_df['address'] = data_df['address'].astype(str) # coerce titles to str
    data_df['geocoded_address'] = data_df['geocoded_address'].astype(str) # coerce titles to str
    data_df['geocoded_country'] = data_df['geocoded_country'].astype(str) # coerce titles to str

    data_df = data_df.rename(columns={'geocoded_address':'Location','geocoded_country':'Country','title':'Title','author':'Author','lat':'Lat','lon':'Lon'})
    data_df = data_df.loc[~(data_df.Location=='nan')] # needed because a couple lat lons are for "asia" and "europe" they have no country so should be excluded
    # have not been excluded because i originally allow datafile based on lat lon not by country presence (line after pd.read above)
    # might want to change this to be upstream of making the datafile especially as its ugly to drop by str nan lol

    dynamic_filters = DynamicFilters(data_df[['Title','Author','Location','Country','Lat','Lon']], filters=['Country','Location', 'Title','Author'])

    with st.sidebar:
        st.write("Find Book By:")
    dynamic_filters.display_filters(location='sidebar')

    
    filtered_df = dynamic_filters.filter_df()
    filtered_data_by_coord_df = group_by_address(filtered_df)
    layers = [
        pdk.Layer(
            "ScatterplotLayer",
            data = filtered_data_by_coord_df,
            pickable=True,
            opacity=0.8,
            stroked=True,
            filled=True,
            radius_min_pixels=5,
            radius_max_pixels=100,
            line_width_min_pixels=1,
            get_position="[Lon, Lat]",
            get_radius=100,
            get_fill_color=[204,85,0],
            get_line_color=[0, 0, 0],
        )
    ]


    st.pydeck_chart(pdk.Deck(
        layers=[layers],
        tooltip={"html": "<b>{Location}:</b><br>{Titles}"},
        map_style='dark',
        map_provider = 'mapbox'
    ))

    dynamic_filters.display_df()

def group_by_address(book_df):
    book_df_group = book_df.groupby(['Location','Lat','Lon'])['Title'].apply(list).reset_index()
    book_df_group['Title_str'] =book_df_group.apply(lambda x: [str(t) for t in x.Title], axis=1) # sometimes titles are not strings
    book_df_group['Titles'] = book_df_group.apply(lambda x:"<br>".join(x.Title_str), axis=1)
    book_df_group = book_df_group[['Location','Lat','Lon','Titles']]
    book_df_group = book_df_group.loc[~(book_df_group.Location=='')] # drops instances where no legitimate address found
    
    return book_df_group
if __name__ == "__main__":
    main()