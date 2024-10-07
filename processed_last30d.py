import PyUber
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import plotly.subplots as sp

sql_last24 = """
SELECT 
          leh.lot AS ent_lot
         ,leh.operation AS ent_operation
         ,leh.entity AS entity
         ,leh.processed_wafer_count AS processed_wafer_count
         ,leh.lot_abort_flag AS lot_abort_flag
         ,leh.reticle AS reticle
         ,leh.lot_entity_process_duration AS lot_entity_process_duration
         ,leh.lot_process AS ent_lot_process
         ,Replace(Replace(Replace(Replace(Replace(Replace(p.product_description,',',';'),chr(9),' '),chr(10),' '),chr(13),' '),chr(34),''''),chr(7),' ') AS product_description
         ,lrc.product AS product
         ,lrc.dotprocess AS dotprocess
         ,lrc.route AS route
         ,lrc.lot_type AS lot_type
         ,lrc.oper_short_desc AS oper_short_desc
         ,l.hotlot AS current_hotlot
         ,lwr.recipe AS lot_recipe
         ,To_Char(leh.last_wafer_end_time,'yyyy-mm-dd hh24:mi:ss') AS last_wafer_end_date
         ,To_Char(leh.first_wafer_end_time,'yyyy-mm-dd hh24:mi:ss') AS first_wafer_end_date
         ,leh.lot_priority AS ent_lot_priority
FROM 
F_LotEntityHist leh
INNER JOIN F_Lot_Wafer_Recipe lwr ON lwr.recipe_id=leh.lot_recipe_id
INNER JOIN F_Lot_Run_card lrc ON lrc.lotoperkey = leh.lotoperkey
INNER JOIN F_Product p ON p.product=lrc.product AND p.facility = lrc.facility AND p.latest_version = 'Y'
INNER JOIN F_LOT l ON l.lot = lrc.lot
WHERE
     (leh.entity LIKE 'SDJ591' or leh.entity LIKE 'TZH591' or leh.entity LIKE 'SCJ591' or leh.entity LIKE 'SBH202' 
     or leh.entity LIKE 'SLM209' or leh.entity LIKE 'TCX101' or leh.entity LIKE 'TNI110' or leh.entity LIKE 'SDJ111' 
     or leh.entity LIKE 'STA118' or leh.entity LIKE 'STA215' or leh.entity LIKE 'STA216' or leh.entity LIKE 'STG111' 
     or leh.entity LIKE 'STG112' or leh.entity LIKE 'STG113' or leh.entity LIKE 'STG114' or leh.entity LIKE 'CIX402' 
     or leh.entity LIKE 'UTX410')
 AND      leh.last_wafer_end_time >= SYSDATE - 30
"""

try:
    conn = PyUber.connect(datasource='F21_PROD_XEUS')
    df_last24 = pd.read_sql(sql_last24, conn)
except:
    print('Cannot run SQL script - Consider connecting to VPN')

#df_last24 = pd.read_csv('synthetic_runrate.csv')
# Ensure 'LAST_WAFER_END_DATE' and 'FIRST_WAFER_END_DATE' are in datetime format
df_last24['LAST_WAFER_END_DATE'] = pd.to_datetime(df_last24['LAST_WAFER_END_DATE'])
df_last24['FIRST_WAFER_END_DATE'] = pd.to_datetime(df_last24['FIRST_WAFER_END_DATE'])
min_date = df_last24['FIRST_WAFER_END_DATE'].min()
max_date = df_last24['LAST_WAFER_END_DATE'].max()
# Create 'WPH' column
df_last24['WPH'] = df_last24['PROCESSED_WAFER_COUNT'] / (df_last24['LAST_WAFER_END_DATE'] - df_last24['FIRST_WAFER_END_DATE']).dt.total_seconds()*3600
# Replace inf with NaN
df_last24['WPH'] = df_last24['WPH'].replace([np.inf, -np.inf], np.nan)
df_last24['WPH'] = df_last24['WPH'].clip(30, 300)  # fix lower and upper WPH to 30 and 300
df_last24['WPH'] = df_last24['WPH'].round(0)

df_last24['HOUR'] = df_last24['LAST_WAFER_END_DATE'].dt.ceil('h')

df_last24['RECIPE'] = df_last24['LOT_RECIPE'].str.split('[ +?]+').str[0]
df_last24['RECIPE'] = df_last24['RECIPE'].str.slice(0, 30)
df_last24 = df_last24.sort_values(by='ENTITY')

#print(df_last24)

# Define a color scale
#color_scale = px.colors.qualitative.Plotly
#color_mapping = {ent_lot: color for ent_lot, color in zip(df_last24['ENT_LOT'].unique(), color_scale)}
#color_dict = {recipe: color for recipe, color in zip(df_last24['RECIPE'].unique(), px.colors.qualitative.Plotly[:len(df_last24['RECIPE'].unique())])}
# Map 'RECIPE' to numbers
df_last24['RECIPE_NUM'] = df_last24['RECIPE'].astype('category').cat.codes

# Create a dropdown menu with options for each unique entity
dropdown = [{'label': entity, 'method': 'update', 'args': [{'visible': [entity == unique_entity for unique_entity in df_last24['ENTITY'].unique()]}]} for entity in df_last24['ENTITY'].unique()]

# Create a bar for each unique entity
fig = go.Figure(data=[go.Bar(
    x=df_last24[df_last24['ENTITY'] == entity]['HOUR'], 
    y=df_last24[df_last24['ENTITY'] == entity]['PROCESSED_WAFER_COUNT'], 
    name=entity,
    marker_color=df_last24[df_last24['ENTITY'] == entity]['RECIPE_NUM'],  # Set the color of the bars based on the 'RECIPE_NUM' column
    marker=dict(
        colorscale='Viridis',  # Use the 'Viridis' color scale
        colorbar=dict(title="RECIPE"),
    ),
    #marker_color=df_last24[df_last24['ENTITY'] == entity]['RECIPE'].map(color_dict),  # Set the color of the bars based on the 'RECIPE' column
    #marker_color=df_last24[df_last24['ENTITY'] == entity]['WPH'],  # Set the color of the bars based on the 'ENT_LOT' column
    #marker_color=df_last24[df_last24['ENTITY'] == entity]['WPH'], # Set the color of the bars based on the 'ENT_LOT' column
    #marker_color=df_last24[df_last24['ENTITY'] == entity]['ENT_LOT'].map(color_mapping),  # Set the color of the bars based on the 'ENT_LOT' column
    width=3600000,  # Set the width of the bars to 1 hour in milliseconds
    hovertemplate=
    '<i>ENT_LOT</i>: %{customdata[0]}<br>'+
    '<i>ENT_OPERATION</i>: %{customdata[1]}<br>' +
    '<i>OPER_SHORT_DESC</i>: %{customdata[2]}<br>' +
    '<i>ENT_LOT_PROCESS</i>: %{customdata[3]}<br>' +
    '<i>PRODUCT_DESCRIPTION</i>: %{customdata[4]}<br>' +
    '<i>PRODUCT</i>: %{customdata[5]}<br>' +
    '<i>DOTPROCESS</i>: %{customdata[6]}<br>' +
    '<i>LOT_ABORT_FLAG</i>: %{customdata[7]}<br>' +
    '<i>ROUTE</i>: %{customdata[8]}<br>' +
    '<i>RECIPE</i>: %{customdata[9]}<br>' +
    '<i>QTY</i>: %{customdata[10]}<br>' +
    '<i>WPH</i>: %{customdata[11]}<br>',
    customdata=df_last24[df_last24['ENTITY'] == entity][['ENT_LOT', 'ENT_OPERATION', 'OPER_SHORT_DESC', 'ENT_LOT_PROCESS', 'PRODUCT_DESCRIPTION', 'PRODUCT', 'DOTPROCESS', 'LOT_ABORT_FLAG', 'ROUTE', 'RECIPE', 'PROCESSED_WAFER_COUNT', 'WPH']].values
) for entity in df_last24['ENTITY'].unique()])

# Set the color scale
#fig.update_traces(marker=dict(color=df_last24['WPH'], colorscale='YlGn', cmin=80, cmax=200))

# Set the barmode to 'stack' and fix x axis range
fig.update_layout(barmode='stack')
fig.update_xaxes(range=[min_date, max_date])

# Add the dropdown menu to the layout
fig.update_layout(updatemenus=[{'buttons': dropdown, 'showactive': True}])

# Get the current date and time
now = datetime.now()

# Format the current date and time as a string
now_str = now.strftime('%Y-%m-%d %H:%M:%S')

# Create a title for the chart
title = f'Last 24 hours - Script Ran at: {now_str}'
fig.update_layout(title=title)
fig.update_layout(yaxis_title='Wafers Processed')

# Write the figure to an HTML file
fig.write_html(f'//f21pucnasn1.f21prod.mfg.intel.com/FuzionUploads/Litho/Tracks/dashLAST24/lots_processed_last_30d_chart.html')


''' Create a table with filterable columns'''
# Define the columns to include
columns = [col for col in df_last24.columns if col not in ['RECIPE_NUM', 'HOUR', 'CURRENT_HOTLOT', 'CURRENT_ONHOLD', 'ENT_LOT_PROCESS', 'LOT_ENTITY_PROCESS_DURATION', 'RETICLE', 'LOT_RECIPE']]

# Add 'LOT_RECIPE' to the end of the list
columns.append('LOT_RECIPE')

# Sort the DataFrame by 'LAST_WAFER_END_DATE' in descending order
df_last24 = df_last24.sort_values('LAST_WAFER_END_DATE', ascending=False)

# Convert the DataFrame to an HTML table without the index and with only the specified columns
html_table = df_last24[columns].to_html(index=False, classes='filterable')

# Define the JavaScript code
javascript = """
<script>
// Get all the headers
var headers = document.querySelectorAll('.filterable th');

// For each header
headers.forEach(function(header, index) {
    // Create a text box
    var textBox = document.createElement('input');

    // When something is typed in the text box
    textBox.onkeyup = function() {
        // Get the rows
        var rows = document.querySelectorAll('.filterable tbody tr');

        // For each row
        rows.forEach(function(row) {
            // If the text box is empty or its content is found in the corresponding cell
            if (textBox.value === '' || row.cells[index].textContent.includes(textBox.value)) {
                // Show the row
                row.style.display = '';
            } else {
                // Hide the row
                row.style.display = 'none';
            }
        });
    };

    // Add the text box to the header
    header.appendChild(textBox);
});
</script>
"""

# Add the JavaScript code to the HTML table
html_table += javascript

# Write the HTML table to a file
with open(f'//f21pucnasn1.f21prod.mfg.intel.com/FuzionUploads/Litho/Tracks/dashLAST24/lots_processed_last_30d_table.html', 'w') as f:
    f.write(html_table)