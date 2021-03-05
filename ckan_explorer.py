import streamlit as st
import datetime
import pandas as pd
import messytables
import itertools
from ckanapi import RemoteCKAN
import json

TYPE_MAPPING = {
    'String': 'text',
    'Integer': 'numeric',
    'Decimal': 'numeric',
    'DateUtil': 'timestamp'
}

TYPES = [messytables.StringType, messytables.DecimalType,
          messytables.IntegerType, messytables.DateUtilType]


def chunky(iterable, n):
    """
    Generates chunks of data that can be loaded into ckan
    :param n: Size of each chunks
    :type n: int
    """
    it = iter(iterable)
    item = list(itertools.islice(it, n))
    while item:
        yield item
        item = list(itertools.islice(it, n))


def parse_data(fh):
    
    try:
        table_set = messytables.any_tableset(fh)
    except messytables.ReadError as e:
        print(e)
    
    get_row_set = lambda table_set: table_set.tables.pop()
    row_set = get_row_set(table_set)
    offset, headers = messytables.headers_guess(row_set.sample)
    # Some headers might have been converted from strings to floats and such.
    headers = [str(header) for header in headers]
    
    row_set.register_processor(messytables.headers_processor(headers))
    row_set.register_processor(messytables.offset_processor(offset + 1))
    types = messytables.type_guess(row_set.sample, types=TYPES, strict=True)
    
    row_set.register_processor(messytables.types_processor(types))

    headers = [header.strip() for header in headers if header.strip()]
    headers_set = set(headers)
    
    def row_iterator():
        for row in row_set:
            data_row = {}
            for index, cell in enumerate(row):
                column_name = cell.column.strip()
                if column_name not in headers_set:
                    continue
                data_row[column_name] = str(cell.value)
            yield data_row
    result = row_iterator()
    
    headers_dicts = [dict(id=field[0], type=TYPE_MAPPING[str(field[1])])
                     for field in zip(headers, types)]
    
    st.info('Determined headers and types: {headers}'.format(
        headers=headers_dicts))
    
    return headers_dicts, result


def update_resource(ckan, input, resource_id):
    _, result = parse_data(input)
    count = 0
    for i, records in enumerate(chunky(result, 250)):
        count += len(records)
        st.info('Saving chunk {number}'.format(number=i))
        ckan.action.datastore_upsert(resource_id=resource_id, records=records, force=True, method='insert')
    
    st.success('Successfully pushed {n} entries to "{res_id}".'.format(
        n=count, res_id=resource_id))
    return True


def authenticate(url, key):
    connection = RemoteCKAN(url, apikey=key)
    return connection


def option_formatter(option):
    return option["text"]


st.title("Interactive CKAN Data Loader")

ckan_url = st.sidebar.text_input('Enter CKAN URL')
api_key = st.sidebar.text_input('Enter API Key')


action = st.sidebar.radio(
    "What would you like to do ?",
    [{'id':0, 'text':"Update an Existing Resource"},
     {'id':1, 'text':"Create a New Resouce"}],
    format_func=option_formatter)


if ckan_url and api_key:
    if action['id'] == 0:
        ckan = authenticate(ckan_url, api_key)
        packages = ckan.action.package_search(rows=100, include_private=True)
        # st.write(packages["results"][0])
        # st.subheader('Select a Package')
        package_list = [{'id':-1, 'text': "Select a Package"}]
        for package in packages["results"]:
            id = package["id"]
            name = package["name"]
            package_list.append({'id': id, 'text':name})
            # st.write(package["id"])

        package_select = st.selectbox(
            "",
            package_list,
            format_func=option_formatter,
            index=0
        )
        if package_select['id'] != -1:
            package_name = package_select['text']
            st.subheader(f"Available Resources for {package_name}:")
            selected_package = ckan.action.package_show(id=package_select['id'])
            available_resources = selected_package["resources"]
            options = []
            for resource in available_resources:
                if resource["datastore_active"] == True:
                    options.append({'text':resource["name"], 'id':resource["id"]})
            selected_resource = st.radio("", options, format_func=option_formatter)
            if selected_resource:
                ds_info = ckan.action.datastore_info(id=selected_resource['id'])
                st.write(ds_info)
                this_id = selected_resource['id']
                # st.write(this_id)
                q = f'SELECT * FROM "{this_id}" order by _id desc limit 5;'
                sql = ckan.action.datastore_search_sql(
                    sql=q
                )
                st.write("Here are the last few records:")
                df = pd.DataFrame.from_dict(sql['records'])
                st.dataframe(df)
                file_type = st.radio(
                    "Looks right? What type of data would you like to add to it?.",
                    ['csv', 'excel', 'json']
                )

                file_upload = st.file_uploader('Upload your file.')
                
                if file_upload is not None:
                    table = None                  
                    if file_type == 'csv':
                        table = pd.read_csv(file_upload)
                    elif file_type == 'json':
                        table = pd.read_json(file_upload)
                    elif file_type == 'excel':
                        table = pd.read_excel(file_upload)

                    if table is not None:
                        st.write("Here are the first few rows:")
                        st.dataframe(table.head())
                    
                    button = st.button('👍 Looks Good Upload!')
                
                    if button:
                        upload = update_resource(ckan, file_upload, this_id)
                        if upload == True:
                            st.success("Sucess!")
                        else:
                            st.error('something\'s wrong')

    elif action['id'] == 1:
        st.write('coming soon')
else:
    st.write("Start by entering your CKAN Site URL")
        

