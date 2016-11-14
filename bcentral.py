
# coding: utf-8

# In[1]:



from bs4 import BeautifulSoup
import pandas as pd
import requests

class BancoCentralChile:
    """
    Requests the Central Bank of Chile for data from a selected time period.
    Object gets a .df attribute with the returned time series as a pandas DataFrame() object.
    
    Params:
        - series_begin: year in which data begins. Starts from January 1st.
        - series_end: year in which data ends. Ends on December 31st.
        - param: Needs to be extracted from bcentral.cl's dynamic Excel query (.iqy)
            http://si3.bcentral.cl/Siete/secure/cuadros/home.aspx
    """
    
    def __init__(self, series_begin, series_end, param):
        url = 'http://si3.bcentral.cl/SieteIQY/secure/carga_series_excel.aspx' # as of 2016.11.01
        self.fechaInicio = series_begin
        self.fechaFin = series_end
        self.param = param
        self.payload = {'fechaInicio': str(self.fechaInicio),
                        'fechaFin': str(self.fechaFin),
                        'param': self.param
                       }
        
        def get_soup(payload):
            r = requests.post(url, data=payload)
            try:
                soup = BeautifulSoup(r.text, 'lxml') #faster
            except:
                soup = BeautifulSoup(r.text, 'html.parser') #default
            return soup
        
        def get_headers(tables):
            headers = tables[0]
            headers = headers.tr
            clean_headers = []
            for i in headers:
                try:
                    clean_headers.append(i.text)
                except AttributeError:
                    # data might contain useless info which raises exception, skip
                    pass
            return clean_headers
            
        def get_data_columns(tables):
            final_data = []
            for tseries in tables[1:]:
                series = []
                for item in tseries:
                    series.extend(item.td.contents)
                final_data.append(series)
            return final_data
        
        def replace_string(data_list, replace_string='--', replacement=None):
            replaced_data = []
            for series in data_list:
                replaced = [replacement if x == replace_string else x for x in series]
                replaced_data.append(replaced)
            return replaced_data
        
        def data_to_float(data):
            return_data = [data[0]]
            for data_series in data[1:]: #skip dates
                temp_data = [float(i) if i is not None else i for i in data_series]
                return_data.append(temp_data)
            return return_data

        def check_data_length(data):
            # http://stackoverflow.com/questions/16720844/compare-length-of-three-lists-in-python
            length = len(data[0])
            if all(len(lst) == length for lst in data[1:]):
                return True
            else:
                return False

        def get_zipped_data(data):
            return [i for i in zip(*data)]
        
        def get_dataframe(data=None, index=None, columns=None):
            return pd.DataFrame(data=data, index=index, columns=columns)

        soup = get_soup(self.payload)
        tables = soup.find_all('table')
        headers = get_headers(tables)
        data = get_data_columns(tables)
        data = replace_string(data, replace_string='--', replacement=None)
        data = data_to_float(data)
        if not check_data_length(data):
            raise AssertionError("Length of data series doesn't match")
        final_data = get_zipped_data(data)
        self.df = get_dataframe(final_data, columns=headers)

