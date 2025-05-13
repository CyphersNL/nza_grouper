import polars as pl

import subtraject as subt


def maak_subtrajecten_dict(df, path_csv=""):
    """
    Deze laad de subtrajectenview data in en maakt hier Subtraject classes van.
    """
    # Laad de csv in. Alles naar string behalve aantal en leeftijd
    if df.is_empty():
        df = pl.read_csv(path_csv, separator=';', dtypes={'aantal_za': pl.Float64,
                                                          'Subtrajectnummer': str,
                                                          'SpecialismeCode': str,
                                                          'DiagnoseCode': str,
                                                          'zorgtype': str})
    first_row = df.row(0, named=True)
    # Maak een lege dictionary om de resultaten op te slaan
    subtraject_dict = {}
    saved_subtraject = first_row['Subtrajectnummer']
    zorgactiviteiten_dict = {}
    dbcOpenDatum = first_row['dbcOpenDatum']
    SpecialismeCode = first_row['SpecialismeCode']
    zorgtype = first_row['zorgtype']
    ZorgVraagCode = first_row['ZorgVraagCode']
    DiagnoseCode = first_row['DiagnoseCode']
    leeftijd = first_row['leeftijd']
    geslacht = first_row['geslacht']
    subtraject = None
    for row in df.iter_rows(named=True):
        # Filter de dataframe voor het huidige subtrajectnummer
        if row['Subtrajectnummer'] != saved_subtraject:
            # Casus nieuw subtraject
            # Extraheer de relevante kolommen voor de subtraject class
            subtraject = subt.Subtraject(DiagnoseCode, SpecialismeCode, ZorgVraagCode, zorgtype, geslacht,
                                         leeftijd, dbcOpenDatum, None, "", zorgactiviteiten_dict, "")
            subtraject_dict[saved_subtraject] = subtraject
            dbcOpenDatum = row['dbcOpenDatum']
            SpecialismeCode = row['SpecialismeCode']
            zorgtype = row['zorgtype']
            ZorgVraagCode = row['ZorgVraagCode']
            DiagnoseCode = row['DiagnoseCode']
            leeftijd = row['leeftijd']
            geslacht = row['geslacht']
            saved_subtraject = row['Subtrajectnummer']
            zorgactiviteiten_dict = {}
            zorgactiviteiten_dict[row['ZorgActiviteitCode']] = row['aantal_za']

        else:
            zorgactiviteiten_dict[row['ZorgActiviteitCode']] = row['aantal_za']

    return subtraject_dict


def polars_df_naar_dict(dataframe, key_column):
    """
    Converteert polars df naar dict met een key column
    VEREISTE: de key column moet voor elke rij uniek zijn, anders ga je overschrijven
    """
    header_dict = {col: i for i, col in enumerate(dataframe.columns)}
    num_columns = len(header_dict)
    i_key = header_dict[key_column]

    processed_dict = {}
    for row in dataframe.rows():
        key_value = row[i_key]
        values = [row[i] for i in range(0, num_columns)]
        processed_dict[key_value] = values

    return processed_dict, header_dict


def polars_df_naar_dict_simple(dataframe, key_column, value_column):
    header_dict = {col: i for i, col in enumerate(dataframe.columns)}
    key_column = header_dict[key_column]
    value_column = header_dict[value_column]

    processed_dict = {}
    for row in dataframe.rows():
        key_value = row[key_column]
        value_value = row[value_column]
        processed_dict[key_value] = value_value
    return processed_dict


if __name__ == "__main__":
    print("Test code begint")
    import os
    PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    FOLDER_PATH_CSV = os.path.join(PARENT_DIR, 'data', 'subtraject_input', 'data2021.csv')
    df = pl.read_csv(FOLDER_PATH_CSV, separator=';', dtypes={'aantal_za': pl.Float64,
                                                             'Subtrajectnummer': str,
                                                             'SpecialismeCode': str,
                                                             'DiagnoseCode': str})
    a, b = polars_df_naar_dict(df, 'Subtrajectnummer')
    print(a)
    print(b)
