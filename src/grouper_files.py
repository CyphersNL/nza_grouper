"""
Hier het inladen van de grouper files
Hulptabellen:
- Attribuutgroepen, gelinked met attributen (als 1)
- Boomparameters
- Diagnoses
- Zorgtypen
- Zorgvragen
- Geslachten
- Zorgactviteit met behandelklassen
- Zorgproductgroep


Inladen in XML:
- Beslisregels

Deze bestanden worden lokaal weggezet zodat de grouper ze direct kan aanspreken zonder door de XML heen te gaan.

De grouper bestanden worden in een speciale data structuur opgeslagen die feitelijk een dictionary-in-dictionary is.
Paketten als Polars kunnen erg snel zijn, maar native data types van Python gaan veel sneller dan deze externe bestanden.
"""

import os
from datetime import date
import re
import pickle

import polars as pl
from lxml import etree

import data_handling as dh


def save_object_as_pickle(obj, save_path):
    # Zeker weten dat de folder bestaat
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    # Sla het bestand op als een pickle file.
    with open(save_path, 'wb') as file:
        pickle.dump(obj, file)
    print(f"Object successfully saved to {save_path}")


def parse_xml_to_dict(element):
    main_list = []
    data = {}
    for child in element.iterchildren():
        
        for grandchild in child.iterchildren():
            if grandchild.tag.endswith("Cluster"):
                for grandgrandchild in grandchild.iterchildren():
                    key = grandgrandchild.get("Key")
                    value = grandgrandchild.text if grandgrandchild.text is not None else ""
                    parent_tag = grandchild.tag
                    num_digits = 1  # Hier kan je nog het getal omhoog gooien als je meer verwacht
                    column_name = f"{parent_tag}_{str(key).zfill(num_digits)}"
                    data[column_name] = value
            else:
                # Ga met de andere objecten direct om
                data[grandchild.tag] = grandchild.text if grandchild.text is not None else ""
        main_list.append(data)
        data = {}
    
    return main_list


def process_referentietabellen(file_path, save_path_pickles):
    """
    Lees de XML data in en maak er dictionaries van. Sla deze vervolgens opals pickles.
    """
    # Parse het XML bestand
    tree = etree.parse(file_path)

    # Kijk waar het begin is
    root = tree.getroot()

    node_names = ['Specialismen', 'ZorgProductGroepen', 'ZorgVragen',  # Zorgtypes en producten niet meenemen
                  'Diagnosen', 'ZorgActiviteiten', 'BehandelKlassen']
    unique_names = ['specialismecode', '', 'zorgvraagattribuutcode',
                    'diagnoseattribuutcode', 'zorgactiviteitcode', '']

    jaren_voor_grouper = [2021, 2022, 2023]

    for index, node_name in enumerate(node_names):
        # Vind de specifieke node die we willen
        specific_node = root.find(".//" + node_name)

        if specific_node is not None:
            # elements = specific_node.findall('*')
            data = parse_xml_to_dict(specific_node)
            df = pl.DataFrame(data, infer_schema_length=10000)
            df = df.with_columns(pl.col("BeginDatum").str.to_date())
            df = df.with_columns(pl.col("EindDatum").fill_null('9999-12-31'))
            df = df.with_columns(pl.col("EindDatum").str.to_date())
            # Opslaan als pickle
            for jaar in jaren_voor_grouper:
                print(jaar)
                begin_filter = date(jaar, 12, 31)  # Waarom niet 1-1-jaar? Soms begint het halverwege en we snijden af.
                eind_filter = date(jaar, 12, 31)
                filtered_df = df.filter(pl.col("BeginDatum") <= pl.lit(begin_filter))
                filtered_df = filtered_df.filter(pl.col("EindDatum") >= pl.lit(eind_filter))
                filtered_df.sort("BeginDatum")
                filtered_df = filtered_df.select(pl.exclude("BeginDatum", "VersieDatum", "EindDatum"))
                filtered_df = filtered_df.unique(keep="last")  # We doen deze stappen zodat we geen halfverwege-veranderaars hebben die dubbel meekomen
                filtered_df = filtered_df.rename(str.lower)
                
                if node_name in ['ZorgActiviteiten', 'Specialismen', 'ZorgVragen', 'Diagnosen']:
                    values_dict, header_dict = dh.polars_df_naar_dict(filtered_df, unique_names[index])
                    save_object_as_pickle(values_dict, os.path.join(save_path_pickles, f"{jaar}_{node_name}_values.pkl"))
                    save_object_as_pickle(header_dict, os.path.join(save_path_pickles, f"{jaar}_{node_name}_headers.pkl"))
                elif node_name == "ZorgProductGroepen":
                    zorgproductgroep_dict = dh.polars_df_naar_dict_simple(filtered_df, 'zorgproductgroepcode', 'beslisregelstart')
                    save_object_as_pickle(zorgproductgroep_dict, os.path.join(save_path_pickles, f"{jaar}_Zorgproductgroep_dict.pkl"))
                else:
                    behandelklasse_dict = {}
                    header_dict = {col: i for i, col in enumerate(filtered_df.columns)}
                    for row in filtered_df.rows():
                        zorgproductgroep_code = row[header_dict['zorgproductgroepcode']]
                        zorg_activiteit_code = row[header_dict['zorgactiviteitcode']]
                        behandeklasse_code = row[header_dict['behandelklassecode']]

                        # Als de zorgproductgroepcode nog niet in the dictionary is, voeg het dan toe
                        if zorgproductgroep_code not in behandelklasse_dict:
                            behandelklasse_dict[zorgproductgroep_code] = {}

                        # Voeg de zorgactiviteitcode en de behandelklassecode toe aan de geneste dictionary
                        behandelklasse_dict[zorgproductgroep_code][zorg_activiteit_code] = behandeklasse_code
                    save_object_as_pickle(behandelklasse_dict, os.path.join(save_path_pickles, f"{jaar}_Behandelklasse_dict.pkl"))

        else:
            print("Node '{}' not found in XML file '{}'.".format(node_name, file_path))


def process_boombestanden(file_path, save_path_pickles):
    # Parse het XML bestand
    tree = etree.parse(file_path)

    # Kijk waar het begin is
    root = tree.getroot()

    node_names = ['BoomParameters', 'BeslisRegels', 'AttribuutGroepen',]

    for node_name in node_names:
        specific_node = root.find(".//" + node_name)

        if specific_node is not None:
            elements = specific_node.findall('*')
            if node_name == "BeslisRegels":
                result = {}
                # Itereeer door elk 'beslisregel' element
                for beslisregel in elements:
                    # Initialiseer een dictionary om voor elke beslisregel een key-value paar te maken
                    beslisregel_dict = {}
                    # Extraheer en sla de data op voor elke key behalve beslisregelid
                    for elem in beslisregel.getchildren():
                        if elem.tag != 'BeslisRegelId':
                            beslisregel_dict[elem.tag] = elem.text
                    # Gebruik beslisregelid als de key en sla de dictionary op voor de beslisregel
                    result[beslisregel.findtext('BeslisRegelId')] = beslisregel_dict
                
                save_object_as_pickle(result, os.path.join(save_path_pickles, "beslisboom.pkl"))

            elif node_name == "AttribuutGroepen":  # Combi met 'AttribuutGroepKoppelingen' en 'Attributen'

                elements = specific_node.findall('*')
                data_attrigroep = [{child.tag: child.text for child in element.iterchildren()} for element in elements]
                data_attrigroep = pl.DataFrame(data_attrigroep)

                specific_node = root.find(".//" + "Attributen")
                elements = specific_node.findall('*')
                data_attri = [{child.tag: child.text for child in element.iterchildren()} for element in elements]
                data_attri = pl.DataFrame(data_attri, infer_schema_length=10000)

                specific_node = root.find(".//" + "AttribuutGroepKoppelingen")
                elements = specific_node.findall('*')
                data_koppel = [{child.tag: child.text for child in element.iterchildren()} for element in elements]
                data_koppel = pl.DataFrame(data_koppel)

                data_attrigroep = data_attrigroep.join(data_koppel, on="AttribuutGroepId", how="left")
                data_attrigroep = data_attrigroep.select("AttribuutGroepId", "AantalVoorwaardenVoorTrue", "AttribuutId", "OnderToetsWaarde", "BovenToetsWaarde")
                data_attrigroep = data_attrigroep.join(data_attri, on="AttribuutId", how="left")
                data_attrigroep = data_attrigroep.select("AttribuutGroepId", "AantalVoorwaardenVoorTrue", "AttribuutId", "OnderToetsWaarde", "BovenToetsWaarde",
                                                         "BoomParameterNummer", "FilterToetsWijze", "FilterWaardeType", "OnderFilterWaarde",
                                                         "BovenFilterWaarde")
                attri_dict = {}
                for atr_groep, andere_koloms in data_attrigroep.group_by(['AttribuutGroepId']):
                    atr_groep = re.sub(r'[^0-9]', '', str(atr_groep))
                    transposed_df = andere_koloms.select([
                        'AantalVoorwaardenVoorTrue',
                        'BoomParameterNummer',
                        'OnderFilterWaarde',
                        'BovenFilterWaarde',
                        'OnderToetsWaarde',
                        'BovenToetsWaarde'
                    ]).transpose()

                    values_list = transposed_df.to_numpy().tolist()

                    attri_dict[atr_groep] = values_list

                save_object_as_pickle(attri_dict, os.path.join(save_path_pickles, "attri_dict.pkl"))

            elif node_name == "BoomParameters":
                elements = specific_node.findall('*')
                boomparameters = [{child.tag: child.text for child in element.iterchildren()} for element in elements]
                boomparameters = pl.DataFrame(boomparameters)
                boomparameters = boomparameters.select("BoomParameterNummer", "VeldNaam", "AttribuutWaardeBepaling")
                boomparameters = dh.polars_df_naar_dict_simple(boomparameters, 'BoomParameterNummer', 'VeldNaam')
                save_object_as_pickle(boomparameters, os.path.join(save_path_pickles, "boomparameters.pkl"))
            else:
                print("Klopt niet.")


if __name__ == "__main__":
    # Vanaf hier testcode
    PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    FOLDER_PATH_REF = os.path.join(PARENT_DIR, 'data', 'grouper_bestanden', '20240101 Referenties v20230928.xml')
    FOLDER_PATH_BOOMBESTANDEN = os.path.join(PARENT_DIR, 'data', 'grouper_bestanden', '20240101 BoomBestanden v20230928.xml')
    FOLDER_PATH_PICKLES = os.path.join(PARENT_DIR, 'data', 'grouper_pickles')
    process_referentietabellen(FOLDER_PATH_REF, FOLDER_PATH_PICKLES)
    process_boombestanden(FOLDER_PATH_BOOMBESTANDEN, FOLDER_PATH_PICKLES)
