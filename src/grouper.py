import os
from datetime import datetime
import mmap
import pickle

import polars as pl

import data_handling as dh


PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FOLDER_PATH_PICKLES = os.path.join(PARENT_DIR, 'data', 'grouper_pickles')
FOLDER_PATH_CSV = os.path.join(PARENT_DIR, 'data', 'subtraject_input', 'data2021.csv')
SUBTRAJECTEN_DICT = dh.maak_subtrajecten_dict(pl.DataFrame(), FOLDER_PATH_CSV)


# Laad alle pickles in waar we onderdelen van de grouper hebben.
for filename in os.listdir(FOLDER_PATH_PICKLES):
    if filename.endswith('.pkl'):
        filepath = os.path.join(FOLDER_PATH_PICKLES, filename)
        
        # Open de pickel bestanden en laat hun inhoud in
        # Dit doen we met mmap om het sneller te doen
        with open(filepath, 'rb') as file:
            # data = pickle.load(file)
            data = pickle.loads(mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ))
        
        # Gebruik het bestands naam zonder extensie als de variabel naam
        variable_name = os.path.splitext(filename)[0]
        
        # Geef een global variable naam aan alles wat we inladen
        globals()[variable_name] = data


def check_attribuut_groep_dicts(attribuut_groep_id, subtraject, jaar='2021'):
    """
    In deze functie checken we of voor het subtraject de attribuutgroep ID is voldaan of niet.
    """
    attri_dict = globals()["attri_dict"]
    boomparameters = globals()["boomparameters"]
    Behandelklasse_dict = globals()[f"{jaar}_Behandelklasse_dict"]
    Diagnosen_values = globals()[f"{jaar}_Diagnosen_values"]
    Diagnosen_headers = globals()[f"{jaar}_Diagnosen_headers"]
    ZorgActiviteiten_values = globals()[f"{jaar}_ZorgActiviteiten_values"]
    ZorgActiviteiten_headers = globals()[f"{jaar}_ZorgActiviteiten_headers"]
    ZorgVragen_values = globals()[f"{jaar}_ZorgVragen_values"]
    ZorgVragen_headers = globals()[f"{jaar}_ZorgVragen_headers"]
    Specialismen_values = globals()[f"{jaar}_Specialismen_values"]
    Specialismen_headers = globals()[f"{jaar}_Specialismen_headers"]

    hoeveelheid_true = 0

    # Attribuut filteren
    aantal_true_grens = int(attri_dict[attribuut_groep_id][0][0])
    boomparameter_nummers = attri_dict[attribuut_groep_id][1]
    onder_filters = attri_dict[attribuut_groep_id][2]
    boven_filters = attri_dict[attribuut_groep_id][3]
    onder_toets = attri_dict[attribuut_groep_id][4]
    boven_toets = attri_dict[attribuut_groep_id][5]

    # Boomparameters veldnamen ophalen
    veldnamen = [boomparameters[key] for key in boomparameter_nummers]

    # Nu gaan we elk attribuut checken, totdat we genoeg attributen hebben en we doorkunnen.
    index = 0
    total_attributen = len(boven_filters)
    for veldnaam, onder_filter, boven_filter, onder_toet, boven_toet in zip(veldnamen, onder_filters, boven_filters, onder_toets, boven_toets):
        index = index + 1

        match veldnaam:
            case v if v[:10] == 'specialism':
                column_positie = Specialismen_headers[v]
                if Specialismen_values.get(subtraject.get_specialismen(), 999999) != 999999:
                    if onder_filter == Specialismen_values[subtraject.get_specialismen()][column_positie]:
                        hoeveelheid_true = hoeveelheid_true + 1

            case v if v[:4] == 'diag' or v == 'icd_diagnosecode':
                column_positie = Diagnosen_headers[v]
                if Diagnosen_values.get(subtraject.get_diagnose_attribuut_code(), 999999) != 999999:
                    if onder_filter == Diagnosen_values[subtraject.get_diagnose_attribuut_code()][column_positie]:
                        hoeveelheid_true = hoeveelheid_true + 1

            case "leeftijd":
                if int(onder_filter) <= subtraject.get_leeftijd() <= int(boven_filter):
                    hoeveelheid_true = hoeveelheid_true + 1

            case "geslacht":
                if onder_filter == subtraject.get_geslachten():
                    hoeveelheid_true = hoeveelheid_true + 1

            case v if v[:8] == 'zorgtype':
                if onder_filter == subtraject.get_zorgtypen():
                    hoeveelheid_true = hoeveelheid_true + 1

            case v if v[:9] == 'zorgvraag':
                column_positie = ZorgVragen_headers[v]
                if ZorgVragen_values.get(subtraject.get_zorgvraag_attribuut_code(), 99999) != 99999:
                    if onder_filter == ZorgVragen_values[subtraject.get_zorgvraag_attribuut_code()][column_positie]:
                        hoeveelheid_true = hoeveelheid_true + 1

            case v if v[:14] == 'zorgactiviteit':
                za_dict_subtraject = subtraject.get_zorgactiviteiten_dict()
                column_positie = ZorgActiviteiten_headers[v]
                common_keys = set(za_dict_subtraject.keys()) & set(ZorgActiviteiten_values.keys())
                sum_za = sum(za_dict_subtraject[za] for za in common_keys if ZorgActiviteiten_values[za][column_positie] == onder_filter)
                if int(onder_toet) <= sum_za <= int(boven_toet):
                    hoeveelheid_true = hoeveelheid_true + 1

            case "behandelklassecode":
                behandelklasse_groep_dict = Behandelklasse_dict.get(subtraject.get_zorgproductgroepcode(), None)

                if behandelklasse_groep_dict is None:
                    pass
                else:
                    za_dict_subtraject = subtraject.get_zorgactiviteiten_dict()
                    # behandelklasse_list = list(behandelklasse_groep_dict.get(key, '') for key in za_dict.keys())
                    behandelklasse_list = [value for key, value in behandelklasse_groep_dict.items() if key in za_dict_subtraject for _ in range(int(za_dict_subtraject[key]))]  # Noqa: E501
                    # sum_za = sum(value for id_, value in za_dict.items() if behandelklasse_groep_dict.get(id_) == onder_filter)
                    # TODO: Onderstaande in combinatie met bovenstaande kan sneller, maar daar moet nog even naar gekeken worden
                    sum_za = behandelklasse_list.count(onder_filter)
                    if int(onder_toet) <= sum_za <= int(boven_toet):
                        hoeveelheid_true = hoeveelheid_true + 1

            case "begindatum":
                if onder_filter <= subtraject.get_begindatum() <= boven_filter:
                    hoeveelheid_true = hoeveelheid_true + 1

            case _:
                print("Geen match gevonden!")

        # Hebben we genoeg? Dan hoeven we de rest niet te bekijken.
        if hoeveelheid_true >= aantal_true_grens:
            return True

        if index == total_attributen and hoeveelheid_true < aantal_true_grens:
            return False


def loop_beslisboom_door(subtraject, jaar='2021', beslisregel_id='1468217'):
    """
    Ga door de beslisboom heen

    1468217 is de start node voor zp groep 0 in 2024b
    """
    beslisboom = globals()["beslisboom"]
    Zorgproductgroep_dict = globals()[f"{jaar}_Zorgproductgroep_dict"]

    # Haal de gegevens op voor de beslisregelID
    attribuut_groep_id = beslisboom[beslisregel_id].get("AttribuutGroepId")
    beslis_regel_true = beslisboom[beslisregel_id].get("BeslisRegelTrue")
    beslis_regel_false = beslisboom[beslisregel_id].get("BeslisRegelFalse")
    label_true = beslisboom[beslisregel_id].get("LabelTrue")
    label_false = beslisboom[beslisregel_id].get("LabelFalse")

    attribuut_bool = check_attribuut_groep_dicts(attribuut_groep_id, subtraject, jaar)

    # Hier bepalen we wat we nu moeten doen gegeven de attribuut groep.
    if attribuut_bool and beslis_regel_true is not None:
        return loop_beslisboom_door(subtraject, jaar, beslis_regel_true)
    elif attribuut_bool and label_true is not None:
        if len(label_true) == 6:
            subtraject.set_zorgproductgroepcode(label_true)
            return loop_beslisboom_door(subtraject, jaar, Zorgproductgroep_dict[label_true])
        else:
            return label_true
    elif not attribuut_bool and beslis_regel_false is not None:
        return loop_beslisboom_door(subtraject, jaar, beslis_regel_false)
    elif not attribuut_bool and label_false is not None:
        if len(label_false) == 6:
            subtraject.set_zorgproductgroepcode(str(label_false))
            return loop_beslisboom_door(subtraject, jaar, Zorgproductgroep_dict[label_false])
        else:
            return label_false
    else:
        print("Geen conditie gevonden! Gaat niet goed.")


def process_subtraject(subtrajectnummer, subtraject):
    if subtraject.get_grouperjaar() == '2021':
        return subtrajectnummer, loop_beslisboom_door(subtraject)
    return None


def process_results(future, results):
    try:
        subtraject_id, zpnummer = future.result()
        results.append((subtraject_id, zpnummer))

    except Exception as e:
        print(f"An error occurred: {e}")


def test_single_processing():
    """
    Test de grouper
    """
    result_dict = {}
    print("Current Time (milliseconds) start:", datetime.now().strftime("%H:%M:%S.%f")[:-3])
    for subtrajectnummer, subtraject in SUBTRAJECTEN_DICT.items():
        if subtraject.get_grouperjaar() == '2021':
            result_dict[subtrajectnummer] = loop_beslisboom_door(subtraject)

    print("Current Time (milliseconds) end:", datetime.now().strftime("%H:%M:%S.%f")[:-3])
    df = pl.DataFrame(result_dict)
    df = df.transpose(include_header=True, header_name='Subtrajectnummer', column_names=['Beslisregel'])
    print(df)


def test_multiprocessing():
    """
    Test de grouper
    """
    import multiprocessing as mp
    print("Current Time (milliseconds) start:", datetime.now().strftime("%H:%M:%S.%f")[:-3])

    with mp.Pool(processes=4) as pool:
        results = pool.starmap(process_subtraject, SUBTRAJECTEN_DICT.items())

    print("Current Time (milliseconds) end:", datetime.now().strftime("%H:%M:%S.%f")[:-3])
    df = pl.DataFrame(results, schema=['subtrajectid', 'zpnummer'])
    print(df)


def test_concurrent_futures():
    """
    Werkt nog niet
    """

    import concurrent.futures
    max_workers = 8
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        results = []
        for subtraject_id, subtraject in SUBTRAJECTEN_DICT.items():
            future = executor.submit(process_subtraject, subtraject_id, subtraject)
            future.add_done_callback(process_results)
            futures.append(future)
        
        concurrent.futures.wait(futures)

    print("Current Time (milliseconds) end:", datetime.now().strftime("%H:%M:%S.%f")[:-3])
    df = pl.DataFrame(results, schema=['subtrajectid', 'zpnummer'])
    print(df)


def test_met_profiling():
    import cProfile
    import pstats
    profiler = cProfile.Profile()
    profiler.enable()
    test_single_processing()
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats('cumtime')
    stats.print_stats()


if __name__ == "__main__":
    test_single_processing()
