# NZA Grouper

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Status: In Development](https://img.shields.io/badge/Status-In%20Development-orange.svg)]()

Dit is een grouper van de NZA geschreven in Python die nog in ontwikkeling is. Het is momenteel mogelijk om met de grouper voor bijna alle specialismes zorgproducten te bepalen. Revalidatie en de specifieke regels die daarbij horen wat betreft minuten komen er momenteel nog niet goed uit. Het is verder getest op de grouperbestanden RZ24b voor het jaar 2021, en daar lijken correcte resultaten uit te komen.

## Inhoudsopgave
- [Beschrijving](#beschrijving)
- [Hoe werkt de grouper](#hoe-werkt-de-grouper)
- [Data map structuur](#Data-map-structuur)
- [Installatie](#installatie)
- [TODO](#todo)
- [Bijdragen](#bijdragen)

## Beschrijving
De NZA Grouper is een Python-implementatie van het Nederlandse Zorgautoriteit (NZA) grouperingssysteem. Dit systeem wordt gebruikt om zorgactiviteiten te groeperen in zorgproducten volgens de NZA-regels en richtlijnen.

## Hoe werkt de grouper

### grouper_files.py
Ten eerste moeten de NZA grouper bestanden worden ingelezen en verwerkt worden tot de speciale data structuur die is opgezet om zo snel mogelijk data op te zoeken. Bij testen kwam namelijk al snel naar voren dat externe libraries simpelweg te lang kosten om waardes als de onder- en bovengrens voor een check in een node op te halen.

Voor elk afzonderlijk jaar maken we afzonderlijk pickle bestanden.

### subtraject.py en data_handling.py
Het subtraject bestand defineert de class die we gebruiken om de data bij te houden voor elk subtraject. Data_handling heeft een functie om data in een bepaald formaat in te lezen en in een dictionary te zetten.

### grouper.py
Het bestand met de daadwerkelijke logica van de grouper en enkele test scripts die de grouper uitprobeert in verschillende manieren. Denk hierbij aan een enkel subtraject in 1 keer of meerdere subtracten verspreiden over verschillende threads.

## Data map structuur
De data map heeft de volgende structuur:

```
data/
├── grouper_bestaden/    # De XMLs met boombestanden en referenties
├── grouper_pickles/     # De verwerkte grouper bestanden voor elk jaar
└── subtrajecten_input/  # Waar de data moet staan zoals in onderstaand kopje aangegeven
```

### Invoerformaat
Het verwachte formaat voor de invoerdata in subtrajecten_input is als volgt (in CSV formaat met ; als seperator):
``` CSV
+----------------+---------+----------+--------------+----------------+----------+--------------+-------------+------------------+------------+-----------+
| Subtrajectnr   | leeftijd| geslacht | dbcOpenDatum | SpecialismeCode| zorgtype | ZorgVraagCode| DiagnoseCode| ZorgActiviteitCode| za_datum   | aantal_za |
+----------------+---------+----------+--------------+----------------+----------+--------------+-------------+------------------+------------+-----------+
| 02150031054    | 20      | M        | 2021-03-22   | 0303           | 11       | onbekend     | 302         | 030616           | 2021-03-22 | 1         |
+----------------+---------+----------+--------------+----------------+----------+--------------+-------------+------------------+------------+-----------+
```

## Installatie
```
git clone https://github.com/CyphersNL/nza_grouper.git
cd nza_grouper
python -m venv .venv
pip install -r requirements.txt
```

Vervolgens kan je de code afzonderlijk afvuren vanuit grouper.py of eventueel de Dockerfile gebruiken.In beide gevallen moeten de bestanden in de data folder klaar staan zoals hierboven aangegeven.

## TODO
- Automatisch aanmaken van folders waarin data wordt opgeslagen
- Opzetten van teststraat
- Testen of nieuwe grouper bestanden correct worden verwerkt
- Testen of verschillende jaren werken
- Revalidatie en de daarbij passende rekenregels toevoegen
- Dockerfile aanpassen en updaten
- Algemene refactoring van de code
- Kijken of het nog sneller kan draaien door vooruitstrevend te grouperen (dus huidige node en de node vooruit)
- Commentaar/documentatie code toevoegen

## Bijdragen
Bijdragen zijn welkom. Open voor grote issues graag eerst een issue om dit te bespreken.