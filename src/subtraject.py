from datetime import datetime


class Subtraject:
    """
    Dit is een class waarin we de subtraject defineren voor de grouper.
    """
    def __init__(self, diagnosen, specialismen, zorgvragen, zorgtypen, geslachten,
                 leeftijd, begindatum, einddatum=None, zorgproduct="", zorgactiviteiten={}, patientnummer=""):
        self._zorgactiviteiten = zorgactiviteiten
        self._diagnosen = diagnosen if diagnosen is not None else '98989898'
        self._specialismen = specialismen if specialismen is not None else '98989898'
        self._zorgvragen = zorgvragen if zorgvragen is not None else '98989898'
        self._zorgtypen = zorgtypen if zorgtypen is not None else '98989898'
        self._geslachten = geslachten if geslachten is not None else 'm'
        self._zorgproduct = zorgproduct
        self._leeftijd = int(leeftijd)
        self._begindatum = begindatum
        self._einddatum = einddatum if einddatum is not None else datetime.today().strftime('%Y-%m-%d')
        self._patientnummer = patientnummer
        self._zorgproductgroepcode = '0'
        self._zorgvraag_attribuut_code = self._specialismen + '.' + self._zorgvragen
        self._diagnose_attribuut_code = self._specialismen + '.' + self._diagnosen
        self._grouperjaar = begindatum[:4] if isinstance(begindatum, str) else str(begindatum.year)

    def get_zorgactiviteiten_dict(self):
        return self._zorgactiviteiten

    def get_zorgactiviteiten_list(self):
        za_dict = self._zorgactiviteiten
        za_list = list(za_dict.keys())
        return za_list

    def set_zorgactiviteiten(self, zorgactiviteiten):
        assert isinstance(zorgactiviteiten, dict), "zorgactiviteiten moet een dictionary zijn"
        self._zorgactiviteiten = zorgactiviteiten

    def get_diagnosen(self):
        return self._diagnosen

    def set_diagnosen(self, diagnosen):
        assert isinstance(diagnosen, str), "diagnosen moet een string zijn"
        self._diagnosen = diagnosen

    def get_specialismen(self):
        return self._specialismen

    def set_specialismen(self, specialismen):
        assert isinstance(specialismen, str), "specialismen moet een string zijn"
        self._specialismen = specialismen

    def get_zorgvragen(self):
        return self._zorgvragen

    def set_zorgvragen(self, zorgvragen):
        assert isinstance(zorgvragen, str), "zorgvragen moet een string zijn"
        self._zorgvragen = zorgvragen

    def get_zorgtypen(self):
        return self._zorgtypen

    def set_zorgtypen(self, zorgtypen):
        assert isinstance(zorgtypen, str), "zorgtypen moet een string zijn"
        self._zorgtypen = zorgtypen

    def get_geslachten(self):
        return self._geslachten

    def set_geslachten(self, geslachten):
        assert isinstance(geslachten, str), "geslachten moet een string zijn"
        self._geslachten = geslachten

    def get_zorgproduct(self):
        return self._zorgproduct

    def set_zorgproduct(self, zorgproduct):
        assert isinstance(zorgproduct, str), "zorgproduct moet een string zijn"
        self._zorgproduct = zorgproduct

    def get_leeftijd(self):
        return self._leeftijd

    def set_leeftijd(self, leeftijd):
        assert isinstance(leeftijd, int), "leeftijd moet een integer zijn"
        self._zorgproduct = int(leeftijd)

    def get_begindatum(self):
        return self._begindatum

    def set_begindatum(self, begindatum):
        assert isinstance(begindatum, str), "begindatum moet een string zijn"
        self._begindatum = begindatum

    def get_einddatum(self):
        return self._einddatum

    def set_einddatum(self, einddatum):
        assert isinstance(einddatum, str), "einddatum moet een string zijn"
        self._einddatum = einddatum

    def get_patientnummer(self):
        return self._patientnummer

    def set_patientnummer(self, patientnummer):
        assert isinstance(patientnummer, str), "patientnummer moet een string zijn"
        self._patientnummer = patientnummer

    def get_diagnose_attribuut_code(self):
        return self._diagnose_attribuut_code

    def get_zorgvraag_attribuut_code(self):
        return self._zorgvraag_attribuut_code

    def get_zorgproductgroepcode(self):
        return self._zorgproductgroepcode

    def set_zorgproductgroepcode(self, zorgproductgroepcode):
        assert isinstance(zorgproductgroepcode, str), "zorgproductgroepcode moet een string zijn"
        self._zorgproductgroepcode = zorgproductgroepcode

    def get_grouperjaar(self):
        return self._grouperjaar
  

if __name__ == "__main__":
    print("hello")
    g = Subtraject('nan', '3', '3', '11', 'm', 11, '', '')
    print(g.get_diagnose_attribuut_code())
