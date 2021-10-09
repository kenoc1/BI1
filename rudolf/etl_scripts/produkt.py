import util
from rudolf import config
from rudolf.oracle_service import F2DBService, CombDBService
from rudolf.rudolf_exceptions import NoCombIDFoundForF2IDException
from rudolf.sqlite_service import SQLiteService


class Produkt:
    def __init__(self):
        self._init_db_connections()

    def init(self) -> None:
        self._get_data_basis()

    def _init_db_connections(self) -> None:
        # TODO try/catch
        self._db_f2 = F2DBService()
        self._db_master = CombDBService()
        self._db_rudolf = SQLiteService()

    def _get_data_basis(self) -> None:
        # TODO try/catch
        self.f2_products = self._db_f2.select_all_produkte()
        self.comb_products = self._db_master.select_all_produkte()

    @staticmethod
    def _convert_mwst(mwst: float) -> float:
        return mwst * 0.01

    def _get_new_product_class_id(self, f2_class_id: int) -> int:
        # Informationen werden hier angereichert
        # TODO nur wenn eine vorliegt, ansonsten manuell
        return self._db_rudolf.select_where_old_id(config.SUBKATEGORIE_TABLE, f2_class_id)

    def _get_new_supplier_id(self, f2_supplier_id: int) -> int:
        return self._db_rudolf.select_where_old_id(config.LIEFERANT_HERSTELLER_TABLE, f2_supplier_id)

    def _get_new_brand_id(self, f2_brand_id: int) -> int:
        return self._db_rudolf.select_where_old_id(config.MARKE_TABLE, f2_brand_id)

    def _get_new_product_id(self, f2_product_id: int) -> int:
        return self._db_rudolf.select_where_old_id(config.PRODUKTE_TABLE, f2_product_id)

    def start(self) -> None:
        for product in self.f2_products:
            if not self._is_produkt_already_transferred(product.get("PRODUKT_ID")):
                self.insert_product(product)

    def insert_product(self, product) -> None:
        f2_product_id = product["PRODUKT_ID"]
        comb_supplier_id = self._get_new_supplier_id(
            self._db_f2.select_hersteller_id_where_marke_id(product["MARKE_ID"]))

        comb_product_class_id = self._get_new_product_class_id(
            self._db_f2.select_produktkategorie_id_where_produkt_id(f2_product_id))

        comb_brand_id = self._get_new_brand_id(product["MARKE_ID"])

        purchasing_price = util.ib_dollar_to_euro(
            self._db_f2.select_current_preis_where_product_id_and_typ(f2_product_id, "Einkauf")["BETRAG"])
        selling_price = util.ib_dollar_to_euro(
            self._db_f2.select_current_preis_where_product_id_and_typ(f2_product_id, "Verkauf")["BETRAG"])
        mwst = self._convert_mwst(float(product["UMSATZSTEUERSATZ"]))

        if product["TYP"] == config.PRODUCT_TYP_F2[0]:
            weight = util.ib_lbs_to_kg(product["NETTOGEWICHT"])
            purchasing_price = (1 / weight) * purchasing_price
            selling_price = (1 / weight) * selling_price
            size_fit = config.GEWICHTSBASIERT_EINHEIT_STUECK
        else:
            size_fit = 1

        new_product_id = self._db_master.insert_product_only_required_fields(supplier_id=comb_supplier_id,
                                                                             product_class_id=comb_product_class_id,
                                                                             product_name=product["BEZEICHNUNG"],
                                                                             sku=product["SKU"],
                                                                             ean=config.DUMMY_EAN,
                                                                             discount=0,
                                                                             size_fit=size_fit,
                                                                             purchasing_price=purchasing_price,
                                                                             selling_price=selling_price,
                                                                             mwst=mwst,
                                                                             brand_id=comb_brand_id,
                                                                             source_system=config.SOURCE_F2)
        self._db_rudolf.insert_id_allocation(config.PRODUKTE_TABLE, new_product_id, f2_product_id)

    def _is_produkt_already_transferred(self, f2_produkt_id: int) -> bool:
        try:
            self._db_rudolf.select_where_old_id(table_name=config.PRODUKTE_TABLE, old_id=f2_produkt_id)
            return True
        except NoCombIDFoundForF2IDException:
            return False

    def _get_matching_products_if_exists(self, f2_product_sku: int):
        return [comb_product for comb_product in self.comb_products if comb_product.get("SKU") == f2_product_sku]


if __name__ == "__main__":
    pass
    # testing
    # Products()

    # prod
    # products = Products()
    # products.insert_brands()
    # products.insert_products_from_f2_to_master()
    # products.insert_product_price_history()
