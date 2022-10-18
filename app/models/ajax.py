import __main__ as app

from models.config import get_version
from models.query_consumption_daily import ConsumptionDaily
from models.query_consumption_detail import ConsumptionDetail
from models.query_production_daily import ProductionDaily
from models.query_production_detail import ProductionDetail

class Ajax:

    def __init__(self, usage_point_id=None):
        self.cache = app.CACHE
        self.application_path = app.APPLICATION_PATH
        self.usage_point_id = usage_point_id
        if self.usage_point_id is not None:
            self.config = self.cache.get_config(self.usage_point_id)
        print(self.usage_point_id)
        print(self.config)
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': self.config['token'],
            'call-service': "myelectricaldata",
            'version': get_version()
        }
        self.usage_points_id_list = ""

    def reset_all_data(self):
        app.LOG.title(f"[{self.usage_point_id}] Reset de la consommation journalière.")
        ConsumptionDaily(
            headers=self.headers,
            usage_point_id=self.usage_point_id,
            config=self.config,
        ).reset()
        app.LOG.title(f"[{self.usage_point_id}] Reset de la consommation détaillée.")
        ConsumptionDetail(
            headers=self.headers,
            usage_point_id=self.usage_point_id,
            config=self.config,
        ).reset()
        app.LOG.title(f"[{self.usage_point_id}] Reset de la production journalière.")
        ProductionDaily(
            headers=self.headers,
            usage_point_id=self.usage_point_id,
            config=self.config,
        ).reset()
        app.LOG.title(f"[{self.usage_point_id}] Reset de la production détaillée.")
        ProductionDetail(
            headers=self.headers,
            usage_point_id=self.usage_point_id,
            config=self.config,
        ).reset()
        return {
            "error": "false",
            "notif": "Toutes les données ont était supprimées.",
        }


    def reset_data(self, target, date):
        app.LOG.title(f"[{self.usage_point_id}] Reset de la {target} journalière du {date}:")
        if target == "consommation":
            result = ConsumptionDaily(
                headers=self.headers,
                usage_point_id=self.usage_point_id,
                config=self.config,
            ).reset(date)
        elif target == "production":
            result = ProductionDaily(
                headers=self.headers,
                usage_point_id=self.usage_point_id,
                config=self.config,
            ).reset(date)
        else:
            return {
                "error": "true",
                "notif": "Target inconnue.",
                "result": ""
            }
        if result:
            return {
                "error": "false",
                "notif": f"Reset de la {target} journalière du {date}",
                "result": result
            }
        else:
            return {
                "error": "true",
                "notif": "Erreur lors du traitement.",
                "result": result
            }

    def fetch(self, target, date):
        app.LOG.title(f"[{self.usage_point_id}] Importation de la {target} journalière du {date}:")
        if target == "consommation":
            result = ConsumptionDaily(
                headers=self.headers,
                usage_point_id=self.usage_point_id,
                config=self.config,
            ).fetch(date)
        elif target == "production":
            result = ProductionDaily(
                headers=self.headers,
                usage_point_id=self.usage_point_id,
                config=self.config,
            ).fetch(date)
        else:
            return {
                "error": "true",
                "notif": "Target inconnue.",
                "result": ""
            }
        if "error" in result and result["error"]:
            print(result)
            return {
                "error": "true",
                "notif": result['notif'],
                "result": {
                    "value": 0,
                    "date": date
                }
            }
        else:
            return {
                "error": "false",
                "notif": f"Importation de la {target} journalière du {date}",
                "result": {
                    "value": result["value"],
                    "date": result["date"]
                }
            }

    def blacklist(self, target, date):
        app.LOG.title(f"[{self.usage_point_id}] Blacklist de la {target} journalière du {date}:")
        if target == "consommation":
            result = ConsumptionDaily(
                headers=self.headers,
                usage_point_id=self.usage_point_id,
                config=self.config,
            ).blacklist(date, True)
        elif target == "production":
            result = ProductionDaily(
                headers=self.headers,
                usage_point_id=self.usage_point_id,
                config=self.config,
            ).blacklist(date, True)
        else:
            return {
                "error": "true",
                "notif": "Target inconnue.",
                "result": ""
            }
        if not result:
            return {
                "error": "true",
                "notif": "Erreur lors du traitement.",
                "result": result
            }
        else:
            return {
                "error": "false",
                "notif": f"Blacklist de la {target} journalière du {date}",
                "result": result
            }

    def whitelist(self, target, date):
        app.LOG.title(f"[{self.usage_point_id}] Whitelist de la {target} journalière du {date}:")
        if target == "consommation":
            result = ConsumptionDaily(
                headers=self.headers,
                usage_point_id=self.usage_point_id,
                config=self.config,
            ).blacklist(date, False)
        elif target == "production":
            result = ProductionDaily(
                headers=self.headers,
                usage_point_id=self.usage_point_id,
                config=self.config,
            ).blacklist(date, False)
        else:
            return {
                "error": "true",
                "notif": "Target inconnue.",
                "result": ""
            }
        if not result:
            return {
                "error": "true",
                "notif": "Erreur lors du traitement.",
                "result": result
            }
        else:
            return {
                "error": "false",
                "notif": f"Whitelist de la {target} journalière du {date}",
                "result": result
            }

    def import_data(self):
        app.LOG.title(f"[{self.usage_point_id}] Récupération de la consommation journalière:")
        result = ConsumptionDaily(
            headers=self.headers,
            usage_point_id=self.usage_point_id,
            config=self.config,
        ).get()
        if not result:
            return {
                "error": "true",
                "notif": "Erreur lors du traitement.",
                "result": result
            }
        else:
            return {
                "error": "false",
                "notif": "Récupération de la consommation journalière",
                "result": result
            }