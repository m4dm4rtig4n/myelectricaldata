"""This module contains the Job class, which is responsible for importing data from the API."""

import logging
import time
import traceback
from typing import List

from config.main import APP_CONFIG
from config.myelectricaldata import UsagePointId
from database import DB
from database.usage_points import DatabaseUsagePoints
from external_services.home_assistant.main import HomeAssistant
from external_services.home_assistant_ws.main import HomeAssistantWs
from external_services.influxdb.main import ExportInfluxDB
from external_services.mqtt.main import ExportMqtt
from external_services.myelectricaldata.address import Address
from external_services.myelectricaldata.contract import Contract
from external_services.myelectricaldata.daily import Daily
from external_services.myelectricaldata.detail import Detail
from external_services.myelectricaldata.ecowatt import Ecowatt
from external_services.myelectricaldata.power import Power
from external_services.myelectricaldata.status import Status
from external_services.myelectricaldata.tempo import Tempo
from models.stat import Stat
from utils import export_finish, finish, get_version, log_usage_point_id, title


class Job:
    """Represents a job for importing data."""

    def __init__(self, usage_point_id=None):
        self.usage_point_id = usage_point_id
        self.usage_point_config: UsagePointId = {}
        self.wait_job_start: int = 10
        self.tempo_enable: bool = False
        if self.usage_point_id is None:
            self.usage_points_all: List[UsagePointId] = DatabaseUsagePoints().get_all()
        else:
            self.usage_points_all: List[UsagePointId] = [DatabaseUsagePoints(self.usage_point_id).get()]

    def boot(self):
        """Boots the import job."""
        if APP_CONFIG.dev:
            logging.warning("=> Import job disable")
        else:
            self.job_import_data()

    def job_import_data(self, wait=True, target=None):  # noqa: PLR0912, C901
        """Import data from the API."""
        if DB.lock_status():
            return {"status": False, "notif": "Importation déjà en cours..."}
        DB.lock()

        if wait:
            title("Démarrage du job d'importation dans 10s")
            i = self.wait_job_start
            while i > 0:
                logging.info(f"{i}s")
                time.sleep(1)
                i = i - 1

        # ######################################################################################################
        # FETCH TEMPO DATA
        if target == "tempo" or target is None:
            self.get_tempo()

        # ######################################################################################################
        # FETCH ECOWATT DATA
        if target == "ecowatt" or target is None:
            self.get_ecowatt()

        for usage_point_config in self.usage_points_all:
            self.usage_point_config = usage_point_config
            usage_point_id = usage_point_config.usage_point_id
            log_usage_point_id(usage_point_id)
            DatabaseUsagePoints(usage_point_id).last_call_update()
            if usage_point_config.enable:
                #######################################################################################################
                # CHECK ACCOUNT DATA
                if target == "account_status" or target is None:
                    self.get_account_status()

                #######################################################################################################
                # CONTRACT
                if target == "contract" or target is None:
                    self.get_contract()

                #######################################################################################################
                # ADDRESSE
                if target == "addresses" or target is None:
                    self.get_addresses()

                #######################################################################################################
                # CONSUMPTION / PRODUCTION
                if target == "consumption" or target is None:
                    self.get_consumption()

                if target == "consumption_detail" or target is None:
                    self.get_consumption_detail()

                if target == "production" or target is None:
                    self.get_production()

                if target == "production_detail" or target is None:
                    self.get_production_detail()

                if target == "consumption_max_power" or target is None:
                    self.get_consumption_max_power()

                #######################################################################################################
                # STATISTIQUES
                if target == "stat" or target is None:
                    self.stat_price()

                #######################################################################################################
                # MQTT
                if target == "mqtt" or target is None:
                    self.export_mqtt()

                #######################################################################################################
                # HOME ASSISTANT
                if target == "home_assistant" or target is None:
                    self.export_home_assistant()

                #######################################################################################################
                # HOME ASSISTANT WS
                if target == "home_assistant_ws" or target is None:
                    self.export_home_assistant_ws()

                #######################################################################################################
                # INFLUXDB
                if target == "influxdb" or target is None:
                    self.export_influxdb()
            else:
                logging.info(
                    " => Point de livraison Désactivé dans la configuration (Exemple: https://tinyurl.com/2kbd62s9)."
                )

        finish()

        self.usage_point_id = None
        DB.unlock()
        return {"status": True, "notif": "Importation terminée"}

    def header_generate(self, token=True):
        """Generate the header for the API request.

        Args:
            token (bool, optional): Whether to include the authorization token in the header. Defaults to True.

        Returns:
            dict: The generated header as a dictionary.
        """
        output = {
            "Content-Type": "application/json",
            "call-service": "myelectricaldata",
            "version": get_version(),
        }
        if token:
            output["Authorization"] = self.usage_point_config.token
        return output

    def get_gateway_status(self):
        """Retrieve the status of the gateway.

        This method retrieves the status of the gateway by pinging it. If an error occurs during the process,
        it logs the error message.

        Returns:
            None
        """
        detail = "Récupération du statut de la passerelle"
        try:
            title(detail)
            Status(headers=self.header_generate(token=False)).ping()
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de la {detail.lower()}")
            logging.error(e)

    def get_account_status(self):
        """Retrieve the account status information.

        This method retrieves the account status information for the usage point(s).
        It sets the error log if there is an error in the status response.

        Returns:
            None
        """
        detail = "Récupération des informations du compte"

        def run():
            usage_point_id = self.usage_point_config.usage_point_id
            title(f"[{usage_point_id}] {detail}")
            status = Status(headers=self.header_generate()).status(usage_point_id=usage_point_id)
            if status.get("error"):
                message = f'{status["status_code"]} - {status["description"]["detail"]}'
                DatabaseUsagePoints(usage_point_id).set_error_log(message)
            else:
                DatabaseUsagePoints(usage_point_id).set_error_log(None)
            export_finish()

        try:
            if self.usage_point_config is None:
                for usage_point_config in self.usage_points_all:
                    self.usage_point_config = usage_point_config
                    if self.usage_point_config.enable:
                        run()
            else:
                run()
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de la {detail.lower()}")
            logging.error(e)

    def get_contract(self):
        """Retrieve contract information for the usage points.

        This method iterates over the list of usage points and retrieves the contract information
        for each enabled usage point. If a specific usage point ID is provided, it retrieves the
        contract information only for that usage point.

        Args:
            self: The current instance of the Jobs class.

        Returns:
            None

        Raises:
            Exception: If an error occurs during the retrieval of contract information.

        """
        detail = "Récupération des informations contractuelles"

        def run(usage_point_config):
            usage_point_id = usage_point_config.usage_point_id
            title(f"[{usage_point_id}] {detail}")
            Contract(
                headers=self.header_generate(),
                usage_point_id=usage_point_id,
                config=usage_point_config,
            ).get()
            export_finish()

        try:
            if self.usage_point_id is None:
                for usage_point_config in self.usage_points_all:
                    if usage_point_config.enable:
                        run(usage_point_config)
            else:
                run(self.usage_point_config)
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de la {detail.lower()}")
            logging.error(e)

    def get_addresses(self):
        """Retrieve the postal addresses for the usage points.

        This method iterates over the list of usage points and retrieves the postal addresses
        for each enabled usage point. It calls the `Address.get()` method to fetch the addresses
        and then calls the `export_finish()` function to indicate the completion of the export.

        If a specific usage point ID is provided, only that usage point will be processed.

        Raises:
            Exception: If an error occurs during the retrieval of postal addresses.

        """
        detail = "Récupération des coordonnées postales"

        def run(usage_point_config):
            usage_point_id = usage_point_config.usage_point_id
            title(f"[{usage_point_id}] {detail}")
            Address(headers=self.header_generate(), usage_point_id=usage_point_id).get()
            export_finish()

        try:
            if self.usage_point_id is None:
                for usage_point_config in self.usage_points_all:
                    if usage_point_config.enable:
                        run(usage_point_config)
            else:
                run(self.usage_point_config)
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de la {detail.lower()}")
            logging.error(e)

    def get_consumption(self):
        """Get consumption from gateway."""
        detail = "Récupération de la consommation journalière"

        def run(usage_point_config):
            usage_point_id = usage_point_config.usage_point_id
            title(f"[{usage_point_id}] {detail}")
            if hasattr(usage_point_config, "consumption") and usage_point_config.consumption:
                Daily(headers=self.header_generate(), usage_point_id=usage_point_id).get()
                export_finish()
            else:
                logging.info(f"{detail} désactivée sur le point de livraison")

        try:
            if self.usage_point_id is None:
                for usage_point_config in self.usage_points_all:
                    if usage_point_config.enable:
                        run(usage_point_config)
            else:
                run(self.usage_point_config)
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de la {detail.lower()}")
            logging.error(e)

    def get_consumption_detail(self):
        """Retrieve the detailed consumption."""
        detail = "Récupération de la consommation détaillée"

        def run(usage_point_config):
            usage_point_id = usage_point_config.usage_point_id
            title(f"[{usage_point_id}] {detail}")
            if hasattr(usage_point_config, "consumption_detail") and usage_point_config.consumption_detail:
                Detail(headers=self.header_generate(), usage_point_id=usage_point_id).get()
                export_finish()
            else:
                logging.info(f"{detail} désactivée sur le point de livraison")

        try:
            if self.usage_point_id is None:
                for usage_point_config in self.usage_points_all:
                    if usage_point_config.enable:
                        run(usage_point_config)
            else:
                run(self.usage_point_config)
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de la {detail.lower()}")
            logging.error(e)

    def get_production(self):
        """Retrieve the detailed production."""
        detail = "Récupération de la production journalière"

        def run(usage_point_config):
            usage_point_id = usage_point_config.usage_point_id
            title(f"[{usage_point_id}] {detail}")
            if hasattr(usage_point_config, "production") and usage_point_config.production:
                Daily(
                    headers=self.header_generate(),
                    usage_point_id=usage_point_id,
                    measure_type="production",
                ).get()
                export_finish()
            else:
                logging.info(f"{detail} désactivée sur le point de livraison")

        try:
            if self.usage_point_id is None:
                for usage_point_config in self.usage_points_all:
                    if usage_point_config.enable:
                        run(usage_point_config)
            else:
                run(self.usage_point_config)
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de la {detail.lower()}")
            logging.error(e)

    def get_production_detail(self):
        """Get production detail from gateway."""
        detail = "Récupération de la production détaillée"

        def run(usage_point_config):
            usage_point_id = usage_point_config.usage_point_id
            title(f"[{usage_point_id}] {detail}")
            if hasattr(usage_point_config, "production_detail") and usage_point_config.production_detail:
                Detail(
                    headers=self.header_generate(),
                    usage_point_id=usage_point_id,
                    measure_type="production",
                ).get()
                export_finish()
            else:
                logging.info(f"{detail} désactivée sur le point de livraison")

        try:
            if self.usage_point_id is None:
                for usage_point_config in self.usage_points_all:
                    if usage_point_config.enable:
                        run(usage_point_config)
            else:
                run(self.usage_point_config)
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de la {detail.lower()}")
            logging.error(e)

    def get_consumption_max_power(self):
        """Get max power from gateway."""
        detail = "Récupération de la puissance maximum journalière"

        def run(usage_point_id: str, usage_point_config: UsagePointId) -> None:
            title(f"[{usage_point_id}] {detail}")
            if getattr(usage_point_config, "consumption_max_power", True):
                Power(headers=self.header_generate(), usage_point_id=usage_point_id).get()
                export_finish()
            else:
                logging.info(f"{detail} désactivée sur le point de livraison")

        try:
            if self.usage_point_id is None:
                usage_point_config: UsagePointId
                for usage_point_id, usage_point_config in APP_CONFIG.myelectricaldata.usage_point_config.items():
                    if usage_point_config.enable:
                        run(usage_point_id, usage_point_config)
            else:
                run(self.usage_point_id, APP_CONFIG.myelectricaldata.usage_point_config[self.usage_point_id])
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de la {detail.lower()}")
            logging.error(e)

    def get_tempo(self):
        """Get tempo from gateway."""
        try:
            title("Récupération des données Tempo")
            Tempo().fetch()
            title("Calcul des jours Tempo")
            Tempo().calc_day()
            title("Récupération des tarifs Tempo")
            Tempo().fetch_price()
            export_finish()
        except Exception as e:
            traceback.print_exc()
            logging.error("Erreur lors de la récupération des données Tempo")
            logging.error(e)

    def get_ecowatt(self):
        """Get ecowatt from gateway."""
        try:
            title("Récupération des données EcoWatt")
            Ecowatt().fetch()
            export_finish()
        except Exception as e:
            traceback.print_exc()
            logging.error("Erreur lors de la récupération des données EcoWatt")
            logging.error(e)

    def stat_price(self):
        """Stat price."""
        detail = "Génération des statistiques Tarifaire de consommation/production "

        def run(usage_point_config):
            usage_point_id = usage_point_config.usage_point_id
            title(f"[{usage_point_id}] {detail}")
            if hasattr(usage_point_config, "consumption_detail") and usage_point_config.consumption_detail:
                logging.info("Consommation :")
                Stat(usage_point_id=usage_point_id, measurement_direction="consumption").generate_price()
            if hasattr(usage_point_config, "production_detail") and usage_point_config.production_detail:
                logging.info("Production :")
                Stat(usage_point_id=usage_point_id, measurement_direction="production").generate_price()
            export_finish()

        try:
            if self.usage_point_id is None:
                for usage_point_config in self.usage_points_all:
                    if usage_point_config.enable:
                        run(usage_point_config)
            else:
                run(self.usage_point_config)
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de l'{detail.lower()}")
            logging.error(e)

    def export_home_assistant(self, target=None):
        """Export data to Home Assistant."""
        detail = "Exportation des données vers Home Assistant (via MQTT)"

        def run(usage_point_id, target):
            title(f"[{usage_point_id}] {detail}")
            if target is None:
                HomeAssistant(usage_point_id).export()
            elif target == "ecowatt":
                HomeAssistant(usage_point_id).ecowatt()
            export_finish()

        try:
            if APP_CONFIG.home_assistant:
                if APP_CONFIG.mqtt:
                    if self.usage_point_id is None:
                        for usage_point_config in self.usage_points_all:
                            if usage_point_config.enable:
                                run(usage_point_config.usage_point_id, target)
                    else:
                        run(self.usage_point_id, target)
                else:
                    logging.critical(
                        "L'export Home Assistant est dépendant de MQTT, "
                        "merci de configurer MQTT avant d'exporter vos données dans Home Assistant"
                    )
            else:
                title("Désactivé dans la configuration (Exemple: https://tinyurl.com/2kbd62s9)")
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Erreur lors de l'{detail.lower()}")
            logging.error(e)

    def export_home_assistant_ws(self):
        """Export to Home Assistant Energy."""
        detail = "Import des données vers l'onglet Energy de Home Assistant (WebSocket)"
        usage_point_id = self.usage_point_config.usage_point_id
        title(f"[{usage_point_id}] {detail}")
        if APP_CONFIG.home_assistant_ws.enable:
            HomeAssistantWs(usage_point_id)
        else:
            title("Désactivé dans la configuration (Exemple: https://tinyurl.com/2kbd62s9)")

    def export_influxdb(self):
        """Export to influxdb data."""
        detail = "Import des données vers InfluxDB"
        usage_point_id = self.usage_point_config.usage_point_id
        title(f"[{usage_point_id}] {detail}")
        if APP_CONFIG.influxdb.enable:
            ExportInfluxDB(usage_point_id)
        else:
            title("Désactivé dans la configuration (Exemple: https://tinyurl.com/2kbd62s9)")

    def export_mqtt(self):
        """MQTT Export."""
        detail = "Import des données vers MQTT"
        usage_point_id = self.usage_point_config.usage_point_id
        title(f"[{usage_point_id}] {detail}")
        if APP_CONFIG.mqtt.enable:
            ExportMqtt(usage_point_id)
        else:
            title("Désactivé dans la configuration (Exemple: https://tinyurl.com/2kbd62s9)")
