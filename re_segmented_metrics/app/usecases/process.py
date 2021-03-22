# pylint: disable=no-member
# utf-8
import logging
from utils.read_params import ReadParams
from usecases.active_ads import ActiveAds
from usecases.ad_views import AdViews
from usecases.deleted_ads import DeletedAds
from usecases.uleads_wo_showphone import UniqueLeadsWithoutShowPhone
from usecases.naa import NewApprovedAds


class Process:

    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):

        # Active Ads
        self.logger.info("Active Ads usecase start")
        self.active_ads = ActiveAds(self.config,
                                    self.params,
                                    self.logger).generate()
        self.logger.info("Active Ads usecase success")
        # Unique Leads w/o ShowPhone
        self.logger.info("Unique Leads w/o ShowPhone usecase start")
        self.uleads_wo_showphone = UniqueLeadsWithoutShowPhone(self.config,
                                                               self.params,
                                                               self.logger).generate()
        self.logger.info("Finished. Unique Leads w/o ShowPhone usecase success")
        # Ad Views
        self.logger.info("Ad-Views usecase start")
        self.ad_views = AdViews(self.config,
                                      self.params,
                                      self.logger).generate()
        self.logger.info("Finished. Ad-Views Ads usecase success")
        # NAA
        self.logger.info("NAA usecase start")
        self.naa = NewApprovedAds(self.config,
                                  self.params,
                                  self.logger).generate()
        self.logger.info("Finished. NAA usecase success")
        # Deleted Ads
        self.logger.info("Deleted Ads usecase starting")
        self.deleted_ads = DeletedAds(self.config,
                                      self.params,
                                      self.logger).generate()
        self.logger.info("Finished. Deleted Ads usecase success")










