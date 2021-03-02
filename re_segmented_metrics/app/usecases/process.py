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
            self.uleads_wo_showphone = UniqueLeadsWithoutShowPhone(self.config,
                                                                   self.params,
                                                                   self.logger).generate()
            self.ad_views = AdViews(self.config,
                                          self.params,
                                          self.logger).generate()
            self.naa = NewApprovedAds(self.config,
                                      self.params,
                                      self.logger).generate()
            self.deleted_ads = DeletedAds(self.config,
                                          self.params,
                                          self.logger).generate()
            self.active_ads = ActiveAds(self.config,
                                        self.params,
                                        self.logger).generate()


