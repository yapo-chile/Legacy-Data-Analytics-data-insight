# pylint: disable=no-member
# utf-8


class PortalSegmentQuery:

    def query(self) -> str:
        """
        Method return str with query
        """
        query = """
            select {0} {1} {2} {3}
            """.format(self.params.get_date_from(),
                       self.params.get_date_to(),
                       self.params.get_current_year(),
                       self.params.get_last_year())
        return query
