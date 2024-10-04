from typing import Dict

from momenttrack_shared_models.core.extensions import db

from .exc import DatabaseConnectionError
from .actions.move import Move


class LicensePlateServiceAgent:
    def __init__(self, db_config, os_client=None):
        self.os_client = os_client
        self.db_config = db_config
        self.db = db.init_db(db_config)

    def move(self, lp, dest_location_id, org_id, headers, user_id, loglocation=None):
        db = self.db
        client = self.os_client
        _move = Move(
            db,
            lp,
            org_id,
            dest_location_id,
            user_id,
            headers,
            client,
            loglocation
        )
        try:
            lp_move = _move.execute()
        except Exception as e:
            print('Errr occured')
            raise e
        return lp_move

    def create(self, lp):
        pass
