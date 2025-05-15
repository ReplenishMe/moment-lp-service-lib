from typing import Dict

from momenttrack_shared_models import (
    Organization,
    LicensePlate,
    LicensePlateStatusEnum,
    ActivityTypeEnum
)
from momenttrack_shared_models.core.schemas import UserCommentsSchema
import momenttrack_shared_models.core.messages as MSG
from momenttrack_shared_models.core.extensions import db
from sqlalchemy.exc import SQLAlchemyError

from .actions.move import Move
from .actions.create import Create
from .actions.deduct import Deduct
from .actions.split import Split
from .actions.wrap import Wrap
from .actions.cycle_count import CycleCount
from .actions.edit import _edit
from .utils.activity import ActivityService
from .utils import DBErrorHandler


class LicensePlateServiceAgent:
    def __init__(self, db_config, os_client=None):
        self.db = db
        self.os_client = os_client
        self.db_config = db_config
        self.pool_size = db_config.pop('SQLALCHEMY_DB_POOL_SIZE', 20)
        self.db = db.init_db(
            db_config,
            pool_size=self.pool_size
        )

    def move(
        self, move_item_id,
        dest_location_id, org_id,
        headers, user_id,
        loglocation=None
    ):
        db = self.db
        client = self.os_client
        _move = Move(
            db,
            move_item_id,
            org_id,
            dest_location_id,
            user_id,
            headers,
            client,
            loglocation
        )
        lp_move = _move.execute()
        return lp_move

    def create(
        self,
        lp, org_id,
        user_id, headers,
        session=None,
        production_order_id=None,
        comment=None
    ):
        db = self.db
        client = self.os_client
        _create = Create(
            db,
            org_id,
            user_id,
            client,
            headers,
            comment=comment
        )
        lp = _create.execute(
            license_plate=lp,
            production_order_id=production_order_id
        )
        return lp

    def deduct(
        self, lp,
        total_qty_to_deduct,
        org_id, headers,
        user_id,
    ):
        db = self.db
        client = self.os_client
        _deduct = Deduct(
            db,
            lp,
            total_qty_to_deduct,
            org_id,
            user_id,
            client,
            headers
        )
        lp_deduct = _deduct.execute()
        return lp_deduct
    
    def split(
        self, lp,
        split_distribution,
    ):
        db = self.db
        client = self.os_client
        _split = Split(
            db,
            lp,
            split_distribution,
            client
        )
        lp_split = _split.execute()
        return lp_split
    
    def wrap(
        self, payload,
        org_id, headers,
        user_id,
    ):
        db = self.db
        client = self.os_client
        _wrap = Wrap(
            db,
            payload,
            org_id,
            user_id,
            headers,
            client
        )
        lp_wrap = _wrap.execute()
        return lp_wrap

    def cycle_count(
        self,
        license_plate_id,
    ):
        db = self.db
        client = self.os_client
        _cycle_count = CycleCount(
            db,
            license_plate_id,
            client
        )
        lp_cycle_count = _cycle_count.execute()
        return lp_cycle_count

    def comment(self, lp_id, message, org_id, user_id, headers):
        org = Organization.get(org_id)
        license_plate = LicensePlate.get_by_id_and_org(lp_id, org)
        if (
            license_plate is None
            or license_plate.status == LicensePlateStatusEnum.DELETED
        ):
            raise Exception(MSG.LICENSE_PLATE_NOT_FOUND)
        license_plate_id = license_plate.id
        schema = UserCommentsSchema()
        activity_service = ActivityService(
            self.db, self.os_client, org.id, user_id, headers
        )
        activity_service.log(
            "license_plate",
            license_plate_id,
            ActivityTypeEnum.COMMENT,
            message=message,
        )
        try:
            db.writer_session.commit()
            resp = schema.dump(license_plate)
            return resp
        except KeyError as ke:
            raise Exception(f"Missing key: {str(ke)}")
        except ValueError as ve:
            raise Exception(f"Invalid value: {str(ve)}")
        except SQLAlchemyError as e:
            DBErrorHandler(e)
        finally:
            db.writer_session.close()

    def edit(self, lp_obj, org_id):
        return _edit(self.db, lp_obj, org_id, self.os_client)
