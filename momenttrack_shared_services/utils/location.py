import datetime
import statistics

from sqlalchemy.orm import lazyload
from momenttrack_shared_models.core.database.models import (
    User,
    Location,
    LicensePlate,
    LicensePlateMove
)


class LocationService:
    """Location service"""

    def __init__(self, db=None):
        self.db = db

    @staticmethod
    def get_location_report(location, session=None):
        """Get report by location object"""

        location.logs = None
        location.oldest_license_plate = None
        location.current_user = None
        location.oldest_log = None
        location.latest_log = None

        if session:
            # Get all the moves from this location
            lp_moves = (
                LicensePlateMove.query.with_session(session)
                .options(lazyload(LicensePlateMove.user))
                .options(lazyload(LicensePlateMove.product))
                .options(lazyload(LicensePlateMove.license_plate))
                .filter_by(dest_location_id=location.id)
                .order_by(LicensePlateMove.created_at.desc())
                .all()
            )
        else:
            # Get all the moves from this location
            lp_moves = (
                LicensePlateMove.query.options(lazyload(LicensePlateMove.user))
                .options(lazyload(LicensePlateMove.product))
                .options(lazyload(LicensePlateMove.license_plate))
                .filter_by(dest_location_id=location.id)
                .order_by(LicensePlateMove.created_at.desc())
                .all()
            )

        # If moves are available, calc:
        if lp_moves:
            location.logs = lp_moves

            # average_duration
            dates = [lpm.created_at for lpm in lp_moves]
            diffs = [(t2 - t1).total_seconds() for t1, t2 in zip(dates[:-1], dates[1:])]
            if len(diffs) > 0:
                location.average_duration = datetime.timedelta(
                    seconds=statistics.mean(diffs)
                ).seconds
            else:
                location.average_duration = 0

            # oldest items
            location.oldest_log = lp_moves[-1]
            location.latest_log = lp_moves[0]
            location.oldest_license_plate = LicensePlate.get(
                lp_moves[-1].license_plate_id
            )
            location.current_user = User.get(lp_moves[-1].user_id)

        return location

    @staticmethod
    def move_lp(src_id, dest_id, db, session=None, count=1):
        if not session:
            session = db.writer_session()
        src_loc = Location.get_by_id(src_id, session=session)
        dest_loc = Location.get_by_id(dest_id, session=session)
        if src_loc.lp_qty > 0:
            src_loc.lp_qty -= count
            dest_loc.lp_qty += count

            session.commit()

    @staticmethod
    def add_lp(location, session=None, count=1):
        location.lp_qty += count
        session.commit()
