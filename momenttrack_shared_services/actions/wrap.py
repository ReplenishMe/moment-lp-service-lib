from sqlalchemy import text
from momenttrack_shared_models import (
    ActivityTypeEnum,
    Location,
    Container
)
import momenttrack_shared_models.core.messages as MSG
from momenttrack_shared_services.utils import HttpError
from momenttrack_shared_models.core.database.dberrors import DBErrorHandler
from momenttrack_shared_models.core.schemas.container_schema import (
    ContainerSchema
)
from momenttrack_shared_services.utils.activity import ActivityService


class Wrap:
    def __init__(self, db, payload, org_id, user_id, headers, client):
        self.db = db
        self.payload = payload
        self.org_id = org_id
        self.user_id = user_id
        self.client = client
        self.activity_service = ActivityService(
            db, client,
            org_id, user_id,
            headers
        )

    def execute(self):
        db = self.db
        with db.writer_session() as sess:
            sess = db.writer_session

            # create container
            loc_id = self.payload['location_id']
            if isinstance(loc_id, str):
                location: Location = sess.query(Location).filter_by(
                    beacon_id=self.payload['beacon_id']
                ).first()
            else:
                location = sess.query(Location).get(loc_id)
            if not location:
                raise HttpError(MSG.LOCATION_NOT_FOUND, 404)

            container = Container(
                quantity=location.lp_qty,
                location_id=Location.get_system_location(
                    self.org_id,
                    session=db.writer_session()
                ).id,
                organization_id=self.org_id
            )
            sess.add(container)
            sess.flush()

            lp_query = f"""
                update license_plate set container_id = {container.id}
                where location_id = {location.id} and container_id is null
            """
            container_query = f"""
                update container set parent_container_id = {container.id}
                where location_id = {location.id} and
                parent_container_id is null
            """

            # dump before activity to prevent flushing out from session
            # in activity_service.log commit()
            resp = ContainerSchema().dump(container)

            self.activity_service.log(
                "Container",
                container.id,
                ActivityTypeEnum.CONTAINER_CREATED,
                sess=db.writer_session(),
                current_org_id=self.org_id,
                current_user_id=self.user_id,
            )

            try:
                sess.execute(text(lp_query))
                sess.execute(text(container_query))
            except Exception as e:
                DBErrorHandler(e)
            else:
                sess.commit()
            return resp