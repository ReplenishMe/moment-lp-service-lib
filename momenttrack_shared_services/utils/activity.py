from momenttrack_shared_models import (
    Activity,
    ActivityChangeTrack,
    ActivityChangeTrackFieldTypeEnum,
    ActivityTypeEnum,
    LicensePlateMove,
    Location,
    User,
    UserStatusEnum,
)
from momenttrack_shared_services.utils import (
    DataValidationError,
    DBErrorHandler
)


class ActivityService:
    def __init__(self, db, client, org_id, user_id, headers):
        self.org_id = org_id
        self.user_id = user_id
        self.headers = headers
        self.db = db
        self.client = client

    def get_logs(self, model_name, model_id, **kwargs):
        """Get logs by model id"""

        activities = (
            Activity.query.filter_by(organization_id=self.org_id)
            .filter_by(model_name=model_name)
            .filter_by(model_id=model_id)
            .all()
        )
        logs = []
        for activity in activities:
            user = User.query.get(activity.user_id)
            if activity.activity_type == ActivityTypeEnum.COMMENT:
                log = {
                    "user": user,
                    "message": activity.message,
                    "activity": "NOTES",
                    "created_at": activity.created_at,
                    "meta": None,
                }
            elif activity.activity_type == ActivityTypeEnum.LICENSE_PLATE_MOVE:
                lp_move = (
                    LicensePlateMove.query.filter_by(organization_id=self.org_id)
                    .filter_by(activity_id=activity.id)
                    .first()
                )
                if lp_move:
                    location = Location.get(lp_move.dest_location_id)
                    log = {
                        "user": user,
                        "message": activity.message,
                        "activity": "LICENSE_PLATE_MOVE",
                        "created_at": activity.created_at,
                        "meta": {"location": {"id": location.id, "name": location.name}}
                        if location
                        else None,
                    }
                else:
                    log = {
                        "user": user,
                        "message": activity.message,
                        "activity": "LICENSE_PLATE_MOVE",
                        "created_at": activity.created_at,
                        "meta": None,
                    }
            elif activity.activity_type == ActivityTypeEnum.LICENSE_PLATE_MADEIT:
                log = {
                    "user": user,
                    "message": activity.message,
                    "activity": "LICENSE_PLATE_MADEIT",
                    "created_at": activity.created_at,
                    "meta": None,
                }
            elif activity.activity_type == ActivityTypeEnum.LICENSE_PLATE_DEDUCT:
                log = {
                    "user": user,
                    "message": activity.message,
                    "activity": "LICENSE_PLATE_DEDUCT",
                    "created_at": activity.created_at,
                    "meta": None,
                }
            else:
                log = {
                    "user": user,
                    "message": activity.message,
                    "activity": None,
                    "created_at": activity.created_at,
                    "meta": None,
                }

            logs.append(log)

        return logs

    def log(self, model_name, model_id, activity_type, sess, **kwargs):
        ip_address = self.headers.get("X-Forwarded-For", None)
        x_user_id = self.headers.get("X-Momenttrack-User", self.user_id)

        if x_user_id != self.user_id:
            x_user = User.get_by_id_and_org(x_user_id, self.org_id)
            if x_user is None or x_user.status not in [
                UserStatusEnum.ACTIVE,
                UserStatusEnum.UNCONFIRMED,
            ]:
                raise DataValidationError(
                    message="User does not exist",
                    errors={
                        "headers": {"X-Momenttrack-User": ["User Id does not exist."]}
                    },
                )

        activity = Activity(
            model_name=model_name,
            model_id=model_id,
            user_id=x_user_id,
            loggedin_user_id=self.user_id,
            organization_id=self.org_id,
            message=kwargs.get("message", None),
            activity_type=activity_type,
            ip_address=ip_address,
        )
        sess.add(activity)

        try:
            sess.flush()
            # data = {
            #     "id": activity.id,
            #     "user_id": activity.user_id,
            #     "organization_id": activity.organization_id,
            #     "ip_address": activity.ip_address,
            #     "created_at": activity.created_at,
            #     "activity_type": activity.activity_type,
            #     "model_name": activity.model_name,
            #     "model_id": activity.model_id,
            # }
            # self.client.index(index="activity", body=data)
        except Exception as e:
            DBErrorHandler(e)

        return activity

    def log_change(self, model_name, model_id, field, old_value, new_value, message):
        activity_id = self.log(
            model_name, model_id, ActivityTypeEnum.CHANGE_TRACK, message=message
        )

        # fetch datatype from field: <InstrumentedAttribute> type
        field_type = str(field.property.columns[0].type)

        # Prepare args
        params = {
            "activity_id": activity_id,
            "organization_id": self.org_id,
            "field_name": field.key,
            # "field_type": field_type,
        }

        # Update fields according to fieldtype
        if field_type == "INTEGER":
            params["field_type"] = ActivityChangeTrackFieldTypeEnum.INTEGER
            params["old_value_integer"] = old_value
            params["new_value_integer"] = new_value
        elif field_type == "FLOAT":
            params["field_type"] = ActivityChangeTrackFieldTypeEnum.FLOAT
            params["old_value_float"] = old_value
            params["new_value_float"] = new_value
        elif field_type == "DATETIME":
            params["field_type"] = ActivityChangeTrackFieldTypeEnum.DATETIME
            params["old_value_datetime"] = old_value
            params["new_value_datetime"] = new_value
        else:
            # fallback to string
            params["field_type"] = ActivityChangeTrackFieldTypeEnum.STRING
            params["old_value_string"] = old_value
            params["new_value_string"] = new_value

        act_change_track = ActivityChangeTrack(**params)
        self.db.writer_session.add(act_change_track)
