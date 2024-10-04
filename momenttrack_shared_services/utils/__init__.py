import datetime
import statistics
import re

from momenttrack_shared_models import (
    LicensePlateMove,
    LicensePlate,
    Product,
    User,
    Location
)
from momenttrack_shared_models.core.extensions import db
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from marshmallow import (
    pre_load, post_dump, pre_dump, Schema, post_load
)
import marshmallow.fields as ma
from marshmallow import fields
from opensearchpy.exceptions import NotFoundError
from sqlalchemy.exc import IntegrityError
from loguru import logger

import momenttrack_shared_services.messages as MSG


class DataValidationError(Exception):
    def __init__(self, message, errors, data=None):
        super(DataValidationError, self).__init__(message, errors, data)
        self.message = message
        self.errors = errors
        self.data = data

    def __reduce__(self):
        return (DataValidationError, (self.message, self.errors, self.data))


class HttpError(Exception):
    """docstring for HttpError"""

    def __init__(self, message, code):
        super(HttpError, self).__init__(message, code)
        self.message = message
        self.code = code

    def __reduce__(self):
        return (DataValidationError, (self.message, self.code))


def validate_unique_violation(e):
    """checks if db error is related to unique violation & if yes, returns the column name"""

    if not isinstance(e, IntegrityError):
        return None

    def _parse_duplicate_col(e):
        dup_key_regexes = [
            re.compile(
                r'^.*duplicate\s+key.*"(?P<columns>[^"]+)"\s*\n.*'
                r"Key\s+\((?P<key>.*)\)=\((?P<value>.*)\)\s+already\s+exists.*$"
            ),
            re.compile(r"^.*duplicate\s+key.*\"(?P<columns>[^\"]+)\"\s*\n.*$"),
        ]

        for dup_key_regex in dup_key_regexes:
            parsed = dup_key_regex.findall(e._message())
            parsed = parsed[0]

            if type(parsed) == tuple:
                fk = parsed[0]
                cols = [col.strip() for col in parsed[1].split(",")]
                vals = [col.strip() for col in parsed[2].split(",")]
                break

        # remove org_id col if exists
        if "organization_id" in cols:
            cols.remove("organization_id")

        return cols

    # ## Check if unique key violation ###
    PG_UNIQUE_VIOLATION_CONSTANT = 23505
    try:
        if int(e.orig.pgcode) == PG_UNIQUE_VIOLATION_CONSTANT:
            return _parse_duplicate_col(e)
    except Exception as e:
        # brute force check
        if "duplicate key value violates unique constraint" in e.args[0]:
            return _parse_duplicate_col(e)


def validate_foreignkey_violation(e):
    # ## Check if foreign key violation ###

    if not isinstance(e, IntegrityError):
        return None

    def _parse_foreign_key_error(e):
        """Parse foreign key error"""
        msg = e._message()
        foreign_key_regex = re.compile(
            r".*DETAIL:  Key \((?P<key>.+)\)=\(.+\) is not present in table \"(?P<key_table>[^\"]+)\""
        )

        try:
            col, table = foreign_key_regex.findall(msg)[0]
            return f"Provided {col} does not exist"
        except Exception:
            return None

    PG_FOREIGN_KEY_VIOLATION = 23503
    try:
        if int(e.orig.pgcode) == PG_FOREIGN_KEY_VIOLATION:
            return _parse_foreign_key_error(e)
    except Exception:
        return "Invalid value for one of the columns"

    return None


def DBErrorHandler(e):
    """handle certain db related exceptions"""
    db.session.rollback()
    db.writer_session.rollback()

    # 1. Check if error is due to unique constraint violation
    uniq_error_cols = validate_unique_violation(e)
    if uniq_error_cols:
        errors = {}
        for col in uniq_error_cols:
            errors[col] = [f"{col} already exists"]

        raise DataValidationError(
            message="One or more fields already exist in the database",
            errors=errors
        )

    # 2. Check if foreign key violation
    foreign_key_error = validate_foreignkey_violation(e)
    if foreign_key_error is not None:
        raise DataValidationError(message=foreign_key_error, errors=None)


def _parse_ma_error(exc, data, **kwargs):
    # format & custom the messages
    message = ""
    for key, val in exc.messages.items():
        err = val[0]
        if "Missing data" in err:
            message = f"{key}: Missing data for required field"
        else:
            message = f"{key}: {err}"
        break

    return message


class BaseSQLAlchemyAutoSchema(SQLAlchemyAutoSchema):
    def handle_error(self, exc, data, **kwargs):
        """Log and raise our custom exception when (de)serialization fails."""
        message = _parse_ma_error(exc, data, **kwargs)

        raise DataValidationError(message=message, errors=exc.messages, data=data)

    @pre_load
    def remove_skip_values(self, data, many, partial):
        """Treat nulls & empty strings are undefined

        As per these guidelines: https://google.github.io/styleguide/jsoncstyleguide.xml#Empty/Null_Property_Values
        """
        if not data:
            return data

        SKIP_VALUES = ["", None]
        return {
            key: value for key, value in data.items()
            if value not in SKIP_VALUES
        }

    @pre_load
    def emails_should_be_lower_case(self, data, many, partial):
        """Convert all `email` fields to lower case"""
        if data and "email" in data:
            data["email"] = data["email"].lower()

        return data


class BaseMASchema(Schema):
    def handle_error(self, exc, data, **kwargs):
        """Log and raise our custom exception when (de)serialization fails."""
        message = _parse_ma_error(exc, data, **kwargs)
        raise DataValidationError(message=message, errors=exc.messages, data=data)

    @pre_load
    def emails_should_be_lower_case(self, data, many, partial):
        """Convert all `email` fields to lower case"""
        if data and "email" in data:
            data["email"] = data["email"].lower()

        return data


class LicensePlateOpenSearchSchema(BaseSQLAlchemyAutoSchema):
    id = ma.Int(dump_only=True)
    created_at = ma.String(dump_only=True)
    organization_id = ma.Integer(dump_only=True, required=False)
    location = fields.Nested(
        "LocationSchema", only=(
            "name", "width", "height", "beacon_id", "depth"
        )
    )
    product = fields.Nested(
        "ProductSchema",
        only=(
            "part_number",
            "description",
        ),
    )

    class Meta:
        model = LicensePlate
        include_relationships = True
        include_fk = True


class LicensePlateSchema(BaseSQLAlchemyAutoSchema):
    id = ma.Int(dump_only=True)
    created_at = ma.String(dump_only=True)  # read-only
    organization_id = ma.Integer(dump_only=True, required=False)  # read-only
    location_id = ma.Integer(required=False)  # optional field
    product = fields.Nested("ProductSchema", only=("part_number",))

    class Meta:
        exclude = ("updated_at",)
        model = LicensePlate
        sqla_session = db.session
        load_instance = True
        include_relationships = True
        include_fk = True


class LocationSchema(BaseSQLAlchemyAutoSchema):
    id = ma.Int(dump_only=True)
    created_at = ma.String(dump_only=True)  # read-only
    organization_id = ma.Integer(dump_only=True, required=False)  # read-only

    class Meta:
        exclude = ("updated_at",)
        model = Location
        sqla_session = db.session
        load_instance = True
        # include_relationships = True
        include_fk = True

    @pre_load
    def check_enum_value(self, data, **kwargs):
        """intercepts 'unit' field in json and preformats it"""
        if data:
            if "unit" in data.keys():
                data["unit"] = data["unit"].upper()
            return data


# class ProductSchema(BaseSQLAlchemyAutoSchema):
#     id = ma.Int(dump_only=True)
#     created_at = ma.String(dump_only=True)  # read-only
#     organization_id = ma.Integer(dump_only=True)  # read-only
#     preferred_vendor = ma.Nested("VendorSchema", dump_only=True)  # read-only

#     class Meta:
#         exclude = ("updated_at",)
#         model = Product
#         sqla_session = db.session
#         load_instance = True
#         include_relationships = True
#         include_fk = True


class LicensePlateMoveLogsSchema(BaseSQLAlchemyAutoSchema):
    id = ma.Int(dump_only=True)
    created_at = ma.String(dump_only=True, data_key="arrived_at")  # read-only
    left_at = ma.String()
    product = ma.Nested(
        'ProductSchema',
        exclude=(
            "license_plate_move", "license_plates", "production_order"
        ),
        dump_only=True,
    )
    user = ma.Nested("UserSchema", dump_only=True)
    license_plate = ma.Nested(
        LicensePlateSchema(
            only=(
                "lp_id",
                "quantity",
                "id",
                "external_serial_number",
                "product_id",
                "product",
            )
        ),
        dump_only=True,
    )

    class Meta:
        exclude = (
            "updated_at",
            "organization_id",
            "trx_id",
        )
        model = LicensePlateMove
        load_instance = True
        include_relationships = True
        include_fk = True

    @pre_dump
    def normalize_date(self, obj, *args, **kwargs):
        if obj:
            obj.created_at = obj.created_at.strftime("%Y-%m-%d %H:%M:%S.%f")
            if obj.left_at:
                obj.left_at = obj.left_at.strftime("%Y-%m-%d %H:%M:%S.%f")
        return obj

    @post_dump
    def data_check(self, data, *, session=None, **kwargs):
        if "lp_id" not in data["license_plate"]:
            data["license_plate"] = LicensePlateSchema().dump(
                LicensePlate.get_by_id_and_org(
                    data["license_plate_id"], data["organization_id"]
                )
            )
        return data


# class UserSchema(BaseSQLAlchemyAutoSchema):
#     id = ma.Int(dump_only=True)
#     person_id = ma.String(dump_only=True)  # read-only
#     created_at = ma.String(dump_only=True)  # read-only

#     class Meta:
#         exclude = (
#             "confirmed_at",
#             "updated_at",
#             "password",
#         )
#         model = User
#         load_instance = True
#         include_fk = True


class InvalidLengthError(Exception):
    pass


class LpMoveField(fields.Field):
    #: Default error messages.
    default_error_messages = {
        "invalid": "Not a valid 'LpMoveField' value.",
        "length": "Invalid length for field value",
    }

    def __init__(self, allowed_len=None, **kwargs):
        super().__init__(**kwargs)
        self.allowed_len = allowed_len

    def _verifyData(self, val):
        int_cond = isinstance(val, int)
        str_cond = isinstance(val, str)

        assert (int_cond or str_cond) == True

        if str_cond:
            try:
                assert len(val) == self.allowed_len
            except AssertionError:
                raise InvalidLengthError

        return val

    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return None
        try:
            return self._verifyData(value)
        except AssertionError as e:
            raise self.make_error("invalid")
            logger.error(e)
        except InvalidLengthError as e:
            raise self.make_error("length")
            logger.error(e)

    def _deserialize(self, value, attr, data, **kwargs):
        if value is None:
            return None
        try:
            return self._verifyData(value)
        except AssertionError as e:
            raise self.make_error("invalid")
            logger.error(e)
        except InvalidLengthError as e:
            raise self.make_error("length")
            logger.error(e)


class LicensePlateMoveSchema(BaseMASchema):
    license_plate_id = LpMoveField(required=True, allowed_len=25)
    dest_location_id = LpMoveField(required=True, allowed_len=11)
    user_id = LpMoveField(required=False, allowed_len=17)

    @post_load
    def add_required_alt(self, data, *args, **kwargs):
        if data:
            if isinstance(data["license_plate_id"], str):
                tmp: [LicensePlate | None] = LicensePlate.query.filter_by(
                    lp_id=data["license_plate_id"]
                ).first()
                if not tmp:
                    raise DataValidationError(
                        MSG.LICENSE_PLATE_NOT_FOUND,
                        MSG.LICENSE_PLATE_NOT_FOUND
                    )
                data["license_plate_id"] = tmp.id
            if isinstance(data["dest_location_id"], str):
                tmp: [Location | None] = Location.query.filter(
                    Location.beacon_id == data["dest_location_id"]
                )
                if not tmp:
                    raise DataValidationError(
                        MSG.LOCATION_NOT_FOUND,
                        MSG.LOCATION_NOT_FOUND
                    )
                data["dest_location_id"] = tmp.id
            if "user_id" in data:  # user_id is an optional field
                if isinstance(data["user_id"], str):
                    tmp: [User | None] = User.query.filter_by(
                        person_id=data["user_id"]
                    ).first()
                    if not tmp:
                        raise DataValidationError(
                            MSG.USER_NOT_FOUND, MSG.LICENSE_PLATE_NOT_FOUND
                        )
                    data["user_id"] = tmp.id
        return data


def gen_pre_report(report, location_id):
    from datetime import datetime as dt

    loc = Location.query.get(location_id)

    if report["logs"]:
        oldest_license_plate = LicensePlateSchema().dump(
            LicensePlate.get(report["logs"][-1]["license_plate_id"])
        )
        cuser = UserSchema().dump(User.get(report["logs"][-1]["user_id"]))
        report["oldest_license_plate"] = oldest_license_plate
        report["current_user"] = cuser
        report["oldest_log"] = report["logs"][-1]
        report["latest_log"] = report["logs"][0]

        # average_duration
        dates = [lpm["arrived_at"] for lpm in report["logs"]]
        diffs = [
            (
                dt.strptime(t2, "%Y-%m-%d %H:%M:%S.%f")
                - dt.strptime(t1, "%Y-%m-%d %H:%M:%S.%f")
            ).total_seconds()
            for t1, t2 in zip(dates[:-1], dates[1:])
        ]
        if len(diffs) > 0:
            report["average_duration"] = datetime.timedelta(
                seconds=statistics.mean(diffs)
            ).seconds
        else:
            report["average_duration"] = 0
    else:
        report["oldest_license_plate"] = None
        report["current_user"] = None
        report["oldest_log"] = None
        report["latest_log"] = None
        report["average_duration"] = None

    report["beacon_id"] = loc.beacon_id
    report["name"] = loc.name
    report["active"] = loc.active
    report["location_id"] = location_id
    report["created_at"] = datetime.datetime.utcnow()
    return report


def append_line_graph_data(data, client):
    log_count = len(data["logs"])
    mset = set()
    line_graph_map = {}
    query = {
        "query": {"match": {"location_id": data["location_id"]}},
        "sort": {"date": {"order": "desc"}},
        "size": 10000,
    }
    res = client.search(index="line_graph_data", body=query)
    line_graph_data = [
        {"_id": hit["_id"], **hit["_source"]} for hit in res["hits"]["hits"]
    ]
    # Create a search request

    for line_item in line_graph_data:
        _part_no = line_item["part_number"]
        if _part_no not in mset:
            others = [
                {"date": x["date_key"], "quantity": x["quantity"]}
                for x in line_graph_data
                if x["part_number"] == _part_no and x["_id"] != line_item["_id"]
            ]
            line_graph_map[_part_no] = [
                {"date": line_item["date_key"], "quantity": line_item["quantity"]},
                *others,
            ]
            mset.add(_part_no)

    line_graph_map = [{"name": k, "values": line_graph_map[k]} for k in line_graph_map]

    # formatting the report
    if "oldest_log" in data and "latest_log" in data:
        resp = {
            "line_graph_data": {
                "dateFrom": data["oldest_log"]["arrived_at"]
                if data["oldest_log"]
                else None,
                "dateTo": data["latest_log"]["arrived_at"]
                if data["latest_log"]
                else None,
                "data": line_graph_map,
            },
            **data,
        }
        if "oldest_log" in resp:
            del resp["oldest_log"]
        if "latest_log" in resp:
            del resp["latest_log"]

        return resp

    elif "line_graph_data" in data:
        data["line_graph_data"]["data"] = line_graph_map

        return data


def create_or_update_doc(client, obj, schema, data, index, type=None):
    try:
        client.get(index=index, id=obj.id)
        ans = client.update(index=index, body=data, id=obj.id)
    except NotFoundError:
        if type == "location":
            lpmoves = LicensePlateMoveLogsSchema(many=True).dump(
                LicensePlateMove.query.filter_by(dest_location_id=obj.id).all()
            )
            rep = {"logs": lpmoves}
            rep = gen_pre_report(rep, obj.id)
            rep = append_line_graph_data(rep, client)
        else:
            rep = schema.dump(obj)
        ans = client.index(index=index, body=rep, id=obj.id)
    return ans


def update_prd_order_totals(client, loc_id, order_id, deduct=False, loc=None):
    # find production_order_total obj and update
    from opensearchpy.exceptions import NotFoundError

    try:
        res = client.get(
            index="production_order_lineitems_totals_alias", id=f"{order_id}_{loc_id}"
        )
        res = {"_id": res["_id"], **res["_source"]}
        if deduct:
            update = {"total_items": int(res["total_items"]) - 1}
        else:
            update = {"total_items": int(res["total_items"]) + 1}
        client.update(
            index="production_order_lineitems_totals_alias",
            body={"doc": update},
            id=res["_id"],
        )
    except NotFoundError:
        logger.error(
            f"Order total doesn't yet exist for the location "
            f"with id {loc_id} in order {order_id}"
        )
        new_sum = {
            "name": loc["name"],
            "total_items": 1,
            "production_order_id": order_id,
            "location_id": loc_id,
            "organization_id": loc["organization_id"],
            "created_at": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
        }
        client.index(
            index="production_order_lineitems_totals_alias",
            body=new_sum,
            id=f"{order_id}_{loc_id}",
        )


def update_line_items(client, lp_id, obj):
    from opensearchpy.helpers.update_by_query import UpdateByQuery

    ubq = (
        UpdateByQuery(using=client, index="production_order_lineitems_alias")
        .query("match", license_plate_id=lp_id)
        .script(
            source="""
            for (entry in params.updates.entrySet())
            {
                ctx._source[entry.getKey()] = entry.getValue();
            }
        """,
            lang="painless",
            params={"updates": obj},
        )
    )
    response = ubq.execute()
    return response
