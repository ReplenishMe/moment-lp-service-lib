import datetime
from dictdiffer import (
    diff, revert
)
import statistics
import re
import os

from momenttrack_shared_models import (
    LicensePlateMove,
    LicensePlate,
    User,
    Location
)
from momenttrack_shared_models.core.schemas import \
    LicensePlateMoveLogsSchema
from momenttrack_shared_models.core.extensions import db
from opensearchpy import (
    OpenSearch,
    RequestsHttpConnection
)
from opensearchpy.exceptions import (
    ConflictError,
    NotFoundError
)
from sqlalchemy.exc import IntegrityError
from loguru import logger


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

    raise e

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


def saobj_as_dict(sa_obj):
    return {
        column: getattr(sa_obj, column)
        for column in sa_obj.__table__.c.keys()
        if getattr(sa_obj, column)
    }


def get_diff(obj1, obj2, ignore_keys=None):
    """Get diff between two objects (supports only Dicts for now)

    Args:
        obj1 (Any): Object 1 (old object)
        obj2 (Any): Object 2 (new object)
        ignore_keys (List): List of keys that needs to be ignored during diff calculation
    """
    return list(diff(obj1, obj2, ignore=ignore_keys))


def revert_diff(diff, obj2):
    """Revert to original obj from new obj using the diff (supports only Dicts for now)

    Args:
        diff (list): Diff sequence
        obj2 (Any): New object
    """
    return revert(diff, obj2)


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
    from opensearchpy import OpenSearch
    import time

    client: OpenSearch = client

    def update_with_retry(
            doc_id,
            index,
            initial_backoff=1.25,
            max_retries=30
        ):
        """
        Attempt at thread-safe approach to updating line-items totals.
        Uses 'backoff retry' policy following a optimistic concurrency control
        pattern, to allow worker processes retry updates if the document has
        already been updated by another process since it last fetched the
        document. Rather than blindly updating the document.
        """
        backoff = initial_backoff
        attempt = 0
        while True:
            try:
                doc = client.get(index=index, id=doc_id)
                if deduct:
                    update = {
                        "total_items": int(doc['_source']["total_items"]) - 1
                    }
                else:
                    update = {
                        "total_items": int(doc['_source']["total_items"]) + 1
                    }
                client.update(
                    index=index,
                    id=doc_id,
                    body={'doc': update},
                    if_seq_no=doc["_seq_no"],
                    if_primary_term=doc["_primary_term"]
                )
                logger.info(f"update happened on the {attempt+1} attempt")
                return 0
            except Exception as e:
                attempt += 1
                logger.error(f"Update failed (attempt {attempt + 1}): {e}")
                time.sleep(backoff)  # Wait before retrying
                backoff = min(backoff * 2, 30)  # backoff delay <= 30s
                print(backoff)
                if max_retries and attempt >= max_retries:
                    logger.error("Max retries reached. Giving up.")
                    raise Exception("Max retries reached. Giving up.")

    try:
        """
        !! DISCLAIMER 🔽🔽
        Done this way to prevent race-condition issues
        with multiple worker processes trying to index the
        same document at the same time.

        NOTE: The `Opensearch.create()` method is used to ensure
        that the lineitems_total document is created exactly once
        if it doesn't already exist by any one worker process.

        This `create()` method raises a conflictError if the 
        document already exists (unlike the index() 
        method which would just reindex it anyways). We can latch
        onto that error to take other actions, which in the 
        case below is to perform an update rather 
        than a re-index which is what would happen if 
        we did a lookup->then `index()` approach.
        """
        new_sum = {
            "name": loc["name"],
            "total_items": 1,
            "production_order_id": order_id,
            "location_id": loc_id,
            "organization_id": loc["organization_id"],
            "created_at": datetime.datetime.utcnow().
            strftime("%Y-%m-%d %H:%M:%S.%f")
        }
        client.create(
            index="production_order_lineitems_totals_alias",
            body=new_sum,
            id=f"{order_id}_{loc_id}",
        )
    except ConflictError:
        res = client.get(
            "production_order_lineitems_totals_alias",
            id=f"{order_id}_{loc_id}"
        )
        update_with_retry(
            f"{order_id}_{loc_id}",
            "production_order_lineitems_totals_alias"
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


def update_lp_moves(client, lp_id, obj):
    from opensearchpy.helpers.update_by_query import (
        UpdateByQuery,
        UpdateByQueryResponse
    )

    ubq = (
        UpdateByQuery(using=client, index="lp_move_alias")
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


def setup_opensearch():
    auth_h = (os.getenv("OPENSEARCH_USER"), os.getenv("OPENSEARCH_PASS"))
    client = OpenSearch(
        hosts=[{"host": os.getenv("OPENSEARCH_HOST"), "port": 443}],
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        http_auth=auth_h,
        timeout=300,
    )
    return client
