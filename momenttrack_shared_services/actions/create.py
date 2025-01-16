import requests
from loguru import logger
from momenttrack_shared_models import (
    LicensePlateStatusEnum,
    LicensePlate,
    Location,
    ActivityTypeEnum,
    ProductionOrderLineitem
)
from momenttrack_shared_models.core.schemas import (
    LocationSchema,
    LicensePlateMadeItRequestSchema,
    LicensePlateOpenSearchSchema,
    ProductionOrderLineitemSchema
)
from sqlalchemy.exc import SQLAlchemyError

from momenttrack_shared_services.utils.activity import ActivityService
from momenttrack_shared_services.utils import (
    DBErrorHandler,
    saobj_as_dict,
    DataValidationError,
    get_diff,
    update_prd_order_totals
)


class Create:
    def __init__(self, db, org_id, user_id, client, headers, comment=None):
        self.db = db
        self.org_id = org_id
        self.user_id = user_id
        self.client = client
        self.headers = headers
        self.comment = comment

        self.activity_service = ActivityService(
            db, client, org_id,
            user_id, headers
        )

    def execute(self, license_plate, production_order_id=None):
        db = self.db
        client = self.client

        """Create a new license plate"""
        message = {}

        logger.info(
            f"Attempting to create a license_plate lp_id={license_plate.lp_id}"
        )
        logger.info(
            f"Attempting to create a license_plate lp_id={license_plate.id}"
        )
        # Added the common data
        with db.writer_session() as sess:
            license_plate.organization_id = self.org_id
            license_plate.status = LicensePlateStatusEnum.CREATED

            # add design imaging redirect
            # TODO: remove this hardcoded portion and make this more extensible
            if self.org_id == 54:
                license_plate.redirect_url = 'https://www.sentrelproducts.com/'
            elif self.org_id == 4:
                license_plate.redirect_url = 'https://momenttrack.com/'

            # default location
            if license_plate.location_id is None:
                logger.debug(
                    "Location doesn't exist, assigning system \
                        location automatically."
                )
                license_plate.location_id = Location.get_system_location(
                    self.org_id
                ).id

            # Check if the LP already exists
            existing_lp = LicensePlate.get_by_lp_id_and_org(
                license_plate.lp_id, self.org_id, session=sess
            )
            if existing_lp:
                # if  not self.check_prev_move(existing_lp):
                #     return 1

                old_lp_dict = saobj_as_dict(existing_lp)
                new_lp_dict = saobj_as_dict(license_plate)

                # If already exists, just update it.
                for col, val in new_lp_dict.items():
                    setattr(existing_lp, col, val)
                license_plate = existing_lp
                message["converted"] = True
                message["diff"] = get_diff(
                    old_lp_dict, new_lp_dict,
                    ignore_keys=["id", "created_at", "updated_at"]
                )
            else:
                # otherwise, add
                sess.add(license_plate)

            sess.commit()
            if production_order_id:
                existing_item = ProductionOrderLineitem.query.filter(
                    ProductionOrderLineitem.license_plate_id == license_plate.id,
                    ProductionOrderLineitem.production_order_id == production_order_id
                ).first()
                if existing_item:
                    DBErrorHandler(Exception('lineitem with lp_id already exists'))
                po_lineitem = ProductionOrderLineitemSchema().load(
                    {
                        "production_order_id": production_order_id,
                        "license_plate_id": license_plate.id,
                    },
                    session=sess
                )
                po_lineitem.organization_id = self.org_id
                try:
                    sess.add(po_lineitem)
                    sess.commit()

                    obj = ProductionOrderLineitemSchema(
                        only=(
                            "id",
                            "created_at",
                            "license_plate_id",
                            "status",
                            "production_order_id",
                            "organization_id",
                        )
                    ).dump(po_lineitem)
                    obj["lp_id"] = None
                    obj["location_id"] = None
                    obj["location"] = None
                    obj["external_serial_number"] = None
                    lp = LicensePlate.get(obj["license_plate_id"])
                    if lp:
                        obj["lp_id"] = lp.lp_id
                        obj["location_id"] = lp.location_id
                        obj["location"] = LocationSchema().dump(
                            Location.get(lp.location_id)
                        )
                        obj["external_serial_number"] = lp.external_serial_number
                    search_query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "production_order_id": po_lineitem.production_order_id
                                        }
                                    },
                                    {"match": {"license_plate_id": lp.id}},
                                ]
                            }
                        }
                    }
                    resp = client.search(
                        index="production_order_lineitems_alias",
                        body=search_query
                    )
                    logger.info("kk")
                    logger.info("Attempting a made a check", resp)
                    check = resp["hits"]["hits"]

                    if len(check) != 0:
                        logger.info("update Attempting made many times ")
                        result= client.index(
                            index="production_order_lineitems_alias",
                            body=obj, id=check[0]["_id"]
                        )
                        requests.patch(
                            "https://mt-sandbox.firebaseio.com/error_log_created.json",
                            json={"index": "production_order_lineitems_alias", 
                            "lp_id": obj["lp_id"], "result": result})
                    else:
                        logger.info("Attempting a made for first time ")
                        client.index(
                            index="production_order_lineitems_alias",
                            body=obj, id=po_lineitem.id
                        )
                        requests.patch(
                            "https://mt-sandbox.firebaseio.com/error_log_created.json",
                            json={"index": "production_order_lineitems_alias", 
                            "lp_id": obj["lp_id"], "result": result})
                    update_prd_order_totals(
                        client,
                        license_plate.location_id,
                        po_lineitem.production_order_id,
                        loc=LocationSchema().dump(
                            Location.get_by_id_and_org(
                                license_plate.location_id, self.org_id
                            )
                        ),
                    )
                except Exception as e:
                    DBErrorHandler(e)

                message["production_order_id"] = production_order_id

            # Create a activity
            self.activity_service.log(
                "license_plate",
                license_plate.id,
                ActivityTypeEnum.LICENSE_PLATE_MADEIT,
                message=str(message),
                current_org_id=self.org_id,
                current_user_id=self.user_id,
            )
            schema = LicensePlateMadeItRequestSchema()
            resp = schema.dump(license_plate)

            # log to opensearch
            self.log_made(license_plate)

            return license_plate

    def rollback_documents(self, index, doc_ids):
        for doc_id in doc_ids:
            try:
                self.client.delete(index=index, id=doc_id)
                print(f"Document with ID {doc_id} deleted from {index}")
            except Exception:  # pylint:disable=W0718
                print(f"Document with ID {doc_id} not found in {index}")

    def log_made(self, license_plate):
        with self.db.writer_session() as sess:
            try:
                logger.info(
                    "OPENSEARCH [INFO]::Attempting to index"
                    " license_plate document.."
                )
                idx_schema = LicensePlateOpenSearchSchema()
                doc = self.client.index(
                    index="lp_alias",
                    body=idx_schema.dump(license_plate),
                    id=license_plate.id,
                )
                lp_indexes = doc['_id']

                if self.comment:
                    activity_service = ActivityService(
                        self.db, self.client,
                        self.org_id, self.user_id, self.headers
                    )
                    activity_service.log(
                        "license_plate",
                        license_plate.id,
                        ActivityTypeEnum.COMMENT,
                        message=self.comment,
                    )
                    try:
                        sess.commit()
                    except SQLAlchemyError as e:
                        DBErrorHandler(e)
            except Exception as e:  # pylint:disable=W0718
                sess.flush(license_plate)
                sess.rollback()
                if lp_indexes:
                    self.rollback_documents("lps", lp_indexes)
                logger.error(
                    "OPENSEARCH [ERROR] An error occurred while trying to "
                    f"index license_plate with id {license_plate.id}"
                )
                logger.error(e)
