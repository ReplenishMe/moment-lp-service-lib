import copy
import datetime

from loguru import logger
from momenttrack_shared_models import (
    LicensePlateStatusEnum,
    LicensePlate,
    Location,
    ActivityTypeEnum,
    ProductionOrderLineitem,
    EverythingReport, LocationPartNoTotals
)
from momenttrack_shared_models.core.schemas import (
    LicensePlateReportSchema,
    ProductionOrder,
    ProductionOrderLineitemSchema,
    LineItemTotals
)
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from momenttrack_shared_services.utils.activity import ActivityService
from momenttrack_shared_services.utils import (
    DBErrorHandler,
    saobj_as_dict,
    get_diff
)

from momenttrack_shared_services.exc import DuplicateMadeTransactionError
import momenttrack_shared_services.messages as MSG


def add_lp(location, session=None, count=1):
    location.lp_qty += count
    session.commit()


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
                loc = Location.get_system_location(self.org_id, session=sess)
                license_plate.location_id = loc.id
            else:
                loc = Location.get_by_id(license_plate.location_id, session=sess)
            # Check if the LP already exists
            existing_lp = LicensePlate.get_by_lp_id_and_org(
                license_plate.lp_id, self.org_id, session=sess
            )
            prev_product = existing_lp.product if existing_lp else None
            intake_data_copy = copy.deepcopy(license_plate)  # preserve data even after a potential rollback
            if existing_lp:
                # if  not self.check_prev_move(existing_lp):
                #     return 1
                old_lp_dict = saobj_as_dict(existing_lp)
                new_lp_dict = saobj_as_dict(license_plate)

                # If already exists, just update it.
                for col, val in new_lp_dict.items():
                    if col not in ['location_id']:
                        setattr(existing_lp, col, val)
                license_plate = existing_lp
                message["converted"] = True
                message["diff"] = get_diff(
                    old_lp_dict, new_lp_dict,
                    ignore_keys=["id", "created_at", "updated_at"]
                )
            else:
                # check if it belongs to some other org
                lp = LicensePlate.query.filter_by(lp_id=license_plate.lp_id).first()
                if lp:
                    DBErrorHandler(
                        Exception(
                            "Licenseplate value already belongs "
                            "to another organization"
                        )
                    )
                loc = Location.get_system_location(self.org_id, session=sess)
                sess.add(license_plate)
                add_lp(loc, sess, license_plate.quantity)

            lp_report = LicensePlateReportSchema(
                exclude=('last_interaction',)
            ).dump(license_plate)
            sess.flush()
            if production_order_id:
                # check if lineitem has been made with same lp_id
                order = sess.scalar(
                    select(ProductionOrder).where(
                        ProductionOrder.id == production_order_id
                    )
                )
                lp_report['production_order_id'] = order.id
                lp_report['product_id'] = order.product_id
                existing_item = ProductionOrderLineitem.query.filter(
                    ProductionOrderLineitem.license_plate_id == license_plate.id,
                    ProductionOrderLineitem.production_order_id == production_order_id
                ).first()
                if existing_item:
                    # log and return
                    sess.rollback()  # rollback any pending change
                    # re-copy transferrable lp data
                    old_lp_dict = saobj_as_dict(existing_lp)
                    new_lp_dict = saobj_as_dict(intake_data_copy)
                    # If already exists, just update it.
                    for col, val in new_lp_dict.items():
                        if not col in ['location_id']:
                            setattr(existing_lp, col, val)
                    license_plate = existing_lp
                    message["converted"] = True
                    message["diff"] = get_diff(
                        old_lp_dict, new_lp_dict, ignore_keys=["id", "created_at", "updated_at"]
                    )
                    # write log with metadata
                    activity = self.activity_service.log(
                        "license_plate",
                        license_plate.id,
                        ActivityTypeEnum.LICENSE_PLATE_MADEIT,
                        sess,
                        metadata={
                            'made_status': False,
                            'prev_part_number': prev_product.part_number
                        },
                        message=MSG.ATTEMPT_TO_REMAKE_LP,
                        current_org_id=self.org_id,
                        current_user_id=self.user_id
                    )
                    sess.commit()
                    raise DuplicateMadeTransactionError(MSG.ATTEMPT_TO_REMAKE_LP)

                po_lineitem = ProductionOrderLineitemSchema().load(
                    {
                        "production_order_id": production_order_id,
                        "license_plate_id": license_plate.id,
                    },
                    session=sess
                )
                po_lineitem.organization_id = self.org_id
                sess.add(po_lineitem)
                try:
                    loc = Location.get_by_id_and_org(
                        license_plate.location_id,
                        self.org_id
                    )
                    upsert_payload = {
                        'production_order_id': production_order_id,
                        'location': loc
                    }
                    LineItemTotals.upsert(upsert_payload, session=sess)
                    upsert_payload = {
                        'loc_id': license_plate.location_id,
                        'product': order.product
                    }
                    LocationPartNoTotals.upsert(upsert_payload, sess)
                    sess.flush()
                except Exception as e:
                    DBErrorHandler(e)

                message["production_order_id"] = production_order_id
                # update everything report with order id
                lp_report['production_order_id'] = production_order_id

            # Create a activity
            activity = self.activity_service.log(
                "license_plate",
                license_plate.id,
                ActivityTypeEnum.LICENSE_PLATE_MADEIT,
                sess,
                message=str(message),
                current_org_id=self.org_id,
                current_user_id=self.user_id,
            )

            # log to opensearch
            self.log_made(license_plate, sess)
            lp_report['last_interaction'] = datetime.datetime.strftime(
                activity.created_at,
                "%Y-%m-%d %H:%M:%S.%f"
            )
            lp_report_upsert_payload = {
                'lp_id': license_plate.lp_id,
                'po_id': lp_report.get('production_order_id', None),
                'report_raw': lp_report
            }
            EverythingReport.upsert(lp_report_upsert_payload, sess)
            try:
                sess.commit()
            except Exception as e:
                sess.rollback()
                raise e
            print(license_plate.lp_id)
            return license_plate

    def rollback_documents(self, index, doc_ids):
        for doc_id in doc_ids:
            try:
                self.client.delete(index=index, id=doc_id)
                print(f"Document with ID {doc_id} deleted from {index}")
            except Exception:  # pylint:disable=W0718
                print(f"Document with ID {doc_id} not found in {index}")

    def log_made(self, license_plate, sess):
        try:
            # logger.info(
            #     "OPENSEARCH [INFO]::Attempting to index"
            #     " license_plate document.."
            # )
            # idx_schema = LicensePlateOpenSearchSchema()
            # doc = self.client.index(
            #     index="lp_alias",
            #     body=idx_schema.dump(license_plate),
            #     id=license_plate.id,
            # )
            # lp_indexes = doc['_id']

            if self.comment:
                activity_service = ActivityService(
                    self.db, self.client,
                    self.org_id, self.user_id, self.headers
                )
                activity_service.log(
                    "license_plate",
                    license_plate.id,
                    ActivityTypeEnum.COMMENT,
                    sess,
                    message=self.comment,
                )
                try:
                    sess.flush()
                except SQLAlchemyError as e:
                    DBErrorHandler(e)
        except Exception as e:  # pylint:disable=W0718
            # sess.flush(license_plate)
            sess.rollback()
            # if lp_indexes:
            #     self.rollback_documents("lps", lp_indexes)
            # logger.error(
            #     "OPENSEARCH [ERROR] An error occurred while trying to "
            #     f"index license_plate with id {license_plate.id}"
            # )
            raise e

    def create_lp_report_entry(self, payload, session):
        # resp = client.index(
        #     index="everything_report_idx",
        #     body=payload,
        #     id=payload['id']
        # )
        # shards = resp.get("_shards", {})
        # if shards.get("failed", 0) > 0:
        #     raise Exception(
        #         f"Warning: Operation succeeded, "
        #         f"but {shards['failed']} shards failed."
        #     )
        try:
            new_report = EverythingReport(**payload)
            session.add(new_report)
            session.flush()
        except Exception as e:
            session.rollback()
            raise e
