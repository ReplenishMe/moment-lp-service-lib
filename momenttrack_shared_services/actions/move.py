import datetime

from loguru import logger
from sqlalchemy.orm import lazyload
from momenttrack_shared_models import (
    LicensePlateStatusEnum,
    LicensePlate,
    Location,
    ActivityTypeEnum,
    ProductionOrderLineitem,
    LicensePlateMove
)
from momenttrack_shared_models.core.schemas import (
    LicensePlateMoveSchema,
    LicensePlateMoveLogsSchema,
    LocationSchema,
    LicensePlateSchema,
    LicensePlateOpenSearchSchema
)

from momenttrack_shared_services.messages import \
     LICENSE_PLATE_MOVE_NOT_PERMITTED_WITH_SAME_DESTINATION as invalid_move_msg
from momenttrack_shared_services.utils.activity import ActivityService
from momenttrack_shared_services import messages as MSG
from momenttrack_shared_services.utils import (
    HttpError,
    create_or_update_doc,
    DBErrorHandler,
    update_line_items,
    update_prd_order_totals
)


class Move:
    def __init__(
        self,
        db,
        lp,
        org_id,
        dest_location_id,
        user_id,
        headers,
        client,
        loglocation=None
    ):
        self.lp = lp
        self.client = client
        self.org_id = org_id
        self.loglocation = loglocation
        self.dest_location_id = dest_location_id
        self.headers = headers
        self.user_id = user_id
        self.db = db
        self.activity_service = ActivityService(db, client, org_id, user_id, headers)

    def execute(self):
        client = self.client
        """
        Move the location of a license plate & also record that transaction
        """
        db = self.db
        # # Validation start ##
        lp = LicensePlate.get_by_id(self.lp, session=db.writer_session())
        dest_location_id = self.dest_location_id
        logger.debug(
            f"MOVE: lp={lp.id} from={lp.location_id} to={dest_location_id}"
        )
        # verify license plate
        if lp is None or lp.status in [
            LicensePlateStatusEnum.RETIRED,
            LicensePlateStatusEnum.DELETED,
        ]:
            raise HttpError(code=404, message=MSG.LICENSE_PLATE_NOT_FOUND)

        # verify if src & dest locs are same
        if lp.location_id == dest_location_id:
            raise HttpError(
                code=400,
                message=invalid_move_msg,
            )

        # verify if dest location exists
        loc = Location.get_by_id_and_org(dest_location_id, self.org_id)
        if loc is None or loc.is_inactive:
            raise HttpError(code=404, message=MSG.LOCATION_NOT_FOUND)
        # # Validation end ##

        # create an activity
        activity_id = self.activity_service.log(
            "license_plate",
            lp.id,
            ActivityTypeEnum.LICENSE_PLATE_MOVE,
            current_org_id=self.org_id,
            current_user_id=self.user_id,
        )

        # Create move trx, first
        lp_move = LicensePlateMove(
            license_plate_id=lp.id,
            product_id=lp.product_id,
            organization_id=self.org_id,
            src_location_id=lp.location_id,
            dest_location_id=dest_location_id,
            user_id=self.user_id,
            activity_id=activity_id,
            created_at=datetime.datetime.utcnow(),
            product=lp.product,
            license_plate=lp
            # move_type=LicensePlateMoveTypeEnum.TRANSFER,
        )
        db.writer_session.add(lp_move)

        prev_lp_move = (
            LicensePlateMove.query.with_session(db.writer_session())
            .options(lazyload(LicensePlateMove.user))
            .options(lazyload(LicensePlateMove.product))
            .options(lazyload(LicensePlateMove.license_plate))
            .filter_by(license_plate_id=lp.id, dest_location_id=lp.location_id)
            .order_by(LicensePlateMove.created_at.desc())
            .first()
        )
        lp_move.created_at = datetime.datetime.utcnow()
        if prev_lp_move:
            prev_lp_move.left_at = lp_move.created_at

            # # update log in OS
            try:
                logger.info(
                    "OPENSEARCH: ATTEMPTING TO UPDATE\
                         LPMOVE LOG LEFT_AT"
                )
                create_or_update_doc(
                    client,
                    prev_lp_move,
                    LicensePlateMoveSchema(),
                    {"doc": {"left_at": lp_move.created_at}},
                    "lp_move_alias",
                )
            except Exception as e:
                msg = "OPENSEARCH: AN ERROR OCCURRED WHILE \
                    ATTEMPTING TO UPDATE LPMOVE LOG LEFT_AT"
                logger.error(msg)
                logger.error(e)

        # move to dest location
        lp.location_id = dest_location_id

        # flush changes from this transaction
        db.writer_session.flush()
        db.writer_session.commit()
        resp = self.log_move(lp=lp, lp_move=lp_move)
        return resp
        # return {
        #     'lp_id': lp.lp_id,
        #     'loc': lp.location_id
        # }

    def log_move(self, lp, lp_move):
        resp = LicensePlateMoveLogsSchema().dump(lp_move)

        lp_move_doc_id = None
        try:
            logger.info(
                    """
                        OPENSEARCH [INFO]::Attempting to index
                        license_plate_move document..
                    """
                )
            self.client.index(
                    index="lp_move_alias", body=resp, id=lp_move.id
                )
            lp_move_doc_id = lp_move.id
        except Exception as e:
            logger.error(
                    f"""
                    OPENSEARCH [ERROR] An error occurred while trying to
                     index license_plate with id {lp_move.id}
                    """
                )
            DBErrorHandler(e)

        logger.info(f"move record has been indexed \
            for lp_move id : {lp_move.id} ")

        try:
            # reindex lp
            res = create_or_update_doc(
                self.client,
                lp,
                LicensePlateSchema(),
                {"doc": LicensePlateOpenSearchSchema().dump(lp)},
                "lp_alias",
            )
            logger.info(
                f"lp record has been re-indexed for \
                    lp_move id : {lp_move_doc_id}"
            )

            # update line-graph-data
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "date_key": str(lp_move.created_at)[:10]
                                }
                            },
                            {
                                "match": {
                                    "location_id": lp_move.dest_location_id
                                }
                            },
                            {
                                "match": {
                                    "part_number": resp["product"]["part_number"]
                                }
                            },
                        ]
                    }
                }
            }

            res = self.client.search(index="line_graph_data", body=query)
            res = [
                {
                    "_id": hit["_id"],
                    **hit["_source"]
                } for hit in res["hits"]["hits"]
            ]
            if res:
                line_graph_item = {
                    "date": lp_move.created_at,
                    "location_id": lp_move.dest_location_id,
                    "quantity": int(res[0]["quantity"]) + 1,
                }
                self.client.update(
                    index="line_graph_data",
                    body={"doc": line_graph_item},
                    id=res[0]["_id"],
                )
            else:
                line_graph_item = {
                    "date": lp_move.created_at,
                    "location_id": lp_move.dest_location_id,
                    "quantity": 1,
                    "date_key": str(lp_move.created_at)[:10],
                    "part_number": resp["product"]["part_number"],
                }
                self.client.index(
                    index="line_graph_data",
                    body=line_graph_item
                )

            orders = ProductionOrderLineitem.query.with_entities(
                ProductionOrderLineitem.production_order_id
            ).filter_by(
                license_plate_id=lp.id
            ).all()
            print(orders)

            if orders:
                dest_loc = LocationSchema().dump(
                    Location.get(lp_move.dest_location_id)
                )
                prev_loc = LocationSchema().dump(Location.get(
                    lp_move.src_location_id
                ))
                update = {
                    "location_id": lp_move.dest_location_id,
                    "location": dest_loc
                }
                update_line_items(self.client, lp.id, update)
                # update production_order total summary
                for order in orders:
                    update_prd_order_totals(
                        self.client,
                        lp_move.dest_location_id,
                        order.production_order_id,
                        loc=dest_loc
                    )
                    update_prd_order_totals(
                        self.client,
                        lp_move.src_location_id,
                        order.production_order_id,
                        deduct=True,
                        loc=prev_loc,
                    )
        except Exception as e:
            logger.exception(e)
        return resp
