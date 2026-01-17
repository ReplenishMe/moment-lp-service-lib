import datetime

from loguru import logger
from sqlalchemy.orm import lazyload
from sqlalchemy import select, update
from momenttrack_shared_models import (
    LicensePlateStatusEnum,
    LicensePlate,
    Location,
    ActivityTypeEnum,
    ProductionOrderLineitem,
    LicensePlateMove,
    Container,
    ContainerMove,
    LineItemTotals,
    ProductionOrder,
    ProductionOrderLineitem,
    Product
)
from momenttrack_shared_models.core.schemas import (
    LicensePlateMoveSchema,
    LocationSchema,
    LicensePlateOpenSearchSchema,
    LicensePlateMoveOpenSearchSchema,
    ContainerMoveSchema
)

from .create import Create
from momenttrack_shared_services.messages import \
     LICENSE_PLATE_MOVE_NOT_PERMITTED_WITH_SAME_DESTINATION as invalid_move_msg
from momenttrack_shared_services.utils.activity import ActivityService
from momenttrack_shared_services.utils.location import LocationService
from momenttrack_shared_services import messages as MSG
from momenttrack_shared_services.utils import (
    HttpError,
    create_or_update_doc,
    DBErrorHandler,
    update_line_items,
    update_prd_order_totals
)


def move_lp(src_id, dest_id, session, count=1):
    src_loc = Location.get_by_id(src_id, session=session)
    dest_loc = Location.get_by_id(dest_id, session=session)
    if src_loc.lp_qty > 0:
        src_loc.lp_qty -= count
        dest_loc.lp_qty += count
    session.commit()


class Move:
    def __init__(
        self,
        db,
        move_item_id: [int | str],
        org_id: int,
        dest_location_id: int,
        user_id: int,
        headers: dict,
        client,
        loglocation: bool = None
    ):
        self.move_item_id = move_item_id
        self.client = client
        self.org_id = org_id
        self.loglocation = loglocation
        self.dest_location_id = dest_location_id
        self.headers = headers
        self.user_id = user_id
        self.db = db
        self.activity_service = ActivityService(
            db, client,
            org_id, user_id,
            headers
        )
        self.is_container = False
        self.move_item = self.get_lp_or_container()

    def execute(self):
        client = self.client
        """
        Move the location of a 'movable' & also record that transaction
        """

        db = self.db
        with db.writer_session() as sess:
            mov_item = self.move_item

            # # Validation start ##
            logger.debug(
                f"MOVE: object={mov_item.id} from={mov_item.location_id}"
                f"to={self.dest_location_id}"
            )

            # lp checks
            is_container = isinstance(mov_item, Container)

            if not is_container:
                # verify license plate
                if mov_item is None or mov_item.status in [
                    LicensePlateStatusEnum.RETIRED,
                    LicensePlateStatusEnum.DELETED,
                ]:
                    raise HttpError(
                        code=404,
                        message=MSG.LICENSE_PLATE_NOT_FOUND
                    )

                activityType = ActivityTypeEnum.LICENSE_PLATE_MOVE
                activityModel = "license_plate"
                moveModel = LicensePlateMove

                Move = moveModel(
                    license_plate_id=mov_item.id,
                    product_id=mov_item.product_id,
                    organization_id=self.org_id,
                    src_location_id=mov_item.location_id,
                    dest_location_id=self.dest_location_id,
                    user_id=self.user_id,
                    created_at=datetime.datetime.utcnow(),
                    product=mov_item.product,
                    license_plate=mov_item
                )
                prev_move = (
                    moveModel.query.with_session(db.writer_session())
                    .options(lazyload(LicensePlateMove.user))
                    .options(lazyload(LicensePlateMove.product))
                    .options(lazyload(LicensePlateMove.license_plate))
                    .filter_by(
                        license_plate_id=mov_item.id,
                        dest_location_id=mov_item.location_id
                    )
                    .order_by(LicensePlateMove.created_at.desc())
                    .first()
                )
            else:
                moveModel = ContainerMove
                activityType = ActivityTypeEnum.CONTAINER_MOVE
                activityModel = "Container"
                Move = moveModel(
                    container_id=mov_item.id,
                    organization_id=self.org_id,
                    src_location_id=mov_item.location_id,
                    dest_location_id=self.dest_location_id,
                    user_id=self.user_id,
                    created_at=datetime.datetime.utcnow()
                )
                prev_move = (
                    moveModel.query.with_session(db.writer_session())
                    .options(lazyload(ContainerMove.user))
                    .filter_by(
                        container_id=mov_item.id,
                        dest_location_id=mov_item.location_id
                    )
                    .order_by(ContainerMove.created_at.desc())
                    .first()
                )

            # verify if src & dest locs are same
            if mov_item.location_id == self.dest_location_id:
                raise HttpError(
                    code=400,
                    message=invalid_move_msg
                )

            # verify if dest location exists
            loc = Location.get_by_id_and_org(
                self.dest_location_id,
                self.org_id
            )
            if loc is None or loc.is_inactive:
                raise HttpError(code=404, message=MSG.LOCATION_NOT_FOUND)
            # # Validation end ##

            # create an activity
            activity = self.activity_service.log(
                activityModel,
                mov_item.id,
                activityType,
                sess,
                current_org_id=self.org_id,
                current_user_id=self.user_id,
            )

            # Create move trx, first
            Move.activity_id = activity.id
            sess.add(Move)

            # add items back in session due to
            # activity_service.log() commit()
            sess.add(mov_item)

            if prev_move:
                # add items back in session due to
                # activity_service.log() commit()
                sess.add(prev_move)
                prev_move.left_at = Move.created_at

                # update log in OS
                try:
                    logger.info("OPENSEARCH: ATTEMPTING TO UPDATE MOVE LOG LEFT_AT")  # noqa: E501
                    if not is_container:
                        create_or_update_doc(
                            client,
                            prev_move,
                            LicensePlateMoveSchema(),
                            {"doc": {"left_at": Move.created_at}},
                            "lp_move_alias",
                        )
                    else:
                        create_or_update_doc(
                            client,
                            prev_move,
                            ContainerMoveSchema(),
                            {"doc": {"left_at": Move.created_at}},
                            "container_move_alias"
                        )
                except Exception as e:
                    logger.error(
                        "OPENSEARCH: AN ERROR OCCURRED WHILE "
                        " ATTEMPTING TO UPDATE LPMOVE LOG LEFT_AT"
                    )
                    logger.error(e)

            # move to dest location
            mov_item.location_id = self.dest_location_id

            # flush changes from this transaction
            # db.writer_session.flush()
            sess.flush()
            line_item = ProductionOrderLineitem.query.filter_by(
                license_plate_id=mov_item.id
            ).order_by(ProductionOrderLineitem.created_at.desc()).first()

            resp = self.log_move(
                entity=mov_item,
                move=Move,
                open_client=client,
                is_container=is_container,
                line_item=line_item
            )
            if line_item:
                po_id = line_item.production_order_id
                stmt = (
                    update(LineItemTotals)
                    .where(
                        LineItemTotals.location_id == Move.src_location_id,
                        LineItemTotals.production_order_id == po_id,
                        LineItemTotals.total_items > 0
                    )
                    .values(total_items=LineItemTotals.total_items - 1)
                )
                sess.execute(stmt)
                existing_row = LineItemTotals.get_by_location_and_order(
                    Move.dest_location_id, po_id, session=sess
                )
                if existing_row:
                    existing_row.total_items += 1
                else:
                    new_stat = LineItemTotals(
                        name=loc.name,
                        production_order_id=po_id,
                        location_id=Move.dest_location_id,
                        organization_id=loc.organization_id,
                        total_items=1
                    )
                    sess.add(new_stat)
            Move.update_associated_report(
                datetime.datetime.strftime(
                    activity.created_at,
                    "%Y-%m-%d %H:%M:%S.%f"
                ),
                sess
            )
            try:
                sess.commit()
            except Exception as e:
                sess.rollback()
                raise e
            return resp

    def log_move(
        self,
        entity: LicensePlate | Container,
        move: LicensePlateMove | ContainerMove,
        open_client,
        is_container: bool = False,
        line_item: ProductionOrderLineitem = None
    ):
        if not is_container:
            schema = LicensePlateMoveOpenSearchSchema()
            moveIndex = 'lp_move_alias'
        else:
            schema = ContainerMoveSchema()
            moveIndex = 'container_move_alias'

        resp = schema.dump(move)
        try:
            logger.info(
                """
                    OPENSEARCH [INFO]::Attempting to index
                    license_plate_move document..
                """
            )
            open_client.index(
                index=moveIndex, body=resp, id=move.id
            )
        except Exception as e:
            logger.error(
                    f"""
                    OPENSEARCH [ERROR] An error occurred while trying to
                     index license_plate with id {move.id}
                    """
                )
            DBErrorHandler(e)

        logger.info(f"move record has been indexed for move id : {move.id} ")

        if not is_container:
            LocationService.move_lp(
                move.src_location_id,
                move.dest_location_id,
                self.db,
                count=entity.quantity
            )
            try:
                # reindex lp
                res = create_or_update_doc(
                    open_client,
                    entity,
                    LicensePlateOpenSearchSchema(),
                    {"doc": LicensePlateOpenSearchSchema().dump(entity)},
                    "lp_alias",
                )
                logger.info(f"record has been re-indexed for move id : {move.id} ")

                # update line-graph-data
                query = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "date_key": str(move.created_at)[:10]
                                    }
                                },
                                {
                                    "match": {
                                        "location_id": move.dest_location_id
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

                res = open_client.search(index="line_graph_data", body=query)
                res = [
                    {
                        "_id": hit["_id"],
                        **hit["_source"]
                    } for hit in res["hits"]["hits"]
                ]
                if res:
                    line_graph_item = {
                        "date": move.created_at,
                        "location_id": move.dest_location_id,
                        "quantity": int(res[0]["quantity"]) + entity.quantity,
                    }
                    open_client.update(
                        index="line_graph_data",
                        body={"doc": line_graph_item},
                        id=res[0]["_id"],
                    )
                else:
                    line_graph_item = {
                        "date": move.created_at,
                        "location_id": move.dest_location_id,
                        "quantity": 1,
                        "date_key": str(move.created_at)[:10],
                        "part_number": resp["product"]["part_number"],
                    }
                    open_client.index(
                        index="line_graph_data",
                        body=line_graph_item
                    )

                # if line_item:
                #     dest_loc = LocationSchema().dump(
                #         Location.get(move.dest_location_id)
                #     )
                #     prev_loc = LocationSchema().dump(Location.get(
                #         move.src_location_id
                #     ))
                #     update = {
                #         "location_id": move.dest_location_id,
                #         "location": dest_loc
                #     }
                #     update_line_items(open_client, entity.id, update)
                #     # update production_order total summary
                #     update_prd_order_totals(
                #         open_client,
                #         move.dest_location_id,
                #         line_item.production_order_id,
                #         loc=dest_loc
                #     )
                #     update_prd_order_totals(
                #         open_client,
                #         move.src_location_id,
                #         line_item.production_order_id,
                #         deduct=True,
                #         loc=prev_loc,
                #     )
            except Exception as e:
                logger.error(e)
                DBErrorHandler(e)
        return resp

    def get_lp_or_container(self):
        db = self.db
        obj = None

        obj = LicensePlate.get_by_lp_id_or_id_and_org(
            self.move_item_id, self.org_id, session=db.writer_session()
        )
        if not obj:
            obj = Container.get_by_id_or_by_container_id(
                self.move_item_id, self.org_id,
                session=db.writer_session()
            )
        if not obj:
            # check if its a container
            print("License plate doesn't already exist creating ...")
            cr = Create(
                db,
                self.org_id,
                self.user_id,
                self.client,
                self.headers,
                comment="Licenseplate made outside of proper made request"
            )
            license_plate = LicensePlate(
                lp_id=self.move_item_id,
                product_id=Product.get_system_product(
                    self.org_id
                ).id,
                quantity=1,
                organization_id=self.org_id
            )
            try:
                obj = cr.execute(
                    license_plate,
                    production_order_id=ProductionOrder.get_system_order(
                        self.org_id,
                        self.user_id
                    ).id
                )
                # fetch obj afterwards
                obj = LicensePlate.get_by_lp_id_or_id_and_org(
                    self.move_item_id, self.org_id, session=db.writer_session()
                )
            except HttpError as e:
                logger.error(e)
                return

        self.is_container = isinstance(obj, Container)
        return obj
