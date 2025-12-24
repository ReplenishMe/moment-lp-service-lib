"""
    This functionality is only temporary, and will be
     removed in favor of a more robust architecture
"""

from loguru import logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import lazyload
from momenttrack_shared_models import (
    LicensePlate,
    ProductionOrderLineitem
)
from momenttrack_shared_models.core.schemas import (
    LicensePlateSchema,
    LicensePlateOpenSearchSchema,
    LicensePlateMove,
    LicensePlateReportSchema
)

from momenttrack_shared_services.utils import (
    HttpError,
    DBErrorHandler,
    update_lp_moves,
    update_line_items
)
from momenttrack_shared_services import messages as MSG


def _edit(db, lp_obj, org_id, client):
    with db.writer_session() as sess:
        license_plate_id = lp_obj.pop('id')
        license_plate = LicensePlate.get_by_lp_id_or_id_and_org(
            license_plate_id, org_id, session=sess
        )
        if not license_plate:
            raise HttpError(code=404, message=MSG.LICENSE_PLATE_NOT_FOUND)

        license_plate_id = license_plate.id
        lp_location_id = license_plate.location_id

        # find line item
        line_item = ProductionOrderLineitem.query.filter_by(
            license_plate_id=license_plate.id
        ).first()

        # find moves
        lp_moves = db.session.query(LicensePlateMove).options(
            lazyload(LicensePlateMove.user)
        ).options(
            lazyload(LicensePlateMove.product)
        ).options(lazyload(LicensePlateMove.license_plate)).filter_by(
            license_plate_id=license_plate_id
        ).all()

        schema = LicensePlateSchema(partial=True, session=sess)
        license_plate = schema.load(lp_obj, instance=license_plate)

        if lp_location_id != license_plate.location_id:
            logger.error(
                "Attempting to change location from {} to {}. Denying!",
                lp_location_id,
                license_plate.location_id,
            )
            raise HttpError(
                code=403, message=MSG.LICENSE_PLATE_MOVE_NOT_PERMITTED_WITH_PUT
            )
        try:
            sess.commit()
            resp = schema.dump(license_plate)
        except KeyError as ke:
            raise HttpError(code=400, message=f"Missing key: {str(ke)}")
        except ValueError as ve:
            raise HttpError(code=400, message=f"Invalid value: {str(ve)}")
        except SQLAlchemyError as e:
            DBErrorHandler(e)
        finally:
            sess.close()

        try:
            logger.info(
                "OPENSEARCH [INFO]:: Attempting "
                "to index license_plate document.."
            )
            client.update(
                index="lp_alias",
                body={
                    "doc": LicensePlateOpenSearchSchema().dump(license_plate)
                },
                id=license_plate_id,
            )

            # update line-item
            if line_item:
                update = {
                    "external_serial_number": resp["external_serial_number"]
                }
                update_line_items(client, license_plate_id, update)

            if lp_moves:
                update = {
                    "license_plate": {
                        "external_serial_number": resp['external_serial_number']
                    }
                }
                update_lp_moves(client, license_plate_id, update)

            client.update(
                index="everything_report_idx",
                body={"doc": LicensePlateReportSchema(
                    exclude=('last_interaction',)
                ).dump(license_plate)},
                id=license_plate_id
            )
        except Exception as e:
            logger.error(
                "OPENSEARCH [ERROR] An error occurred while trying to "
                "update the report indexes [production_order_lineitem]"
                " and or [license_plates]"
            )
            db.writer_session.rollback()
            raise e

        return resp
