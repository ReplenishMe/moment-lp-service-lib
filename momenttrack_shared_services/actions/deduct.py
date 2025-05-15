from loguru import logger
from momenttrack_shared_services.utils import DataValidationError
from momenttrack_shared_models import (
    LicensePlateStatusEnum,
    LicensePlate,
    ActivityTypeEnum,
)
from momenttrack_shared_services.utils.activity import ActivityService


class Deduct:
    def __init__(self, db, lp, total_qty_to_deduct, org_id, user_id, client, headers):
        self.db = db
        self.lp = lp
        self.total_qty_to_deduct = total_qty_to_deduct
        self.org_id = org_id
        self.user_id = user_id
        self.client = client
        self.headers = headers
        self.activity_service = ActivityService(
            db, client,
            org_id, user_id,
            headers
        )

    def execute(self):
        db = self.db
        """
        Create a new license_plate with qty `total_qty_to_deduct` &
        reduces that no. in existing license plate
        """

        # ErrorIF: Deduct Qty >  quantity
        if self.total_qty_to_deduct > self.lp.quantity:
            logger.error(
                """total_qty_to_deduct:{}
                    greater than the total available quantity:{}.""",
                self.total_qty_to_deduct,
                self.lp.quantity,
            )
            raise DataValidationError(
                message="""Requested quantity to split is greater
                             than the available quantity""",
                errors={
                    "split_distribution": [
                        f"Max deductible quantity for this license_plate is {
                            self.lp.quantity
                            }"
                    ]
                },
            )
        # Create a new license plate
        new_lp = LicensePlate(
            product_id=self.lp.product_id,
            quantity=self.total_qty_to_deduct,
            location_id=self.lp.location_id,
            organization_id=self.lp.organization_id,
            parent_license_plate_id=self.lp.id,
            status=LicensePlateStatusEnum.CREATED,
        )

        logger.debug(
            "Deducting the qty from original license_plate {}", self.lp.id
            )
        self.lp.quantity = self.lp.quantity - self.total_qty_to_deduct

        # flush everything
        db.writer_session.add(new_lp)
        db.writer_session.flush()

        # create an activity
        self.activity_service.log(
            "license_plate",
            self.lp.id,
            ActivityTypeEnum.LICENSE_PLATE_DEDUCT,
            sess=db.writer_session(),
            current_org_id=self.org_id,
            current_user_id=self.user_id,
        )

        return new_lp
