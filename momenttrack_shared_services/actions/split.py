from loguru import logger
from momenttrack_shared_services.utils import DataValidationError
from momenttrack_shared_models import (
    LicensePlateStatusEnum,
    LicensePlate,
)


class Split:
    def __init__(self, db, licenseplate, split_distribution, client):
        self.db = db
        self.client = client
        self.licenseplate = licenseplate
        self.split_distribution = split_distribution

    def execute(self):
        db = self.db

        """Creates 2 license plate out of this lp &
            splits the quantity accordingly."""

        total_qty_to_deduct = sum(self.split_distribution)

        logger.debug(
            "Attempting to split {} qty into {} splits from license_plate: {}",
            total_qty_to_deduct,
            len(self.split_distribution),
            self.licenseplate.id,
        )

        # ErrorIF: Deduct Qty >  quantity
        if total_qty_to_deduct > self.licenseplate.quantity:
            logger.error(
                """total_qty_to_deduct: {}
                    greater than the total available quantity:{}.""",
                total_qty_to_deduct,
                self.licenseplate.quantity,
            )
            raise DataValidationError(
                message="""Requested quantity to split is greater
                             than the available quantity""",
                errors={
                    "split_distribution": [
                        f"Max deductible quantity for this license_plate is {
                            self.licenseplate.quantity
                            }"
                    ]
                },
            )

        # FixIF: deduct Qty < quantity
        if total_qty_to_deduct < self.licenseplate.quantity:
            remaining_qty = round(
                self.licenseplate.quantity - total_qty_to_deduct, 4
                )
            logger.warning(
                """total_qty_to_deduct: {}is less than
                    total available quantity.Creating a new license plate
                    for remaning quantity i.e.,{}""",
                total_qty_to_deduct,
                remaining_qty,
            )
            self.split_distribution.append(remaining_qty)
            total_qty_to_deduct = sum(self.split_distribution)

        # Ensure total deduct qty is same as total available quantity
        assert total_qty_to_deduct == self.licenseplate.quantity

        # Loop & create license_plates as per distribution
        logger.debug("Creating {} license_plates", len(
            self.split_distribution
            ))
        lp_splits = []
        for split_qty in self.split_distribution:
            lp_splits.append(
                LicensePlate(
                    product_id=self.licenseplate.product_id,
                    quantity=split_qty,
                    location_id=self.licenseplate.location_id,
                    organization_id=self.licenseplate.organization_id,
                    parent_license_plate_id=self.licenseplate.id,
                    status=LicensePlateStatusEnum.CREATED,
                )
            )

        logger.debug("Retiring parent license_plate {}", self.licenseplate.id)
        self.licenseplate.status = LicensePlateStatusEnum.RETIRED
        self.licenseplate.quantity = 0

        # flush everything
        db.writer_session.add_all(lp_splits)
        # db.writer_session.flush()
        return lp_splits