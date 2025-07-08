import json
from sqlalchemy import (
    and_, or_
)
from momenttrack_shared_models.core.database.models import (
    Activity,
    Stack,
    LicensePlate,
    Product
)


class CycleCount:
    def __init__(self, db, license_plate_id, client):
        self.db = db
        self.license_plate_id = license_plate_id
        self.client = client

    def execute(self):
        db = self.db
        query = db.select(Activity).where(
            and_(
                Activity.model_id == self.license_plate_id,
                Activity.model_name == 'license_plate',
                or_(
                    Activity.message.like("%'prev_lp_id'%"),
                    Activity.message.like('%"prev_lp_id"%'),
                ),
                Activity.message.like('%}%')
            )
        ).order_by(Activity.created_at.desc())
        stack_note = db.session.scalars(query).first()
        if stack_note:
            data = json.loads(stack_note.message)
            stack = Stack.query.filter_by(
                stack_id=data['stack_id']
            ).first()
            lp = LicensePlate.query.get(self.license_plate_id)
            prod = Product.query.get(lp.product_id)
            return {
                'item_count': data['stack_index'] + 1,
                'stack_id': data['stack_id'],
                'status': stack.status.name.lower(),
                'part_number': prod.part_number
                }