import os

from dotenv import load_dotenv

from momenttrack_shared_services import LicensePlateServiceAgent
from momenttrack_shared_services.utils import setup_opensearch
# from momenttrack_shared_models.core.schemas import LicensePlateSchema

# load_dotenv()

# WRITER_DB_URI = os.getenv("DATABASE_URL_WRITER", "sqlite:///dev.db")

# DATABASE_URL_REFRESHER = os.getenv("DATABASE_URL_REFRESHER", "sqlite:///dev.db")

# conf = {
#     'SQLALCHEMY_DATABASE_URI': os.getenv('DATABASE_URL'),
#     'SQLALCHEMY_BINDS': {
#         "writer": WRITER_DB_URI,
#         "cache_refresher": DATABASE_URL_REFRESHER
#     }
# }

# open_client = setup_opensearch()
# agent = LicensePlateServiceAgent(conf, os_client=open_client)


##MOVE##
# lp_move = agent.move(
#     64, 45, 7, {}, 4282
# )
# print(lp_move)

##EDIT##
# lp = {
#     'product_id': 1031,
#     'id': 45
# }
# lp_edit = agent.edit(lp, 7)
# print(lp_edit)

##DEDUCT##
# licenseplate = {
#     'quantity': 10,
#     'product_id': 1031,
#     'location_id': 39,
#     'organization_id': 7,
#     'id': 46
# }
# lp_deduct = agent.deduct(licenseplate, 1, 7, {}, 4282)
# print(lp_deduct)

##SPLIT##
# licenseplate = {
#     'quantity': 10,
#     'product_id': 1031,
#     'location_id': 39,
#     'organization_id': 7,
#     'status': 'CREATED',
#     'id': 46
# }
# lp_split = agent.split(licenseplate, [2, 3, 5])
# print(lp_split)

##WRAP##
# payload = {
#     'location_id': 10,
#     'beacon_id': 1031,
# }
# lp_wrap = agent.wrap(payload, 7, {}, 4282)
# print(lp_wrap)

##cycle_count##
# lp_cycle_count = agent.cycle_count(481399)
# print(lp_cycle_count)

##create##
# lpd = {
#     "lp_id": '125402mkohunbhnjnbhg12m50',
#     "id": 804046,
#     "organization_id": 7,
#     "status": "CREATED",
#     "redirect_url": '',
#     "location_id": 6232,
#     "quantity": 1,
#     "product_id": 10918,
#     "external_serial_number": ''
# }
# db = agent.db
# schema = LicensePlateSchema(unknown='exclude', session=db.writer_session())
# lp = schema.load(lpd)
# lp_create = agent.create(lp, 7, 10, {}, 'eb518a226b614be4acac91626b754006')
# print(lp_create)
