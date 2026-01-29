import os

from dotenv import load_dotenv

from momenttrack_shared_models.core.schemas import LicensePlateSchema
from momenttrack_shared_services import LicensePlateServiceAgent
from momenttrack_shared_services.utils import setup_opensearch

load_dotenv()


WRITER_DB_URI = os.getenv("DATABASE_URL_WRITER", "sqlite:///dev.db")
DATABASE_URL_REFRESHER = os.getenv("DATABASE_URL_REFRESHER", "sqlite:///dev.db")
conf = {
    'SQLALCHEMY_DATABASE_URI': os.getenv('DATABASE_URL'),
    'SQLALCHEMY_BINDS': {
        "writer": WRITER_DB_URI,
        "cache_refresher": DATABASE_URL_REFRESHER
    }
}
open_client = setup_opensearch()
agent = LicensePlateServiceAgent(conf, os_client=open_client)

# with agent.db.writer_session() as sess:
sess = agent.db.writer_session()
# lp = agent.create(
#     LicensePlateSchema().load(
#         {'product_id': 2653, 'quantity': 1, 'lp_id': '37RJSU4LNKWPMI3VF2CBITUVR'},
#         session=sess
#     ),
#     7, 10, {}, production_order_id=28
# )
# print(LicensePlateSchema().dump(lp))
# print(lp.lp_id)

lp_move = agent.move(
    64, 10290, 7, {}, 10
)
# lp = {
#     'external_serial_number': 'new serial test',
#     'id': 812306
# }
# agent.edit(lp, 7)
print(lp_move)

# comment test
# agent.comment(
#     46,
#     "test note",
#     7,
#     10,
#     {}
# )
